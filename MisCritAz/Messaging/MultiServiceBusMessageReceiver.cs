using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Immutable;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;

namespace MisCritAz.Messaging
{
    /// <summary>
    /// Type that receives messages from service bus.
    /// </summary>
    public class MultiServiceBusMessageReceiver : IServiceBusMessageReceiver
    {
        private readonly IMemoryCache _cache;
        private readonly ILogger<MultiServiceBusMessageReceiver> _logger;
        private readonly string _serviceBusSubscription;
        private readonly string _serviceBusTopic;

        private const int MaxDeliveryCount = 10;
        private readonly string _primaryServiceBusConnectionString;
        private readonly string _secondaryServiceBusConnectionString;
        private IMessageReceiver _primaryMessageReceiver;
        private IMessageReceiver _secondaryMessageReceiver;

        /// <summary>
        /// Indicates max amount of concurrent calls to Service Bus. Defaults to 1.
        /// </summary>
        public int MaxConcurrentCalls { get; set; } = 1;


        /// <inheritdoc />
        public MultiServiceBusMessageReceiver(IOptions<ServiceBusConnectionSettings> serviceBusConnectionSettings,
            IMemoryCache cache,
            ILogger<MultiServiceBusMessageReceiver> logger = null)
        {
            if (string.IsNullOrWhiteSpace(serviceBusConnectionSettings.Value.ServiceBusSubscription))
                throw new ArgumentException($"Configuration value '{nameof(serviceBusConnectionSettings.Value.ServiceBusSubscription)}' cannot be null or whitespace.", nameof(serviceBusConnectionSettings));
            if (string.IsNullOrWhiteSpace(serviceBusConnectionSettings.Value.ServiceBusTopic))
                throw new ArgumentException($"Configuration value '{nameof(serviceBusConnectionSettings.Value.ServiceBusTopic)}' cannot be null or whitespace.", nameof(serviceBusConnectionSettings));
            if (string.IsNullOrWhiteSpace(serviceBusConnectionSettings.Value.PrimaryServiceBusConnectionStringForSend))
                throw new ArgumentException($"Configuration value '{nameof(serviceBusConnectionSettings.Value.PrimaryServiceBusConnectionStringForSend)}' cannot be null or whitespace.", nameof(serviceBusConnectionSettings));
            if (string.IsNullOrWhiteSpace(serviceBusConnectionSettings.Value.SecondaryServiceBusConnectionStringForSend))
                throw new ArgumentException($"Configuration value '{nameof(serviceBusConnectionSettings.Value.SecondaryServiceBusConnectionStringForSend)}' cannot be null or whitespace.", nameof(serviceBusConnectionSettings));

            _primaryServiceBusConnectionString = serviceBusConnectionSettings.Value.PrimaryServiceBusConnectionStringForSend;
            _secondaryServiceBusConnectionString = serviceBusConnectionSettings.Value.SecondaryServiceBusConnectionStringForSend;

            _serviceBusSubscription = serviceBusConnectionSettings.Value.ServiceBusSubscription;
            _serviceBusTopic = serviceBusConnectionSettings.Value.ServiceBusTopic;
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _logger = logger;
        }

        /// <summary>
        /// Creates a new <see cref="IMessageReceiver"/>.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        protected virtual IMessageReceiver CreateReceiver(string connectionString)
        {
            var client = new MessageReceiver(connectionString, EntityNameHelper.FormatSubscriptionPath(_serviceBusTopic, _serviceBusSubscription));
            return client;
        }

        /// <summary>
        /// Receives a message from the primary client.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private async Task<bool> ProcessPrimaryMessagesAsync(Message message, CancellationToken token)
        {
            await ProcessMessageForClient(_primaryMessageReceiver, message).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Receives a message from the primary client.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private async Task<bool> ProcessSecondaryMessagesAsync(Message message, CancellationToken token)
        {
            await ProcessMessageForClient(_secondaryMessageReceiver, message).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Processes an incoming message.
        /// </summary>
        /// <param name="receiver"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        private async Task ProcessMessageForClient(IMessageReceiver receiver, Message message)
        {
            SampleMessage messageData = null;
            try
            {
                messageData = DeserializeMessage(message);
            }
            catch (Exception ex)
            {
                //this is an issue, we received data that cannot be interpreted as MessageData.
                await MessageProcessingFailed(message, ex).ConfigureAwait(false);
                //move to dlc
                if (message.SystemProperties.IsReceived)
                {
                    await receiver.DeadLetterAsync(message.SystemProperties.LockToken, "Message cannot be deserialized into MessageData.").ConfigureAwait(false);
                }
            }

            try
            {
                //if messageData is null, this means message is not supposed to be handled
                if (messageData != null)
                {
                    await ProcessMessage(receiver, message, messageData)
                        .ConfigureAwait(false);
                }
                else
                {
                    //unable to process, and not interested
                    await receiver.CompleteAsync(message.SystemProperties.LockToken).ConfigureAwait(false);
                }
            }
            catch (MessageLockLostException ex)
            {
                _logger?.LogWarning(ex, "Message lock was lost for message {MessageId}", message.MessageId);
            }
            catch (Exception ex)
            {
                await MessageProcessingFailed(message, ex).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Converts <see cref="Message.Body"/> into an instance of <see cref="SampleMessage"/>.
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private SampleMessage DeserializeMessage(Message message)
        {
            if (!message.UserProperties.TryGetValue("Type", out var type) || !string.Equals(typeof(SampleMessage).Name, type.ToString(), StringComparison.Ordinal))
                throw new InvalidOperationException($"Unexpected type {type}");
            var json = Encoding.UTF8.GetString(message.Body);
            return JsonConvert.DeserializeObject<SampleMessage>(json);
        }

        /// <summary>
        /// Handles one message.
        /// </summary>
        /// <param name="receiver"></param>
        /// <param name="serviceBusMessage"></param>
        /// <param name="messageData"></param>
        /// <returns></returns>
        private async Task ProcessMessage(IMessageReceiver receiver, Message serviceBusMessage, SampleMessage messageData)
        {
            //TODO: de-duplicate messages

            ImmutableList<SampleMessage> value;
            if (_cache.TryGetValue("CacheKey", out var val))
            {
                value = (ImmutableList<SampleMessage>)val;
            }
            else
            {
                value = ImmutableList<SampleMessage>.Empty;
            }

            messageData.Receiver = receiver.ClientId;
            value = value.Add(messageData);
            _cache.Set("CacheKey", value);


            _logger.LogInformation("Received message with body: {MessageData}", messageData);
            await receiver.CompleteAsync(serviceBusMessage.SystemProperties.LockToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Handle failure of processing a message.
        /// Default implementation logs an error and stops message processing for 'message.SystemProperties.DeliveryCount' * 10 seconds.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        private Task MessageProcessingFailed(Message message, Exception ex)
        {
            int deliveryCount = message.SystemProperties.DeliveryCount;
            if (deliveryCount >= MaxDeliveryCount)
            {
                _logger?.LogError(ex,
                    "Failed to process Service Bus message {MessageId} body. Delivery count:{DeliveryCount} Error:'{Error}'",
                    message.MessageId, deliveryCount, ex);
            }
            else
            {
                _logger?.LogWarning(ex,
                    "Failed to process Service Bus message {MessageId} body. Delivery count:{DeliveryCount} Error:'{Error}'. Operation will be retried.",
                    message.MessageId, deliveryCount, ex);
            }

            return Task.Delay(deliveryCount * 10000);
        }

        /// <summary>
        /// Processes an error.
        /// </summary>
        /// <param name="arg"></param>
        /// <returns></returns>
        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs arg)
        {
            _logger?.LogWarning(arg.Exception, "Exception from Service Bus. Operation will be retried.");
            return Task.CompletedTask;
        }

        /// <summary>
        /// Closes the connection.
        /// </summary>
        private void Close()
        {
            _primaryMessageReceiver?.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            _secondaryMessageReceiver?.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Opens the connection(s) and starts listening.
        /// </summary>
        private void Open()
        {
            if (_primaryMessageReceiver != null)
            {
                //already listening.
                return;
            }

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = MaxConcurrentCalls,
                AutoComplete = false
            };


            //optional secondary
            if (!string.IsNullOrEmpty(_secondaryServiceBusConnectionString))
            {
                _secondaryMessageReceiver = CreateReceiver(_secondaryServiceBusConnectionString);
                _secondaryMessageReceiver.RegisterMessageHandler(ProcessSecondaryMessagesAsync, messageHandlerOptions);
            }

            if (_primaryMessageReceiver == null)
            {
                _primaryMessageReceiver = CreateReceiver(_primaryServiceBusConnectionString);
                _primaryMessageReceiver.RegisterMessageHandler(ProcessPrimaryMessagesAsync, messageHandlerOptions);
            }
        }

        /// <inheritdoc />
        public Task StartAsync(CancellationToken cancellationToken)
        {
            Open();
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.Run((Action)Dispose, cancellationToken);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}