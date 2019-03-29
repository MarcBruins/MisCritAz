using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Polly;
using Polly.CircuitBreaker;
using System;
using System.Text;
using System.Threading.Tasks;

namespace MisCritAz.Messaging
{
    /// <summary>
    /// Implements <see cref="IServiceBusMessageSender"/> with a primary and secondary topic client.
    /// </summary>
    public class MultiServiceBusMessageSender : IServiceBusMessageSender
    {
        private readonly ILogger<MultiServiceBusMessageSender> _logger;
        private const int ExceptionsAllowedBeforeBreaking = 1;
        private const int TimeOutIfBreaksInMilliseconds = 10_000;

        protected TopicClient PrimaryClient { get; private set; }
        protected TopicClient SecondaryClient { get; private set; }

        private readonly string _secondaryServiceBusConnectionString;
        private readonly string _primaryServiceBusConnectionString;
        private readonly string _serviceBusTopic;
        private CircuitBreakerPolicy _circuitBreaker;

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        public MultiServiceBusMessageSender(IOptions<ServiceBusConnectionSettings> serviceBusConnectionSettings, ILogger<MultiServiceBusMessageSender> logger = null)
        {
            if (string.IsNullOrWhiteSpace(serviceBusConnectionSettings.Value.PrimaryServiceBusConnectionStringForSend))
                throw new ArgumentException($"Configuration value '{nameof(serviceBusConnectionSettings.Value.PrimaryServiceBusConnectionStringForSend)}' cannot be null or whitespace.", nameof(serviceBusConnectionSettings));
            if (string.IsNullOrWhiteSpace(serviceBusConnectionSettings.Value.SecondaryServiceBusConnectionStringForSend))
                throw new ArgumentException($"Configuration value '{nameof(serviceBusConnectionSettings.Value.SecondaryServiceBusConnectionStringForSend)}' cannot be null or whitespace.", nameof(serviceBusConnectionSettings));
            if (string.IsNullOrWhiteSpace(serviceBusConnectionSettings.Value.ServiceBusTopic))
                throw new ArgumentException($"Configuration value '{nameof(serviceBusConnectionSettings.Value.ServiceBusTopic)}' cannot be null or whitespace.", nameof(serviceBusConnectionSettings));
            _logger = logger;

            _primaryServiceBusConnectionString = serviceBusConnectionSettings.Value.PrimaryServiceBusConnectionStringForSend;
            _secondaryServiceBusConnectionString = serviceBusConnectionSettings.Value.SecondaryServiceBusConnectionStringForSend;
            _serviceBusTopic = serviceBusConnectionSettings.Value.ServiceBusTopic;

            ConfigureCircuitBreaker();
        }


        /// <inheritdoc />
        public Task SendMessage(SampleMessage message)
        {
            var policyWrap = Policy.Handle<Exception>()
                .FallbackAsync(cts => ProcessMessageForClient(SecondaryClient, message))
                .WrapAsync(_circuitBreaker);

            return policyWrap.ExecuteAsync(() => ProcessMessageForClient(PrimaryClient, message));
        }

        /// <inheritdoc />
        public void Initialize()
        {
            if (PrimaryClient != null)
                return;

            PrimaryClient = CreateClient(_primaryServiceBusConnectionString);
            SecondaryClient = CreateClient(_secondaryServiceBusConnectionString);
        }

        /// <summary>
        /// Creates and returns a new <see cref="TopicClient"/>
        /// </summary>
        /// <returns></returns>
        protected virtual TopicClient CreateClient(string connectionString)
        {
            return new TopicClient(connectionString, _serviceBusTopic);
        }

        /// <summary>
        /// Publishes <paramref name="message"/> to registered subscribers using <paramref name="client"/>.
        /// </summary>
        /// <returns></returns>
        private async Task ProcessMessageForClient(ISenderClient client, SampleMessage message)
        {
            //Close the PrimaryClient object to see an exception that causes fail-over (PrimaryClient.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult(); in watch)
            message.Sender = client == PrimaryClient ? "PrimaryClient" : "SecondaryClient";
            string json = JsonConvert.SerializeObject(message);
            var brokeredMessage = new Message(Encoding.UTF8.GetBytes(json));
            brokeredMessage.UserProperties.Add("Type", message.GetType().Name);

            await client.SendAsync(brokeredMessage).ConfigureAwait(false);
        }

        /// <summary>
        /// Configures circuit breaker.
        /// </summary>
        private void ConfigureCircuitBreaker()
        {
            void OnBreak(Exception exception, TimeSpan timespan)
            {
                //recreate secondary sender
                if (SecondaryClient == null)
                {
                    SecondaryClient = CreateClient(_secondaryServiceBusConnectionString);
                }

                //kill primary sender
                PrimaryClient.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                PrimaryClient = null;

                _logger?.LogWarning(exception, "Switched from primary to secondary service bus sender");
            }

            void OnHalfOpen()
            {
                //recreate primary sender
                if (PrimaryClient == null)
                {
                    PrimaryClient = CreateClient(_primaryServiceBusConnectionString);
                }

                //kill secondary
                SecondaryClient.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                SecondaryClient = null;

                _logger?.LogWarning("Switched back from secondary to primary service bus sender");
            }

            void OnReset()
            {
                _logger?.LogInformation("Circuit breaker was reset.");
            }

            _circuitBreaker = Policy
                .Handle<Exception>()
                .CircuitBreakerAsync(ExceptionsAllowedBeforeBreaking, TimeSpan.FromMilliseconds(TimeOutIfBreaksInMilliseconds), OnBreak, OnReset, OnHalfOpen);
        }

        /// <summary>
        /// Cleans up (un)managed resources.
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            PrimaryClient?.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            SecondaryClient?.CloseAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }
    }
}
