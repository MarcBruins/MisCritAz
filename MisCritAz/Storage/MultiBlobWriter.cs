using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Polly;
using Polly.CircuitBreaker;

namespace MisCritAz.Storage
{
    public class MultiBlobWriter : IBlobWriter
    {
        private readonly ILogger<MultiBlobWriter> _logger;
        private CloudBlobClient _primaryCloudBlobClient;
        private CloudBlobClient _secondaryCloudBlobClient;
        private readonly string _primaryConnectionString;
        private readonly string _secondaryConnectionString;
        private CircuitBreakerPolicy _circuitBreaker;

        /// <summary>
        /// Max amount of errors before cb is broken. Defaults to 1.
        /// </summary>
        public int ExceptionsAllowedBeforeBreaking { get; set; } = 1;

        /// <summary>
        /// Duration of broken cb. Defaults to 1m
        /// </summary>
        public double TimeOutIfBreaksInMs { get; set; } = 60_000D;

        /// <inheritdoc />
        public MultiBlobWriter(IOptions<BlobStorageConnectionSettings> blobStorageConnectionSettings, ILogger<MultiBlobWriter> logger)
        {
            _logger = logger;
            _primaryConnectionString = blobStorageConnectionSettings.Value.PrimaryBlobStorageConnectionString;
            if (string.IsNullOrWhiteSpace(_primaryConnectionString))
                throw new ArgumentException($"Configuration value '{nameof(blobStorageConnectionSettings.Value.PrimaryBlobStorageConnectionString)}' cannot be null or whitespace.",
                    nameof(blobStorageConnectionSettings));

            _primaryCloudBlobClient = CreateBlobClient(_primaryConnectionString);
            _secondaryConnectionString = blobStorageConnectionSettings.Value.SecondaryBlobStorageConnectionString;
            ConfigureCircuitBreaker();
        }

        /// <inheritdoc />
        public Task Upload(SampleBlobData message)
        {
            //upload function
            async Task UploadBlob(SampleBlobData msg, CloudBlobClient client)
            {
                //message to json
                string json = JsonConvert.SerializeObject(message);
                var buffer = Encoding.UTF8.GetBytes(json);

                await Upload(buffer, msg.Name, msg.Container, client).ConfigureAwait(false);
            }

            if (_secondaryCloudBlobClient != null)
            {
                var policyWrap = Policy.Handle<Exception>()
                    .FallbackAsync(cts => UploadBlob(message, _secondaryCloudBlobClient))
                    .WrapAsync(_circuitBreaker);

                return policyWrap.ExecuteAsync(() => UploadBlob(message, _primaryCloudBlobClient));
            }

            return UploadBlob(message, _primaryCloudBlobClient);

        }

        /// <summary>
        /// Creates a storage container, if it's not already there.
        /// </summary>
        /// <returns></returns>
        private async Task CreateContainer(string containerName, CloudBlobClient client)
        {
            var container = client.GetContainerReference(containerName);
            if (!await container.ExistsAsync().ConfigureAwait(false))
            {
                await container.CreateAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Creates and returns a new CloudBlobClient.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        private static CloudBlobClient CreateBlobClient(string connectionString)
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var client = storageAccount.CreateCloudBlobClient();
            return client;
        }

        /// <summary>
        /// Configures circuit breaker.
        /// </summary>
        private void ConfigureCircuitBreaker()
        {
            if (_secondaryConnectionString == null && _secondaryCloudBlobClient == null) return;

            void OnBreak(Exception exception, TimeSpan timespan)
            {
                //kill primary sender
                _primaryCloudBlobClient = null;
                _secondaryCloudBlobClient = CreateBlobClient(_secondaryConnectionString);
            }

            void OnHalfOpen()
            {
                _logger?.LogWarning("Circuit breaker to 'half open' to move back from secondary to primary blob sender");
                //recreate primary sender
                _primaryCloudBlobClient = CreateBlobClient(_primaryConnectionString);
            }

            void OnReset()
            {
                _secondaryCloudBlobClient = null;
                _logger?.LogWarning("Switched back from secondary to primary blob sender");
            }

            _circuitBreaker = Policy
                .Handle<Exception>()
                .CircuitBreakerAsync(ExceptionsAllowedBeforeBreaking, TimeSpan.FromMilliseconds(TimeOutIfBreaksInMs), OnBreak, OnReset, OnHalfOpen);
        }

        /// <summary>
        /// /Uploads a blob
        /// </summary>
        private async Task Upload(byte[] buffer, string blobName, string blobContainer, CloudBlobClient cloudBlobClient)
        {
            var container = cloudBlobClient.GetContainerReference(blobContainer);
            await CreateContainer(blobContainer, cloudBlobClient).ConfigureAwait(false);
            var blobRef = container.GetBlockBlobReference(blobName);
            await blobRef.UploadFromByteArrayAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
        }
    }
}