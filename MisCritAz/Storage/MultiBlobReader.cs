using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Polly;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace MisCritAz.Storage
{
    /// <summary>
    /// Implements <see cref="IBlobReader"/>, reads from Azure Blob Storage.
    /// </summary>
    public class MultiBlobReader : IBlobReader
    {
        private readonly ILogger<MultiBlobWriter> _logger;
        private readonly CloudBlobClient _primaryCloudBlobClient;
        private readonly CloudBlobClient _secondaryCloudBlobClient;

        /// <inheritdoc />
        public MultiBlobReader(IOptions<BlobStorageConnectionSettings> blobStorageConnectionSettings, ILogger<MultiBlobWriter> logger)
        {
            _logger = logger;
            string primaryConnectionString = blobStorageConnectionSettings.Value.PrimaryBlobStorageConnectionString;
            if (string.IsNullOrWhiteSpace(primaryConnectionString))
                throw new ArgumentException($"Configuration value '{nameof(blobStorageConnectionSettings.Value.PrimaryBlobStorageConnectionString)}' cannot be null or whitespace.",
                    nameof(blobStorageConnectionSettings));

            _primaryCloudBlobClient = CreateBlobClient(primaryConnectionString);
            string secondaryConnectionString = blobStorageConnectionSettings.Value.SecondaryBlobStorageConnectionString;

            if (!string.IsNullOrWhiteSpace(secondaryConnectionString))
                _secondaryCloudBlobClient = CreateBlobClient(secondaryConnectionString);
        }

        /// <inheritdoc />
        public async Task<SampleBlobData> GetBlob(string container, string name)
        {
            try
            {
                //attempt to download from primary first, and then from secondary.
                Stream stream;
                if (_secondaryCloudBlobClient != null)
                {
                    var policy = Policy<Stream>
                        .Handle<Exception>()
                        .FallbackAsync(ctx => FetchStream(_secondaryCloudBlobClient, container, name));

                    stream = await policy
                        .ExecuteAsync(() => FetchStream(_primaryCloudBlobClient, container, name));
                }
                else
                {
                    stream = await FetchStream(_primaryCloudBlobClient, container, name).ConfigureAwait(false);
                }

                if (stream == null)
                {
                    return null;
                }

                using (stream)
                {
                    using (var ms = new MemoryStream())
                    {
                        await stream.CopyToAsync(ms);
                        string json = Encoding.UTF8.GetString(ms.ToArray());
                        return JsonConvert.DeserializeObject<SampleBlobData>(json);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to fetch blob named {name} from container {container}", name, container);
                throw;
            }
        }

        /// <summary>
        /// Gets one blob (stream) from the blob store.
        /// </summary>
        /// <returns></returns>
        private async Task<Stream> FetchStream(CloudBlobClient client, string container, string name)
        {
            var containerReference = client.GetContainerReference(container);
            var blob = containerReference.GetBlockBlobReference(name);
            if (!(await blob.ExistsAsync().ConfigureAwait(false)))
                return null;

            var stream = new MemoryStream();
            await blob.DownloadToStreamAsync(stream).ConfigureAwait(false);
            stream.Seek(0, SeekOrigin.Begin);
            return stream;
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
    }
}