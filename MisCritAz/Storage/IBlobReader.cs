using System.Threading.Tasks;

namespace MisCritAz.Storage
{
    /// <summary>
    /// Provides read access to blob storage.
    /// </summary>
    public interface IBlobReader
    {
        /// <summary>
        /// Gets a blob from storage
        /// </summary>
        /// <param name="container"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        Task<SampleBlobData> GetBlob(string container, string name);

    }
}
