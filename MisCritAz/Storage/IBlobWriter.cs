using System.Threading.Tasks;

namespace MisCritAz.Storage
{
    public interface IBlobWriter
    {
        Task Upload(SampleBlobData message);
    }
}
