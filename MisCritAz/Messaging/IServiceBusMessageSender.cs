using System;
using System.Threading.Tasks;

namespace MisCritAz.Messaging
{
    public interface IServiceBusMessageSender : IDisposable
    {
        /// <summary>
        /// Sends a message to a service bus topic.
        /// </summary>
        Task ProcessMessageImpl(SampleMessage message);

        /// <summary>
        /// Prepares instance for use.
        /// </summary>
        Task Initialize();
    }
}