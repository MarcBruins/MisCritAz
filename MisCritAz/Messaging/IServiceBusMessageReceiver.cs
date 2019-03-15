using Microsoft.Extensions.Hosting;
using System;

namespace MisCritAz.Messaging
{
    /// <summary>
    /// Type that receives messages from service bus.
    /// </summary>
    public interface IServiceBusMessageReceiver : IHostedService, IDisposable
    {
    }
}
