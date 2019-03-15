namespace MisCritAz.Messaging
{
    public class ServiceBusConnectionSettings
    {
        public string PrimaryServiceBusConnectionStringForSend { get; set; }
        public string SecondaryServiceBusConnectionStringForSend { get; set; }
        public string ServiceBusTopic { get; set; }
        public string ServiceBusSubscription { get; set; }
    }
}
