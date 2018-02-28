using System;
using System.Collections.Generic;
using System.Text;
using Orleans.Hosting;

namespace Orleans.Runtime.Streams
{
    public class Sample
    {
        public void main()
        {
            var siloBuilder = new SiloHostBuilder()
                .AddPersistentStream("EH")
                .ConfigureQueueAdapter<AzureQueueAdapter, AzureQueueAdapterOptions>(options => ())
                .ConfigureQueueBalancer<>
                .Builder()
        }
    }
}
