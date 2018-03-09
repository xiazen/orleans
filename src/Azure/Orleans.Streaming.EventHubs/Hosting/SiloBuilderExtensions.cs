using System;
using Orleans.Configuration;
using Orleans.ServiceBus.Providers;
using Orleans.Streams;

namespace Orleans.Hosting
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use event hub persistent streams. This returns a configurator which allows further configuration.
        /// </summary>
        public static SiloEventHubStreamConfigurator ConfigureEventHubStreams(
            this ISiloHostBuilder builder,
            string name)
        {
            return new SiloEventHubStreamConfigurator(name, builder);
        }

        /// <summary>
        /// Configure silo to use event hub persistent streams with default check pointer and other settings
        /// </summary>
        public static ISiloHostBuilder AddEventHubStreams(
            this ISiloHostBuilder builder,
            string name, Action<EventHubOptions> configureEventHub, Action<AzureTableStreamCheckpointerOptions> configureDefaultCheckpointer)
        {
            builder.ConfigureEventHubStreams(name)
                .ConfigureEventHub(ob => ob.Configure(configureEventHub))
                .UseEventHubCheckpointer(ob => ob.Configure(configureDefaultCheckpointer));
            return builder;
        }
    }
}