using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Hosting;
using Orleans.Streams;
using Orleans.Providers.Streams.Common;
using Orleans.Providers;
using Orleans.Configuration;
using Orleans.Providers.Streams.SimpleMessageStream;

namespace Orleans.Hosting
{
    public interface IPersistentStreamProviderConfigurator : INamedServiceConfigurator
    {
        PersistentStreamProviderBuilder ConfigureQueueAdapter<TOptions, TQueueAdapter>(
            Action<TOptions> configureOptions)
            where TQueueAdapter : class, IQueueAdapter
            where TOptions : class, new();

        PersistentStreamProviderBuilder ConfigureQueueBalancer<TOptions, TQueueBalancer>(
            Action<TOptions> configureOptions)
            where TQueueBalancer : class, IStreamQueueBalancer
            where TOptions : class, new();

        PersistentStreamProviderBuilder ConfigureQueueAdapterReceiver<TOptions, TQueueAdapterReceiver>(
            Action<TOptions> configureOptions)
            where TQueueAdapterReceiver : class, IQueueAdapterReceiver
            where TOptions : class, new();

        PersistentStreamProviderBuilder ConfigureQueueMapper<TOptions, TQueueMapper>(Action<TOptions> configureOptions)
            where TQueueMapper : class, IStreamQueueMapper
            where TOptions : class, new ();
    }

    public interface INamedServiceConfigurator
    {
        INamedServiceConfigurator ConfigureComponent<TOptions, TComponent>(Action<TOptions> configureOptions)
            where TComponent : class
            where TOptions : class, new();
    }

    public class NamedServiceConfigurator : INamedServiceConfigurator
    {
        private readonly string name;
        private readonly IServiceCollection services;
        private readonly ISiloHostBuilder builder;
        public NamedServiceConfigurator(string name, IServiceCollection services)
        {
            this.name = name;
            this.services = services;
        }

        public NamedServiceConfigurator(string name, ISiloHostBuilder builder)
        {
            this.name = name;
            this.builder = builder;
        }

        public INamedServiceConfigurator ConfigureComponent<TOptions, TComponent>(Action<TOptions> configureOptions)
            where TComponent : class
            where TOptions : class, new()
        {
            if (services != null)
            {
                services.Configure<TOptions>(name, configureOptions);
                services.AddSingletonNamedService<TComponent>(name, (s, n) => ActivatorUtilities.CreateInstance<TComponent>(s, name, s.GetService<IOptionsSnapshot<TOptions>>().Get(name)));
            }
            if (builder != null)
            {
                builder.ConfigureServices(s =>
                {
                    s.Configure<TOptions>(name, configureOptions);
                    s.AddSingletonNamedService<TComponent>(name,
                        (sp, n) => ActivatorUtilities.CreateInstance<TComponent>(sp, name,
                            sp.GetService<IOptionsSnapshot<TOptions>>().Get(name)));
                });
            }
            return this;
        }
    }

    public class PersistentStreamProviderBuilder : IPersistentStreamProviderConfigurator
    {
        private readonly NamedServiceConfigurator namedServiceBuilder;
        public PersistentStreamProviderBuilder(string name, IServiceCollection services)
        {
            this.namedServiceBuilder = new NamedServiceConfigurator(name, services);
        }

        public PersistentStreamProviderBuilder(string name, ISiloHostBuilder builder)
        {
            this.namedServiceBuilder = new NamedServiceConfigurator(name, builder);
        }

        public PersistentStreamProviderBuilder ConfigureQueueAdapter<TOptions, TQueueAdapter>(Action<TOptions> configureOptions)
            where TQueueAdapter: class, IQueueAdapter
            where TOptions : class, new()
        {
            namedServiceBuilder.ConfigureComponent<TOptions, TQueueAdapter>(configureOptions);
            return this;
        }

        public PersistentStreamProviderBuilder ConfigureQueueBalancer<TOptions, TQueueBalancer>(Action<TOptions> configureOptions)
            where TQueueBalancer : class, IStreamQueueBalancer
            where TOptions : class, new()
        {
            namedServiceBuilder.ConfigureComponent<TOptions, TQueueBalancer>(configureOptions);
            return this;
        }

        public PersistentStreamProviderBuilder ConfigureQueueAdapterReceiver<TOptions, TQueueAdapterReceiver>(Action<TOptions> configureOptions)
            where TQueueAdapterReceiver : class, IQueueAdapterReceiver
            where TOptions : class, new()
        {
            namedServiceBuilder.ConfigureComponent<TOptions, TQueueAdapterReceiver>(configureOptions);
            return this;
        }

        public PersistentStreamProviderBuilder ConfigureQueueMapper<TOptions, TQueueMapper>(Action<TOptions> configureOptions)
            where TQueueMapper : class, IStreamQueueMapper
            where TOptions : class, new()
        {
            namedServiceBuilder.ConfigureComponent<TOptions, TQueueMapper>(configureOptions);
            return this;
        }
        
        void Validate()
        {
            //validate if all components configured
        }
    }

    public static class PersistentStreamProviderBuilderExtensions
    {
        public static PersistentStreamProviderBuilder AddPersistentStream(this ISiloHostBuilder builder, string name)
        {
            var providerBuilder =  new PersistentStreamProviderBuilder(name, builder);
            return providerBuilder;
        }

        public static PersistentStreamProviderBuilder AddPersistentStreams(this IServiceCollection services, string name)
        {
            var builder = new PersistentStreamProviderBuilder(name, services);
            //services.AddSingletonNamedService<PersistentStreamProviderBuilder>(name, (s, n)=>builder);
            return builder;
        }
    }

    public static class StreamHostingExtensions
    {
        /// <summary>
        /// Configure silo to use persistent streams.
        /// </summary>
        public static ISiloHostBuilder AddPersistentStreams<TOptions>(this ISiloHostBuilder builder, string name, Func<IServiceProvider, string, IQueueAdapterFactory> adapterFactory, Action<TOptions> configureOptions)
            where TOptions : PersistentStreamOptions, new()
        {
            return builder.ConfigureServices(services => services.AddSiloPersistentStreams<TOptions>(name, adapterFactory, configureOptions));
        }

        /// <summary>
        /// Configure silo to use persistent streams.
        /// </summary>
        public static ISiloHostBuilder AddPersistentStreams<TOptions>(this ISiloHostBuilder builder, string name, Func<IServiceProvider, string, IQueueAdapterFactory> adapterFactory, Action<OptionsBuilder<TOptions>> configureOptions = null)
            where TOptions : PersistentStreamOptions, new()
        {
            return builder.ConfigureServices(services => services.AddSiloPersistentStreams<TOptions>(name, adapterFactory, configureOptions));
        }

        /// <summary>
        /// Configure silo to use persistent streams.
        /// </summary>
        public static IServiceCollection AddSiloPersistentStreams<TOptions>(this IServiceCollection services, string name, Func<IServiceProvider, string, IQueueAdapterFactory> adapterFactory, Action<TOptions> configureOptions)
            where TOptions : PersistentStreamOptions, new()
        {
            return services.AddSiloPersistentStreams<TOptions>(name, adapterFactory, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use persistent streams.
        /// </summary>
        public static IServiceCollection AddSiloPersistentStreams<TOptions>(this IServiceCollection services, string name, Func<IServiceProvider, string,IQueueAdapterFactory> adapterFactory,
            Action<OptionsBuilder<TOptions>> configureOptions = null)
            where TOptions : PersistentStreamOptions, new()
        {
            configureOptions?.Invoke(services.AddOptions<TOptions>(name));
            return services.AddSingletonNamedService<IStreamProvider>(name, PersistentStreamProvider.Create<TOptions>)
                           .AddSingletonNamedService<ILifecycleParticipant<ISiloLifecycle>>(name, (s, n) => ((PersistentStreamProvider)s.GetRequiredServiceByName<IStreamProvider>(n)).ParticipateIn<ISiloLifecycle>())
                           .AddSingletonNamedService<IQueueAdapterFactory>(name, adapterFactory)
                           .AddSingletonNamedService(name, (s, n) => s.GetServiceByName<IStreamProvider>(n) as IControllable);
        }

        /// <summary>
        /// Configure silo to use SimpleMessageProvider
        /// </summary>
        public static ISiloHostBuilder AddSimpleMessageStreamProvider(this ISiloHostBuilder builder, string name,
            Action<SimpleMessageStreamProviderOptions> configureOptions)

        {
            return builder.ConfigureServices(services =>
                services.AddSiloSimpleMessageStreamProvider(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use SimpleMessageProvider
        /// </summary>
        public static ISiloHostBuilder AddSimpleMessageStreamProvider(this ISiloHostBuilder builder, string name,
            Action<OptionsBuilder<SimpleMessageStreamProviderOptions>> configureOptions = null)

        {
            return builder.ConfigureServices(services =>
                services.AddSiloSimpleMessageStreamProvider(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use simple message stream provider
        /// </summary>
        public static IServiceCollection AddSiloSimpleMessageStreamProvider(this IServiceCollection services, string name,
            Action<SimpleMessageStreamProviderOptions> configureOptions = null)
        {
            return services.AddSiloSimpleMessageStreamProvider(name, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use simple message provider
        /// </summary>
        public static IServiceCollection AddSiloSimpleMessageStreamProvider(this IServiceCollection services, string name,
        Action<OptionsBuilder<SimpleMessageStreamProviderOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<SimpleMessageStreamProviderOptions>(name));
            return services.ConfigureNamedOptionForLogging<SimpleMessageStreamProviderOptions>(name)
                           .AddSingletonNamedService<IStreamProvider>(name, SimpleMessageStreamProvider.Create);
        }
    }
}
