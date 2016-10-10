using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;

namespace Orleans.Runtime.Startup
{
    public class SiloHostBuilder : ISiloHostBuilder
    {
        private Microsoft.Extensions.Logging.ILoggerFactory loggerFactory;
        private List<Action<IServiceCollection>> externalServiceConfigDelegates;
        private ClusterConfiguration clusterConfiguration;
        private string startUpType;
        public SiloHostBuilder()
        {
        }

        public ISiloHostBuilder UseStartup<TStartUp>()
        {
            this.startUpType = typeof(TStartUp).AssemblyQualifiedName;
            return this;
        }

        public ISiloHostBuilder ConfigureLogging(System.Action<Microsoft.Extensions.Logging.ILoggerFactory> loggerFactoryDelegate)
        {
            if (loggerFactoryDelegate == null)
            {
                throw new ArgumentNullException(nameof(loggerFactoryDelegate));
            }
            loggerFactoryDelegate(loggerFactory);
            return this;
        }

        public ISiloHostBuilder ConfigureServices(
            System.Action<Microsoft.Extensions.DependencyInjection.IServiceCollection> configServiceDelegate)
        {
            if (configServiceDelegate == null)
            {
                throw new ArgumentNullException(nameof(configServiceDelegate));
            }
            externalServiceConfigDelegates.Add(configServiceDelegate);
            return this;
        }

        public ISiloHostBuilder UseLoggerFactory(Microsoft.Extensions.Logging.ILoggerFactory loggerFactory)
        {
            if (loggerFactory == null)
            {
                throw new ArgumentNullException(nameof(loggerFactory));
            }
            this.loggerFactory = loggerFactory;
            return this;
        }

        public ISiloHostBuilder UseClusterConfiguration(ClusterConfiguration clusterConfiguration)
        {
            this.clusterConfiguration = clusterConfiguration;
            return this;
        }

        public SiloHost Build(string siloName)
        {
            // concept of internal and external service provider. 
            // internal service provider only register system types, and use our own specified DI container, only used by other system types
            // external service provider can use whatever DI container user want and it can contain system types which can be used for external
            // external service provider mainly used in GrainCreator to create Grain with DI, or can potentially used in creating providers
            IServiceProvider internalServiceProvider = StartupBuilder.RegisterAllSystemTypes(new ServiceCollection()).BuildServiceProvider();
            IServiceProvider externalServiceProvider = null;
            bool useCustomServiceProvider;
            if (externalServiceConfigDelegates != null)
            {
                IServiceCollection externalCollection = null;
                foreach (var configServiceDelegate in externalServiceConfigDelegates)
                {
                    configServiceDelegate(externalCollection);
                }
                StartupBuilder.RegisterSystemTypesWhichCanbeUsedByExternal(internalServiceProvider, externalCollection);
                externalServiceProvider = externalCollection.BuildServiceProvider();
                useCustomServiceProvider = true;
            }else if (this.startUpType != null)
            {
                externalServiceProvider = StartupBuilder.ConfigureStartup(this.startUpType, internalServiceProvider, out useCustomServiceProvider);
            }
            else
            {
                // if user didn't inject any services into DI
                IServiceCollection externalCollection = new ServiceCollection();
                StartupBuilder.RegisterSystemTypesWhichCanbeUsedByExternal(internalServiceProvider, externalCollection);
                useCustomServiceProvider = false;
                externalServiceProvider = externalCollection.BuildServiceProvider();
            }

            return new SiloHost(siloName, clusterConfiguration, internalServiceProvider, 
                externalServiceProvider, useCustomServiceProvider);
        }
    }
}
