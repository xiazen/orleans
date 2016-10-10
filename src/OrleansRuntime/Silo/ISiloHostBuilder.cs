using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;

namespace Orleans.Runtime.Startup
{
    public interface ISiloHostBuilder
    {
        SiloHost Build(string siloName);
        ISiloHostBuilder ConfigureLogging(System.Action<Microsoft.Extensions.Logging.ILoggerFactory> loggerFactoryDelegate);
        ISiloHostBuilder ConfigureServices(System.Action<Microsoft.Extensions.DependencyInjection.IServiceCollection> configServiceDelegate);

        ISiloHostBuilder UseLoggerFactory(Microsoft.Extensions.Logging.ILoggerFactory loggerFactory);

        ISiloHostBuilder UseClusterConfiguration(ClusterConfiguration clusterConfiguration);

        ISiloHostBuilder UseStartup<TStartup>();
    }
}
