using System;
using System.Globalization;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using Orleans.Runtime.Startup;
using Orleans.TestingHost;
using Tester;
using UnitTests.GrainInterfaces;
using UnitTests.Grains;
using UnitTests.Tester;
using Xunit;

namespace UnitTests.General
{
    [TestCategory("DI")]
    public class DependencyInjectionGrainTestsWithSiloHostBuilder
    {

        [Fact, TestCategory("BVT"), TestCategory("Functional")]
        public async Task CanGetGrainWithSIngletonInjectedDependencies()
        {
            AppDomain appDomain =  AppDomain.CreateDomain("OrleansHost", null, GetAppDomainSetupInfo());

            var config = ClientConfiguration.LocalhostSilo();
            GrainClient.Initialize(config);

            var grain1 = GrainClient.GrainFactory.GetGrain<IDIGrainWithInjectedServices>(0);
            var grain2 = GrainClient.GrainFactory.GetGrain<IDIGrainWithInjectedServices>(1);
            // the injected service will return the same value only if it's the same instance
            Assert.Equal(
                await grain1.GetStringValue(),
                await grain2.GetStringValue());
        }

        private static AppDomainSetup GetAppDomainSetupInfo()
        {
            var currentAppDomain = AppDomain.CurrentDomain;

            return new AppDomainSetup
            {
                ApplicationBase = Environment.CurrentDirectory,
                ConfigurationFile = currentAppDomain.SetupInformation.ConfigurationFile,
                ShadowCopyFiles = currentAppDomain.SetupInformation.ShadowCopyFiles,
                ShadowCopyDirectories = currentAppDomain.SetupInformation.ShadowCopyDirectories,
                CachePath = currentAppDomain.SetupInformation.CachePath,
                AppDomainInitializer = InitSilo,
            };
        }

        public static void InitSilo(string[] args)
        {
            siloHost = new SiloHostBuilder()
                .UseClusterConfiguration(ClusterConfiguration.LocalhostPrimarySilo())
                .UseStartup<TestStartup>()
                .Build("TestSilo");
            siloHost.InitializeOrleansSilo();
            siloHost.StartOrleansSilo();
        }

        private static SiloHost siloHost;
       
        public class TestStartup
        {
            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
                services.AddSingleton<IInjectedService, InjectedService>();
                return services.BuildServiceProvider();
            }
        }
    }
}
