using System;
using System.Threading;
using System.Threading.Tasks;
using HelloWorld.Interfaces;
using Orleans;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;

namespace OrleansClient
{
    /// <summary>
    /// Orleans test silo client
    /// </summary>
    public class Program
    {
        static int Main(string[] args)
        {
            var config = ClientConfiguration.LocalhostSilo();
            const int initializeAttemptsBeforeFailing = 5;
            int attemp = 0;
            while (true)
            {
                try
                {
                    GrainClient.Initialize(config);
                    Console.WriteLine("Client successfully connect to silo host");
                    break;
                }
                catch (SiloUnavailableException e)
                {
                    attemp++;
                    if (attemp > initializeAttemptsBeforeFailing)
                    {
                        Console.WriteLine("Connect to silo host failed due to " + e);
                        Console.WriteLine("Press Enter to terminate...");
                        Console.ReadLine();
                        return 1;
                    }
                    Thread.Sleep(TimeSpan.FromSeconds(2));
                }
            }

            DoClientWork().Wait();
            Console.WriteLine("Press Enter to terminate...");
            Console.ReadLine();
            return 0;

        }

        private static async Task DoClientWork()
        {
            var friend = GrainClient.GrainFactory.GetGrain<IHello>(0);
            var response = await friend.SayHello("Good morning, my friend!");
            Console.WriteLine("\n\n{0}\n\n", response);
        }

    }
}
