using System;

namespace OrleansHost
{
    /// <summary>
    /// Orleans test host
    /// </summary>
    public class Program
    {
        private static OrleansHostWrapper hostWrapper;
        static int Main(string[] args)
        {
            int returnCode = StartSilo(args);

            Console.WriteLine("Press Enter to terminate...");
            Console.ReadLine();

            returnCode += ShutdownSilo();
            return returnCode;//either StartSilo or ShutdownSilo failed would result on a non-zero return code. 
        }


        private static int StartSilo(string[] args)
        {
            hostWrapper = new OrleansHostWrapper(args);
            return hostWrapper.Run();
        }

        private static int ShutdownSilo()
        {
            if (hostWrapper != null)
            {
                return hostWrapper.Stop();
            }
            return 0;
        }
    }
}
