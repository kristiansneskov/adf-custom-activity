using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ExecuteRScriptWithCustomActivity;

namespace RExecutionActivityStartup
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting app");

            string inputDataFile = "file.txt";
            string outputBlobPath = "output";
            string outputFile = "output_from_r.txt";
            var consoleLogger = new ConsoleLogger();
            
            RExecutionActivity.InvokeR(inputDataFile, outputBlobPath, outputFile, consoleLogger);
            Console.WriteLine("All done");
            Console.ReadLine();
        }
    }
}
