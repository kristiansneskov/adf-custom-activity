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
            string outputBlobPath = "us-sales-kpi/output";
            string inputBlobPath = "us-sales-kpi";
            string outputFile1 = "output_from_r.txt";
            string outputFile2 = "output_from_r.txt";
            string rScriptName = "etl.R";

            var consoleLogger = new ConsoleLogger();
            const string accountName = "labarlaetl";
            const string accountKey =
                "";
            var connectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                accountName, accountKey);

            consoleLogger.Write("Azure storage connection string {0}", connectionString);

            string[] blobNames = new[] { rScriptName, inputDataFile };
            RExecutionActivity.InvokeR(connectionString,  inputBlobPath, blobNames, outputBlobPath, outputFile1, outputFile2, consoleLogger);
            Console.WriteLine("All done");
            Console.ReadLine();
        }
    }
}
