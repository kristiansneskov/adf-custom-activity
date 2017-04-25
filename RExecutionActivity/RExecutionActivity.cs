using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Blob;
using Activity = Microsoft.Azure.Management.DataFactories.Models.Activity;

namespace ExecuteRScriptWithCustomActivity
{
    public class RExecutionActivity : IDotNetActivity
    {
        /// <summary>
        ///     Override Execute method in ADF .Net Activity
        /// </summary>
        /// <param name="linkedServices"></param>
        /// <param name="datasets"></param>
        /// <param name="activity"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices,
            IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            logger.Write("Executing R with ADF .Net Activity");

            AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;

            var extendedProperties = ((DotNetActivity) activity.TypeProperties).ExtendedProperties;

            //These file paths are specific to the R script.  We pass the file paths to our R script as parameters via the pipeline json

            string orderData;
            extendedProperties.TryGetValue("orderData", out orderData);
            string currencyData;
            extendedProperties.TryGetValue("currencyData", out currencyData);
            string forecastSalesData;
            extendedProperties.TryGetValue("forecastSalesData", out forecastSalesData);
            string fiscalDimData;
            extendedProperties.TryGetValue("fiscalDimData", out fiscalDimData);
            string dayDimData;
            extendedProperties.TryGetValue("dayDimData", out dayDimData);
            string rHelper1;
            extendedProperties.TryGetValue("rHelper1", out rHelper1);
            string rHelper2;
            extendedProperties.TryGetValue("rHelper2", out rHelper2);
            string dailyOutputFileName;
            extendedProperties.TryGetValue("dailyFileName", out dailyOutputFileName);
            string monthlyOutputFileName;
            extendedProperties.TryGetValue("monthlyFileName", out monthlyOutputFileName);
            string rscriptPath;
            extendedProperties.TryGetValue("rScript", out rscriptPath);

            // to log information, use the logger object
            // log all extended properties            
            logger.Write("Logging extended properties if any...");
            foreach (var entry in extendedProperties)
                logger.Write("<key:{0}> <value:{1}>", entry.Key, entry.Value);

            // linked service for input and output data stores
            // in this example, same storage is used for both input/output
            AzureStorageLinkedService inputLinkedService;
            
            // get the input dataset
            Dataset inputDataset = datasets.Single(dataset => dataset.Name == activity.Inputs.Single().Name);

            //Get the folder path from the input data set definition
            string inputBlobPath = GetFolderPath(inputDataset);
            
            // get the output dataset
            Dataset outputDataset = datasets.Single(dataset => dataset.Name == activity.Outputs.Single().Name);

            //Get the folder path from the input data set definition
            string outputBlobPath = GetFolderPath(outputDataset);
  
            // get the first Azure Storate linked service from linkedServices object
            // using First method instead of Single since we are using the same
            // Azure Storage linked service for input and output.
            inputLinkedService = linkedServices.First(
                linkedService =>
                linkedService.Name ==
                inputDataset.Properties.LinkedServiceName).Properties.TypeProperties
                as AzureStorageLinkedService;

            // get the connection string in the linked service
            string connectionString = inputLinkedService.ConnectionString;
            
            logger.Write("Starting Batch Execution Service");

            string[] blobNames = new[] { rscriptPath, orderData, currencyData, forecastSalesData, dayDimData, fiscalDimData, rHelper1, rHelper2 };

            new RInvoker(logger).Invoke(connectionString, inputBlobPath, blobNames, outputBlobPath, dailyOutputFileName, monthlyOutputFileName);

         //   InvokeR(connectionString, inputBlobPath, blobNames, outputBlobPath, dailyOutputFileName, monthlyOutputFileName, logger);

            return new Dictionary<string, string>();
        }

        /// <summary>
        ///     Resolve local path on HDInsight VM where the ADF .Net activity is running
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            var assemblyname = new AssemblyName(args.Name).Name;
            if (assemblyname.Contains("Microsoft.WindowsAzure.Storage"))
            {
                assemblyname = "Microsoft.WindowsAzure.Storage";
                var assemblyFileName = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                    assemblyname + ".dll");
                var assembly = Assembly.LoadFrom(assemblyFileName);
                return assembly;
            }
            return null;
        }


        /// <summary>
        /// Gets the folderPath value from the input/output dataset.
        /// </summary>

        private static string GetFolderPath(Dataset dataArtifact)
        {
            if (dataArtifact == null || dataArtifact.Properties == null)
            {
                return null;
            }

            // get type properties of the dataset   
            AzureBlobDataset blobDataset = dataArtifact.Properties.TypeProperties as AzureBlobDataset;
            if (blobDataset == null)
            {
                return null;
            }

            // return the folder path found in the type properties
            return blobDataset.FolderPath;
        }


        /// <summary>
        ///     Copy files from source to destination directory recursively
        /// </summary>
        /// <param name="sourceDirName"></param>
        /// <param name="destDirName"></param>
        /// <param name="copySubDirs"></param>
        private static void DirectoryCopy(string sourceDirName, string destDirName, bool copySubDirs)
        {
            // Get the subdirectories for the specified directory.
            var dir = new DirectoryInfo(sourceDirName);

            if (!dir.Exists)
                throw new DirectoryNotFoundException(
                    "Source directory does not exist or could not be found: "
                    + sourceDirName);

            var dirs = dir.GetDirectories();
            // If the destination directory doesn't exist, create it.
            if (!Directory.Exists(destDirName))
                Directory.CreateDirectory(destDirName);

            // Get the files in the directory and copy them to the new location.
            var files = dir.GetFiles();
            foreach (var file in files)
            {
                var temppath = Path.Combine(destDirName, file.Name);
                file.CopyTo(temppath, false);
            }

            // If copying subdirectories, copy them and their contents to new location.
            if (!copySubDirs) return;
            foreach (var subdir in dirs)
            {
                var temppath = Path.Combine(destDirName, subdir.Name);
                DirectoryCopy(subdir.FullName, temppath, true);
            }
        }
    }
}