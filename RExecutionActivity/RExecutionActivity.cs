using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;

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
            /*
            string usDataFile;
            extendedProperties.TryGetValue("usDataFile", out usDataFile);


            string blobPath;
            extendedProperties.TryGetValue("blobPath", out blobPath);
            string churnTagFile;
            extendedProperties.TryGetValue("churnTagFile", out churnTagFile);
            string outputFile;
            extendedProperties.TryGetValue("outputFile", out outputFile);
            */
            // to log information, use the logger object
            // log all extended properties            
            logger.Write("Logging extended properties if any...");
            foreach (var entry in extendedProperties)
                logger.Write("<key:{0}> <value:{1}>", entry.Key, entry.Value);


            string inputDataFile = "file.txt";
            string outputBlobPath = "output";
            string outputFile = "output_from_r.txt";

            logger.Write("Starting Batch Execution Service");

            InvokeR(inputDataFile, outputBlobPath, outputFile, logger);

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
        ///     Invoke RScript.exe and run the R script
        /// </summary>
        /// <param name="experimentType"></param>
        /// <param name="snapShotFile"></param>
        /// <param name="timeSeriesFile"></param>
        /// <param name="churnTagFile"></param>
        /// <param name="blobPath"></param>
        /// <param name="outputFile"></param>
        /// <param name="logger"></param>
        public static void InvokeR(string inputDataFile, string outputBlobPath, string outputFile, IActivityLogger logger)
        {
            const string accountName = "labarlaetl";
            const string accountKey =
                "";
            var connectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                accountName, accountKey);

            logger.Write("Azure storage connection string {0}", connectionString);


            var process = new Process();

            try
            {
                string pathToRExecutable;
                string[] blobNames;
                const string containerName = "us-sales-kpi";

                pathToRExecutable = "etl.R";
                blobNames = new[] {pathToRExecutable, inputDataFile };


                var resultBlobPath = string.Format("{0}/{1}", containerName, outputBlobPath);

                logger.Write("Creating working directory");
                logger.Write(string.Format("Machine Name: {0}", Environment.MachineName));

                var workingDirectory = new FileInfo(typeof(RExecutionActivity).Assembly.Location).DirectoryName;
                logger.Write(string.Format("Directory Name : {0}", workingDirectory));

                logger.Write(string.Format("Working directory created: {0}", workingDirectory));


                logger.Write("Download and unpack R binaries");
                var RBinariesContainerName = "rbinaries";
                var RBinariesBlobName = "R-3.3.3.zip";

                DownloadInputFiles(workingDirectory, connectionString, RBinariesContainerName, new[] {RBinariesBlobName});

                //var downloadedRBinariesPath = @"C:\temp\custom-activity";
                //var RBinariesZipFileName = "R-3.3.3.zip";
                Console.WriteLine("Finished downloading...");

                try
                {
                    ZipFile.ExtractToDirectory(Path.Combine(workingDirectory, RBinariesBlobName), workingDirectory);

                }
                catch (IOException e)
                {
                    Console.WriteLine(e.Message);
                    Console.WriteLine("ignoring");
                }

                
                // DirectoryCopy(@"C:\R-3.3.3", workingDirectory, true);
                
                logger.Write("Downloading input files used by this sample to the Working Directory");

                
                var inputFileNames = DownloadInputFiles(workingDirectory, connectionString, containerName, blobNames);

                var index = 0;
                for (; index < inputFileNames.Length; index++)
                {
                    var file = inputFileNames[index];
                    if (File.Exists(file))
                    {
                        logger.Write(String.Format("File : {0} exists", file));
                    }
                }

                logger.Write("Input Files Download completed");
                
                //Note this assumes the data, and r etl logic lies in the same container (but different blobs)
                pathToRExecutable = inputFileNames[0];
                var inputData = inputFileNames[1];
                
                
                //var usFile = "name-of-raw-data-file";
                string args;
                var outputFileName = String.Format("{0}\\{1}", workingDirectory, outputFile);

                logger.Write(String.Format("Output file name : {0}", outputFileName));

                
                args = String.Format("{0} {1}", inputData, outputFileName);
                logger.Write(String.Format("Arguments in training are : {0}", args));
               
                //Copy R file containing etl logic into working directory
                //pathToRExecutable = Path.Combine(workingDirectory, "etl.r");
                //File.Copy(@"C:/Dev/Playground/RWorkspace/CustomRScript\etl.r", pathToRExecutable, overwrite: true);
                logger.Write(String.Format("R script path: {0} ", pathToRExecutable));
                //Copy data files into working directory
                //TODO



                
                /////R execution/////

                ProcessStartInfo startInfo = new ProcessStartInfo
                {
                    WindowStyle = ProcessWindowStyle.Hidden,
                    UseShellExecute = false,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                };

                logger.Write(File.Exists(String.Format("{0}{1}", workingDirectory, @"\R-3.3.3\bin\x64\Rscript.exe"))
                    ? "R File exists"
                    : "R file does not exist");

                startInfo.FileName = String.Format("{0}{1}", workingDirectory, @"\R-3.3.3\bin\x64\Rscript.exe");
                startInfo.Arguments = String.Format("{0} {1}", pathToRExecutable, args);
                if (workingDirectory != null) startInfo.WorkingDirectory = workingDirectory;
                logger.Write("R Execution started");
                process.StartInfo = startInfo;
                process.Start();

                logger.Write(String.Format("Process started with process id : {0} on machine : {1}", process.Id, process.MachineName));

                var errorReader = process.StandardError;
                var outputReader = process.StandardOutput;

                while (!outputReader.EndOfStream)
                {
                    var text = outputReader.ReadLine();
                    logger.Write(text);
                }

                logger.Write("output reader complete");

                while (!errorReader.EndOfStream)
                {
                    errorReader.ReadLine();
                }

                logger.Write(String.Format("Standard Output : {0}", process.StandardOutput.ReadToEnd()));
                logger.Write(String.Format("Standard Error: {0}", process.StandardError.ReadToEnd()));

                logger.Write("output reader end of stream complete");

                process.WaitForExit();

                while (!process.HasExited)
                {
                    logger.Write("R is still running");
                }

                logger.Write(String.Format("Process start time : {0}, end time : {1}", process.StartTime, process.ExitTime));
                
                /////Upload file/////
                if (File.Exists(outputFileName))
                {
                    logger.Write("Uploading file started");

                    UploadFile(connectionString, resultBlobPath, outputFileName, outputFile);
                }
                else
                {
                    logger.Write("output file not found");
                }
                

            }
            catch (Exception ex)
            {
                logger.Write(string.Format("Exception is : {0}", ex.Message));
            }
        }

        /// <summary>
        ///     Upload file to  Azure Blob
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="containerName"></param>
        /// <param name="filePath"></param>
        /// <param name="fileName"></param>
        private static void UploadFile(string connectionString, string containerName, string filePath, string fileName)
        {
            var storageAccount =
                CloudStorageAccount.Parse(connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(fileName);
            blob.UploadFromFile(filePath, FileMode.Open);
            Console.WriteLine("File upload completed");
        }

        /// <summary>
        ///     Download input files from Azure blob
        /// </summary>
        /// <param name="workingDirectory"></param>
        /// <param name="connectionString"></param>
        /// <param name="containerName"></param>
        /// <param name="blobNames"></param>
        /// <returns></returns>
        private static string[] DownloadInputFiles(string workingDirectory, string connectionString,
            string containerName, string[] blobNames)
        {
            var inputStorageAccount =
                CloudStorageAccount.Parse(connectionString);
            var inputClient = inputStorageAccount.CreateCloudBlobClient();
            var container = inputClient.GetContainerReference(containerName);
            var inputFiles = new string[blobNames.Length];

            for (var blobCnt = 0; blobCnt < blobNames.Length; blobCnt++)
            {
                var blobName = blobNames[blobCnt];
                var blockBlob =
                    container.GetBlockBlobReference(blobName);

                using (var fileStream =
                    File.OpenWrite(Path.Combine(workingDirectory, blockBlob.Name)))
                {
                    blockBlob.DownloadToStream(fileStream);

                    inputFiles[blobCnt] = Path.Combine(workingDirectory, blockBlob.Name);
                }
            }

            return inputFiles;
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