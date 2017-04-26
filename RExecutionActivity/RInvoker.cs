using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.WindowsAzure.Storage;

namespace ExecuteRScriptWithCustomActivity
{
    public class RInvoker
    {
        private readonly IActivityLogger _logger;

        public RInvoker(IActivityLogger logger)
        {
            _logger = logger;
        }

        public void Invoke(string connectionString, string inputBlobPath, string[] blobNames, string outputBlobPath, string dailyOutputFileName, string monthlyOutputFileName)
        {
            using (var process = new Process())
            {
                try
                {

                    _logger.Write(String.Format("Machine Name: {0}", Environment.MachineName));

                    var workingDirectory = new FileInfo(typeof(RExecutionActivity).Assembly.Location).DirectoryName;
                    _logger.Write(String.Format("Directory Name : {0}", workingDirectory));

                    _logger.Write(String.Format("Working directory detected: {0}", workingDirectory));

                    string[] inputFileNames = DownloadAllInputFiles(_logger, workingDirectory, connectionString,
                        inputBlobPath, blobNames);

                    _logger.Write("Input Files Download completed");

                    //Note this assumes the data, and r etl logic lies in the same container (but different blobs)
                    string pathToRScript = inputFileNames[0];
                    //   var inputData = inputFileNames[1];

                    string args;
                    var dailyOutputPath = String.Format("{0}\\{1}", workingDirectory, dailyOutputFileName);
                    var monthlyOutputPath = String.Format("{0}\\{1}", workingDirectory, monthlyOutputFileName);


                    _logger.Write(String.Format("Daily output file name : {0}", dailyOutputPath));
                    _logger.Write(String.Format("¨Monthly output file name : {0}", monthlyOutputPath));



                    args = String.Format("{0} {1} {2}", workingDirectory, dailyOutputPath, monthlyOutputPath);
                    _logger.Write(String.Format("Arguments in etl are : {0}", args));

                    _logger.Write(String.Format("R script path: {0} ", pathToRScript));


                    /////R execution/////
                    ExecuteRProcess(_logger, workingDirectory, pathToRScript, args, process);


                    /////Upload file/////
                    if (File.Exists(dailyOutputPath))
                    {
                        _logger.Write("Uploading daily file started");

                        UploadFile(connectionString, outputBlobPath, dailyOutputPath, dailyOutputFileName);
                    }
                    else
                    {
                        _logger.Write("daily output file not found");
                    }


                    /////Upload file/////
                    if (File.Exists(monthlyOutputPath))
                    {
                        _logger.Write("Uploading monthly file started");

                        UploadFile(connectionString, outputBlobPath, monthlyOutputPath, monthlyOutputFileName);
                    }
                    else
                    {
                        _logger.Write("monthly output file not found");
                    }


                }
                catch (ETLException ex)
                {
                    _logger.Write(String.Format("Detected ETL error in R code: {0}",ex.Message));
                    
                    throw ex;
                }
                catch (Exception ex)
                {
                    _logger.Write(String.Format("Exception is : {0}", ex.Message));

                    throw ex;
                }
            }
        }


        private static void ExecuteRProcess(IActivityLogger logger, string workingDirectory, string pathToRScript, string args, Process process)
        {
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                WindowStyle = ProcessWindowStyle.Hidden,
                UseShellExecute = false,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                CreateNoWindow = true
            };

            var rBinariesExists = File.Exists(String.Format("{0}{1}", workingDirectory, @"\R-3.3.3\bin\x64\Rscript.exe"));

            if (!rBinariesExists)
            {
                throw new ETLException(String.Format("RScript.exe does not exist at {0}",
                    String.Format("{0}{1}", workingDirectory, @"\R-3.3.3\bin\x64\Rscript.exe")));
            }
            
            startInfo.FileName = String.Format("{0}{1}", workingDirectory, @"\R-3.3.3\bin\x64\Rscript.exe");
            startInfo.Arguments = String.Format("{0} {1}", pathToRScript, args);
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
                var errorText = errorReader.ReadLine();
                if (!String.IsNullOrWhiteSpace(errorText))
                {
                    // If warning message from R => ignore otherwise break pipeline
                    if (!errorText.ToLower().Contains("warning"))
                    {
                        //TODO: 
                        //throw new ETLException(errorText);
                        logger.Write("Nonwarning exception from R: " + errorText);
                    }
                    
                }
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
        }


        private static void DownloadAndUnpackFiles(IActivityLogger logger, string workingDirectory, string connectionString, string containerName, string blobName)
        {
            logger.Write("Download and unpack R binaries");


            DownloadInputFiles(workingDirectory, connectionString, containerName, new[] { blobName }, logger);

            Console.WriteLine("Finished downloading...");

            try
            {
                ZipFile.ExtractToDirectory(Path.Combine(workingDirectory, blobName), workingDirectory);

            }
            catch (IOException e)
            {
                //Just swallow it for now - its probably an indication you are running locally and reusing a workspace
                Console.WriteLine(e.Message);
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
            string containerName, string[] blobNames, IActivityLogger logger)
        {
            var inputStorageAccount =
                CloudStorageAccount.Parse(connectionString);
            var inputClient = inputStorageAccount.CreateCloudBlobClient();
            var container = inputClient.GetContainerReference(containerName);
            var inputFiles = new string[blobNames.Length];

            logger.Write("Starting to download");

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

     
        private static string[] DownloadAllInputFiles(IActivityLogger logger, string workingDirectory, string connectionString, string containerName, string[] blobNames)
        {

            //Download input files
            DownloadAndUnpackFiles(logger, workingDirectory, connectionString, "rbinaries", "R-3.3.3.zip");

            logger.Write("Downloading input files used by this sample to the Working Directory");


            var inputFileNames = DownloadInputFiles(workingDirectory, connectionString, containerName, blobNames, logger);

            var index = 0;
            for (; index < inputFileNames.Length; index++)
            {
                var file = inputFileNames[index];
                if (File.Exists(file))
                {
                    logger.Write(String.Format("File : {0} exists", file));
                }
            }
            return inputFileNames;
        }
    }

    
}