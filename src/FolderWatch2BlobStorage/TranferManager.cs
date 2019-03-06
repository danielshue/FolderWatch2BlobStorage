// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// This is sample code and not meant to be used in a production environment.

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.Collections.Concurrent;
using System.Threading;
using System.Collections.ObjectModel;

namespace FolderWatch2BlobStorage
{
    public class TranferManager : ITranferManager, IDisposable
    {
        private static readonly BlockingCollection<string> _transferQueue = new BlockingCollection<string>();

        private string AccountName { get; set; }

        private string AccountKey { get; set; }

        private string ContainerName { get; set; }

        private readonly ILogger _logger;

        private readonly Thread _outputThread;

        public IEnumerable<FileDetails> TransferHistory { get { return _transferHistory; } }

        private ObservableCollection<FileDetails> _transferHistory;

        public TranferManager(ILogger logger, string accountName, string accountKey, string containerName)
        {
            _transferHistory = new ObservableCollection<FileDetails>();
            _logger = logger;
            AccountName = accountName;
            AccountKey = accountKey;
            ContainerName = containerName;

            // Start Transfer queue processor
            _outputThread = new Thread(ProcessTransferQueueAsync)
            {
                IsBackground = true,
                Name = "File Transfer queue processing thread"
            };
            _outputThread.Start();
        }

        public virtual void UploadFile(string filePath)
        {
            if (!_transferQueue.IsAddingCompleted)
            {
                try
                {
                    _transferQueue.Add(filePath);

                    _logger.LogInformation($"Current Queue Length is {_transferQueue.Count}");

                    return;
                }
                catch (InvalidOperationException) { }
            }
        }

        public void Dispose()
        {
            _transferQueue.CompleteAdding();

            try
            {
                _outputThread.Join(1500);
            }
            catch (ThreadStateException) { }
        }

        /// <summary>
        /// Designed to transfer a file that is less than 1 MB to BLOB storage.
        /// </summary>
        internal virtual async Task UploadSmallFileAsync(string filePath)
        {
            var fileDetails = CreateFileDetails(filePath);

            var blobStorageDirectoryName = fileDetails.DestinationPath;

            CloudBlobClient cloudBlobClient = CreateBlobClient(AccountName, AccountKey);

            var cloudBlobContainerWithPolicy = cloudBlobClient.GetContainerReference(ContainerName);

            TimeSpan backOffPeriod = TimeSpan.FromSeconds(2);
            int retryCount = 1;
            BlobRequestOptions bro = new BlobRequestOptions()
            {
                SingleBlobUploadThresholdInBytes = 1024 * 1024, //1MB, the minimum
                ParallelOperationThreadCount = 1,
                RetryPolicy = new ExponentialRetry(backOffPeriod, retryCount),
            };

            cloudBlobClient.DefaultRequestOptions = bro;

            //just in case, check to see if the container exists, and create it if it doesn't
            await cloudBlobContainerWithPolicy.CreateIfNotExistsAsync();

            CloudBlockBlob blob = cloudBlobContainerWithPolicy.GetBlockBlobReference(blobStorageDirectoryName);

            blob.StreamWriteSizeInBytes = 256 * 1024; //256 k

            fileDetails.StartTransferTime = DateTime.Now;

            await blob.UploadFromFileAsync(filePath);

            EndFileTransfer(fileDetails);

        }

        /// <summary>
        /// Designed to transfer a file that is greater than 1 MB to BLOB storage.
        /// </summary>
        /// <remarks>
        /// based off https://www.red-gate.com/simple-talk/cloud/platform-as-a-service/azure-blob-storage-part-3-using-the-storage-client-library/
        /// </remarks>
        internal virtual async Task UploadLargeFileAsync(string filePath)
        {
            var fileDetails = CreateFileDetails(filePath);

            CloudBlobClient cloudBlobClient = CreateBlobClient(AccountName, AccountKey);

            var cloudBlobContainer = cloudBlobClient.GetContainerReference(ContainerName);

            //just in case, check to see if the container exists, and create it if it doesn't
            await cloudBlobContainer.CreateIfNotExistsAsync();

            var blobStorageDirectoryName = filePath.Substring(Path.GetPathRoot(filePath).Length);

            CloudBlockBlob blob = cloudBlobContainer.GetBlockBlobReference(blobStorageDirectoryName);

            int blockSize = 256 * 1024; //256 kb

            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                long fileSize = fileStream.Length;

                //block count is the number of blocks + 1 for the last one
                int blockCount = (int)((float)fileSize / (float)blockSize) + 1;

                //List of block ids; the blocks will be committed in the order of this list 
                List<string> blockIDs = new List<string>();

                //starting block number - 1
                int blockNumber = 0;

                try
                {
                    int bytesRead = 0; //number of bytes read so far
                    long bytesLeft = fileSize; //number of bytes left to read and upload

                    fileDetails.StartTransferTime = DateTime.Now;

                    //do until all of the bytes are uploaded
                    while (bytesLeft > 0)
                    {
                        blockNumber++;
                        int bytesToRead;
                        if (bytesLeft >= blockSize)
                        {
                            //more than one block left, so put up another whole block
                            bytesToRead = blockSize;
                        }
                        else
                        {
                            //less than one block left, read the rest of it
                            bytesToRead = (int)bytesLeft;
                        }

                        //create a blockID from the block number, add it to the block ID list
                        //the block ID is a base64 string
                        string blockId = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes(string.Format("BlockId{0}", blockNumber.ToString("0000000"))));

                        blockIDs.Add(blockId);
                        //set up new buffer with the right size, and read that many bytes into it 
                        byte[] bytes = new byte[bytesToRead];
                        fileStream.Read(bytes, 0, bytesToRead);

                        //calculate the MD5 hash of the byte array
                        string blockHash = GetMD5HashFromStream(bytes);

                        //upload the block, provide the hash so Azure can verify it
                        await blob.PutBlockAsync(blockId, new MemoryStream(bytes), blockHash);

                        //increment/decrement counters
                        bytesRead += bytesToRead;
                        bytesLeft -= bytesToRead;
                    }

                    //commit the blocks
                    await blob.PutBlockListAsync(blockIDs);

                    EndFileTransfer(fileDetails);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Exception thrown = {0}");
                }
            }
        }

        internal async Task<FileSize> CalculcateFileSize(string filePath)
        {
            return await Task.Run(() =>
            {
                var file = new FileInfo(filePath);

                long length = file.Length;

                if (length <= (1024 * 1024)) // 1 MB
                {
                    return FileSize.Small;
                }

                return FileSize.Large;
            });
        }

        internal virtual async Task TransferFile(string filePath)
        {
            if (!string.IsNullOrWhiteSpace(filePath) && File.Exists(filePath))
            {
                _logger.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Transfering file: \t {filePath}");

                FileSize transferType = await CalculcateFileSize(filePath);

                if (transferType == FileSize.Small)
                {
                    await UploadSmallFileAsync(filePath);
                }
                else if (transferType == FileSize.Large)
                {
                    await UploadLargeFileAsync(filePath);
                }
            }
        }

        private void EndFileTransfer(FileDetails fileDetails)
        {
            fileDetails.EndTransferTime = DateTime.Now;

            _transferHistory.Add(fileDetails);

            _logger.LogInformation($"End File Transfer: Start Time: {fileDetails.StartTransferTime} | End Time: {fileDetails.EndTransferTime} {fileDetails.DestinationPath}");
        }

        private async void ProcessTransferQueueAsync()
        {
            try
            {
                foreach (var file in _transferQueue.GetConsumingEnumerable())
                {

                    await TransferFile(file);
                }
            }
            catch
            {
                try
                {
                    _transferQueue.CompleteAdding();
                }
                catch { }
            }
        }

        private CloudBlobClient CreateBlobClient(string accountName, string accountKey)
        {
            string connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey}";
            CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            return cloudBlobClient;
        }

        private string GetMD5HashFromStream(byte[] data)
        {
            MD5 md5 = new MD5CryptoServiceProvider();
            byte[] blockHash = md5.ComputeHash(data);
            return Convert.ToBase64String(blockHash, 0, 16);
        }

        private FileDetails CreateFileDetails(string filePath)
        {
            var fileInfo = new FileInfo(filePath);

            var fileDetails = new FileDetails
            {
                FileName = filePath,
                LocalFilePath = fileInfo.DirectoryName,
                Size = fileInfo.Length,
                DestinationPath = filePath.Substring(Path.GetPathRoot(filePath).Length),
            };

            return fileDetails;
        }

    }
}
