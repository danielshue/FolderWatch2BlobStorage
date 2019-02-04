// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// This is sample code and not meant to be used in a production environment.

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace FolderWatch2BlobStorage
{
    internal class TranferManager : ITranferManager
    {
        private static readonly Queue<string> _transferQueue = new Queue<string>();

        public string AccountName { get; set; }

        public string AccountKey { get; set; }

        public string ContainerName { get; set; }

        public TranferManager(string accountName, string accountKey, string containerName)
        {
            AccountName = accountName;
            AccountKey = accountKey;
            ContainerName = containerName;
        }

        public async Task UploadFileAsync(string filePath)
        {

            if (_transferQueue.Contains(filePath) == false)
            {
                _transferQueue.Enqueue(filePath);
            }

            string file = string.Empty;

            if (_transferQueue.TryDequeue(out file) && !string.IsNullOrWhiteSpace(filePath) && File.Exists(filePath))
            {
                Console.WriteLine($"Transfering file: \t {file}");

                FileSize transferType = await CalculcateFileSize(file);
                if (transferType == FileSize.Small)
                {
                    await UploadSmallFileAsync(file);
                }
                else
                {
                    await UploadLargeFileAsync(file);
                }

            }

            Console.WriteLine($"Current Queue Length is {_transferQueue.Count}");

        }

        /// <summary>
        /// Upload Files less than 1 MB
        /// </summary>
        /// <remarks>
        /// </remarks>
        internal async Task UploadSmallFileAsync(string fileName)
        {
            var blobStorageDirectoryName = fileName.Substring(Path.GetPathRoot(fileName).Length);
            
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

            await blob.UploadFromFileAsync(fileName);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <remarks>
        /// based off https://www.red-gate.com/simple-talk/cloud/platform-as-a-service/azure-blob-storage-part-3-using-the-storage-client-library/
        /// </remarks>
        internal async Task UploadLargeFileAsync(string fileName)
        {
            CloudBlobClient cloudBlobClient = CreateBlobClient(AccountName, AccountKey);

            var cloudBlobContainer = cloudBlobClient.GetContainerReference(ContainerName);

            //just in case, check to see if the container exists, and create it if it doesn't
            await cloudBlobContainer.CreateIfNotExistsAsync();

            var blobStorageDirectoryName = fileName.Substring(Path.GetPathRoot(fileName).Length);

            CloudBlockBlob blob = cloudBlobContainer.GetBlockBlobReference(blobStorageDirectoryName);

            int blockSize = 256 * 1024; //256 kb

            using (FileStream fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
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

                }
                catch (Exception ex)
                {
                    Debug.Print("Exception thrown = {0}", ex);
                }
            }
        }

        internal async Task<FileSize> CalculcateFileSize(string filePath)
        {
            return await Task.Run(() =>
            {
                long length = new FileInfo(filePath).Length;

                if (length <= (1024 * 1024)) // 1 MB
                {
                    return FileSize.Small;
                }

                return FileSize.Large;
            });
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
        
    }
}
