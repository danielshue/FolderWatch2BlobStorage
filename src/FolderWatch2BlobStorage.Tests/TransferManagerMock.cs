// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// This is sample code and not meant to be used in a production environment.

using System;
using System.Threading;

namespace FolderWatch2BlobStorage.Tests
{
    internal class TransferManagerMock : ITranferManager
    {
        public void UploadFile(string filePath)
        {
            Console.WriteLine($"Transfering File: {filePath}");
            Thread.Sleep(TimeSpan.FromSeconds(5));
        }
    }
}
