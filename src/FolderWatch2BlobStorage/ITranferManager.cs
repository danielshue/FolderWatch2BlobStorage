// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// This is sample code and not meant to be used in a production environment.

using System.IO;
using System.Threading.Tasks;

namespace FolderWatch2BlobStorage
{
    public interface ITranferManager
    {
        void UploadFile(string filePath);
    }
}