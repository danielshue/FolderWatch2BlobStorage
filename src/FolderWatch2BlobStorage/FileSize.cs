// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// This is sample code and not meant to be used in a production environment.

namespace FolderWatch2BlobStorage
{
    internal enum FileSize
    {
        /// <summary>
        /// Files that are less than 1 MB
        /// </summary>
        Small = 0,

        /// <summary>
        /// Files that are larger than 1 MB
        /// </summary>
        Large = 1,
    }
}
