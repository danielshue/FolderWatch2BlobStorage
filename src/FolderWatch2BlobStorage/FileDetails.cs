using System;

namespace FolderWatch2BlobStorage
{
    public class FileDetails
    {
        public string FileName { get; set; }

        public string LocalFilePath { get; set; }

        public string DestinationPath { get; set; }

        public long Size { get; set; }

        public DateTime StartTransferTime { get; set; }

        public DateTime EndTransferTime { get; set; }

    }
}
