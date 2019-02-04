// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// This is sample code and not meant to be used in a production environment.

using System;
using System.Diagnostics;
using System.ComponentModel.DataAnnotations;
using System.IO;
using McMaster.Extensions.CommandLineUtils;
using System.Threading;

namespace FolderWatch2BlobStorage
{
    [Command(ThrowOnUnexpectedArgument = false, AllowArgumentSeparator = true, ExtendedHelpText = @"Monitor a folder for file addtions or changes and automatically transfer those files to a Azure BLOB Storage when those events occur.")]
    [HelpOption("-? | --? | -help | --help")]
    public class TransferToStorage
    {
        private static FileSystemWatcher _watcher = new FileSystemWatcher();
        private static ManualResetEvent _exitEvent = new ManualResetEvent(false);
        private static ITranferManager _transferManger;

        public static int Main(string[] args) => CommandLineApplication.Execute<TransferToStorage>(args);

        [Required]
        [Option("-account | -accountname", "Azure Storage Account Name", CommandOptionType.SingleValue)]
        public string AccountName { get; set; }

        [Required]
        [Option("-key | --accountkey", "Azure Storage Account Key Value", CommandOptionType.SingleValue)]
        public string AccountKey { get; set; }

        [Required]
        [Option("-name | --containername", "Azure Storage Container Name", CommandOptionType.SingleValue)]
        public string ContainerName { get; set; }

        [Required]
        [Option("-d | --directory", "Directory to monitor for addtions or changes such as c:\\myfolder", CommandOptionType.SingleValue)]
        public string DirectoryName { get; set; }

        [Option("-i | --include", "Include Subdirectories. Default is false.", CommandOptionType.NoValue)]
        public bool IncludeSubdirectories { get; set; }

        [Option("--filter", "A filter pattern to apply to files to be monitored such as *.txt. Default is *.*", CommandOptionType.SingleValue)]
        public string Filter { get; set; }

        [Option("-v | --verbose", "Enables verbose output", CommandOptionType.NoValue)]
        public bool Verbose { get; set; }

        [Option("-l|--log-level", "Logging Level", CommandOptionType.SingleOrNoValue)]
        public int LogLevel { get; set; }

        private int OnExecute()
        {

            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                eventArgs.Cancel = true;
                _exitEvent.Set();
            };

            var timer = Stopwatch.StartNew();

            if (string.IsNullOrWhiteSpace(Filter))
            {
                Filter = "*.*";
            }


            if (Verbose)
            {
                Console.WriteLine($"Account Name \t{AccountName}");
                Console.WriteLine($"Account Key \t{AccountKey}");
                Console.WriteLine($"Container Name \t{ContainerName}");
                Console.WriteLine($"Directory Name \t{DirectoryName}");
                Console.WriteLine($"Filter \t\t{Filter}");
                Console.WriteLine($"Verbose \t{Verbose}");
                Console.WriteLine($"LogLevel \t{LogLevel}");

            }

            _watcher.Path = DirectoryName;
            _watcher.IncludeSubdirectories = IncludeSubdirectories;

            // Watch for changes in LastAccess and LastWrite times, and
            // the renaming of files or directories.
            _watcher.NotifyFilter = NotifyFilters.LastAccess
                                 | NotifyFilters.LastWrite
                                 | NotifyFilters.FileName
                                 | NotifyFilters.DirectoryName;

            // Only watch text files.
            _watcher.Filter = Filter;

            // Add event handlers.
            _watcher.Changed += OnFileChange;

            // Begin watching.
            _watcher.EnableRaisingEvents = true;

            Console.WriteLine($"Monitoring: {DirectoryName}, Filter: {Filter}, Including Subdirectories: {IncludeSubdirectories}");

            _transferManger = new TranferManager(AccountName, AccountKey, ContainerName);
            //_transferManger = new TransferManagerMock();

            _exitEvent.WaitOne();

            timer.Stop();

            Console.WriteLine($"Elapsed time \t{ timer.Elapsed.TotalSeconds } seconds");

            return 0;
        }


        private async void OnFileChange(object source, FileSystemEventArgs e)
        {
            // Specify what is done when a file is changed, created, or deleted.
            Console.WriteLine($"File: {e.FullPath} {e.ChangeType}");

            await _transferManger.UploadFileAsync(e.FullPath);
        }
    }
}