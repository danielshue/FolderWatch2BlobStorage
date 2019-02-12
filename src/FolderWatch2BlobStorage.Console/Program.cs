// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// This is sample code and not meant to be used in a production environment.

using System;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.IO;
using System.Threading;
using McMaster.Extensions.CommandLineUtils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace FolderWatch2BlobStorage
{
    [Command(ThrowOnUnexpectedArgument = false,
         AllowArgumentSeparator = true,
         OptionsComparison = StringComparison.OrdinalIgnoreCase,
         ResponseFileHandling = ResponseFileHandling.ParseArgsAsSpaceSeparated,
         ExtendedHelpText = @"Monitor a folder for file addtions or changes and automatically transfer those files to a Azure BLOB Storage when those events occur.")]
    [HelpOption("-? | --? | -help | --help")]
    public class TransferToStorage
    {
        private static FileSystemWatcher _watcher = new FileSystemWatcher();
        private static ManualResetEvent _exitEvent = new ManualResetEvent(false);
        private static ITranferManager _transferManger;
        private ILogger<TransferToStorage> _logger;

        [Required]
        [Option("--accountname", "Azure Storage Account Name", CommandOptionType.SingleValue)]
        public string AccountName { get; set; }

        [Required]
        [Option("--accountkey", "Azure Storage Account Key Value", CommandOptionType.SingleValue)]
        public string AccountKey { get; set; }

        [Required]
        [Option("--containername", "Azure Storage Container Name", CommandOptionType.SingleValue)]
        public string ContainerName { get; set; }

        [Required]
        [Option("--directory", "Directory to monitor for addtions or changes such as c:\\myfolder", CommandOptionType.SingleValue)]
        public string DirectoryName { get; set; }

        [Option("--include", "Include Subdirectories. Default is false.", CommandOptionType.NoValue)]
        public bool IncludeSubdirectories { get; set; }

        [Option("--filter", "A filter pattern to apply to files to be monitored such as *.txt. Default is *.*", CommandOptionType.SingleValue)]
        public string Filter { get; set; }

        [Option("--verbose", "Enables verbose output", CommandOptionType.NoValue)]
        public bool Verbose { get; set; }

        [Option("--loglevel", "Logging Level", CommandOptionType.SingleOrNoValue)]
        public int LogLevel { get; set; }

        public static int Main(string[] args) => CommandLineApplication.Execute<TransferToStorage>(args);

        private int OnExecute()
        {
            var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<ITranferManager>(sp => new TranferManager(_logger, AccountName, AccountKey, ContainerName))
            .AddSingleton<IConsole>(PhysicalConsole.Singleton)
            .BuildServiceProvider();

            //configure console log
            services
                .GetService<ILoggerFactory>()
                .AddConsole(Microsoft.Extensions.Logging.LogLevel.Debug);

            _logger = services.GetService<ILoggerFactory>().CreateLogger<TransferToStorage>();

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

            _logger.LogTrace($"Account Name \t{AccountName}");
            _logger.LogTrace($"Account Key \t{AccountKey}");
            _logger.LogTrace($"Container Name \t{ContainerName}");
            _logger.LogTrace($"Directory Name \t{DirectoryName}");
            _logger.LogTrace($"Filter \t\t{Filter}");
            _logger.LogTrace($"Verbose \t{Verbose}");
            _logger.LogTrace($"LogLevel \t{LogLevel}");

            _watcher.Path = DirectoryName;
            _watcher.IncludeSubdirectories = IncludeSubdirectories;

            // Watch for changes in LastAccess and LastWrite times, and
            // the renaming of files or directories.
            _watcher.NotifyFilter = NotifyFilters.LastAccess;

            // Only watch text files.
            _watcher.Filter = Filter;

            // Add event handlers.
            _watcher.Changed += OnFileChange;

            // Begin watching.
            _watcher.EnableRaisingEvents = true;

            _logger.LogInformation($"Monitoring: {DirectoryName}, Filter: {Filter}, Including Subdirectories: {IncludeSubdirectories}");

            _transferManger = services.GetService<ITranferManager>();

            _exitEvent.WaitOne();

            timer.Stop();

            _logger.LogInformation($"Elapsed time \t{ timer.Elapsed.TotalSeconds } seconds");

            if(_transferManger is IDisposable)
            {
                ((IDisposable)_transferManger).Dispose();
            }

            return 0;
        }
       
        private void OnFileChange(object source, FileSystemEventArgs e)
        {
            // Specify what is done when a file is changed, created, or deleted.
            _logger.LogInformation($"Detect File {e.ChangeType}: {e.FullPath}");

            _transferManger.UploadFile(e.FullPath);
        }
    }
}