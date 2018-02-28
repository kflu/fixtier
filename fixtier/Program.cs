﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Core.Util;
using Newtonsoft.Json;

namespace fixtier
{
    class Program
    {
        static void Main(string[] args)
        {
            var (success, config) = Configuration.TryParseCommandline(args);
            Console.WriteLine(config);
            if (!success)
            {
                Configuration.PrintUsage();
                return;
            }

            Action<string> log = Console.WriteLine;
            IFixer fixer = config.DryRun ? (IFixer)new DryRunFixer(log) : new TierFixer(log);
            var client = Utils.CreateBlobClient(config.ConnectionString);
            IBlobProvider provider = new AllBlobProvider(client, config.ContainerName, log);

            foreach (var blob in provider.Provide())
            {
                fixer.Fix(blob);
            }
        }
    }

    public class DryRunFixer : IFixer
    {
        private readonly Action<string> log;
        public DryRunFixer(Action<string> log) => this.log = log;

        public void Fix(CloudBlockBlob blob)
        {
            this.log($"[dry run] would fix {blob.Name}");
        }
    }

    public interface IFixer
    {
        void Fix(CloudBlockBlob blob);
    }

    public interface IBlobProvider
    {
        IEnumerable<CloudBlockBlob> Provide();
    }

    public class TierFixer : IFixer
    {
        private readonly Action<string> log;

        public TierFixer(Action<string> log) => this.log = log;

        public void Fix(CloudBlockBlob blob)
        {
            this.log($"Setting {blob.Name} to Archive tier");
            blob.SetStandardBlobTier(StandardBlobTier.Archive);
        }
    }

    public class AllBlobProvider : IBlobProvider
    {
        private readonly CloudBlobClient client;
        private readonly string containerPath;
        private readonly Action<string> log;

        public AllBlobProvider(CloudBlobClient client, string containerPath, Action<string> log)
        {
            this.client = client;
            this.containerPath = containerPath;
            this.log = log;
        }

        public IEnumerable<CloudBlockBlob> Provide()
        {
            var container = this.client.GetContainerReference(this.containerPath);
            return container.ListBlobs().OfType<CloudBlockBlob>();
        }
    }

    public static class Utils
    {
        public static CloudBlobClient CreateBlobClient(string connectionString)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            var client = storageAccount.CreateCloudBlobClient();
            client.DefaultRequestOptions = new BlobRequestOptions
            {
                StoreBlobContentMD5 = true,
                UseTransactionalMD5 = true,
                SingleBlobUploadThresholdInBytes = 1024 * 1024 * 20,
            };

            return client;
        }
    }
}