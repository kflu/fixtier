using System;
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
    enum ExitCode
    {
        OK = 0,
        ErrorParseArgs = 1,
        Other = 2,
    }

    class Program
    {
        static int Main(string[] args)
        {
            try
            {
                var (success, config) = Configuration.TryParseCommandline(args);
                Console.WriteLine(config);
                if (!success)
                {
                    Configuration.PrintUsage();
                    return (int)ExitCode.ErrorParseArgs;
                }

                if (config.Debug)
                {
                    Console.WriteLine("Press any key to continue...");
                    Console.Read();
                }

                Action<string> log = Console.WriteLine;
                IFixer fixer = config.DryRun ? (IFixer)new DryRunFixer(log) : new TierFixer(log);
                var client = Utils.CreateBlobClient(config.ConnectionString);
                IBlobProvider provider;
                if (config.BlobPath != null)
                {
                    provider = new SpecifiedBlobProvider(client, config.ContainerName, log, config.BlobPath);
                }
                else
                {
                    //provider = new AllBlobProvider(client, config.ContainerName, log);
                    provider = new WarmBlobProvider(client, config.ContainerName, log);
                }

                int numBlobs = 0;
                List<CloudBlockBlob> blobs = new List<CloudBlockBlob>(config.MaxBlobs);

                foreach (var blob in provider.Provide())
                {
                    numBlobs++;
                    if (numBlobs > config.MaxBlobs)
                    {
                        throw new InvalidOperationException($"Number of provided blobs exceeds the limit {config.MaxBlobs}");
                    }
                    else
                    {
                        blobs.Add(blob);
                    }
                }

                foreach (var blob in blobs)
                {
                    fixer.Fix(blob);
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"An error happened: {e.ToString()}");
                return (int)ExitCode.Other;
            }

            return (int)ExitCode.OK;
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

    public class SpecifiedBlobProvider : IBlobProvider
    {
        private readonly CloudBlobClient client;
        private readonly string containerPath;
        private readonly Action<string> log;
        private readonly string path;

        public SpecifiedBlobProvider(CloudBlobClient client, string containerPath, Action<string> log, string path)
        {
            this.client = client;
            this.containerPath = containerPath;
            this.log = log;
            this.path = path;
        }

        public IEnumerable<CloudBlockBlob> Provide()
        {
            var blob = new CloudBlockBlob(new Uri(this.client.StorageUri.PrimaryUri, $"{this.containerPath}/{this.path}"), this.client.Credentials);
            this.log($"Blob specified: {blob.Uri}");
            return new[] { blob };
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
            return container.ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.Metadata | BlobListingDetails.Snapshots).OfType<CloudBlockBlob>();
        }
    }

    public class WarmBlobProvider : IBlobProvider
    {
        private readonly CloudBlobClient client;
        private readonly string containerPath;
        private readonly Action<string> log;

        public WarmBlobProvider(CloudBlobClient client, string containerPath, Action<string> log)
        {
            this.client = client;
            this.containerPath = containerPath;
            this.log = log;
        }

        public IEnumerable<CloudBlockBlob> Provide()
        {
            var container = this.client.GetContainerReference(this.containerPath);
            return container.ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.Metadata | BlobListingDetails.Snapshots)
                            .OfType<CloudBlockBlob>()
                            .Where(b => b.Properties.StandardBlobTier.HasValue && b.Properties.StandardBlobTier.Value != StandardBlobTier.Archive);
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
