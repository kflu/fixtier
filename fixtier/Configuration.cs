using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace fixtier
{
    public class Configuration
    {
        public string ConnectionString;
        public string ContainerName;
        public bool DryRun;
        public int MaxBlobs = 5000;
        public bool Debug = false;
        public string BlobPath = null;

        private static Dictionary<string, Action<string, Configuration>> map = new Dictionary<string, Action<string, Configuration>>(StringComparer.OrdinalIgnoreCase)
        {
            { "container", (val, config) => config.ContainerName = val },
            { "connection-string", (val, config) => config.ConnectionString = val },
            { "dry-run", (val, config) => config.DryRun = bool.Parse(val) },
            { "max-blobs", (val, config) => config.MaxBlobs = int.Parse(val) },
            { "debug", (val, config) => config.Debug = bool.Parse(val) },
            { "blob-path", (val, config) => config.BlobPath = val },
        };

        public override string ToString() => JsonConvert.SerializeObject(this);

        public static (bool, Configuration) TryParseCommandline(string[] args)
        {
            if (args.Contains("--help")) return (false, null);

            var configuration = new Configuration();
            foreach (var kv in map)
            {
                var (pattern, action) = (kv.Key, kv.Value);
                var prefix = $"--{pattern}=";
                var arg = args.FirstOrDefault(a => a.StartsWith(prefix));
                if (arg != null)
                {
                    action(arg.Substring(prefix.Length), configuration);
                }
            }

            return (true, configuration);
        }

        public static void PrintUsage()
        {
            foreach (var kv in map) Console.WriteLine($"--{kv.Key}");
        }
    }
}
