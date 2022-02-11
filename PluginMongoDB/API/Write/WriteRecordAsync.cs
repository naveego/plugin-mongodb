using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Write
{
    public static partial class Write
    {
        private static readonly SemaphoreSlim WriteSemaphoreSlim = new SemaphoreSlim(1, 1);

        public static async Task<string> WriteRecordAsync(IClientFactory clientFactory, Schema schema, Record record,
            IServerStreamWriter<RecordAck> responseStream)
        {
            throw new NotImplementedException();
        }
    }
}