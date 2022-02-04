using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        public static async Task<Count> GetCountOfRecords(IMongoCollection<BsonDocument> collection, Schema schema)
        {
            var count = 0;
            
            var count_64 = await collection.CountDocumentsAsync(new BsonDocument());

            if (count_64 > Int32.MaxValue)
            {
                count = Int32.MaxValue;
            }
            else
            {
                count = Convert.ToInt32(count_64);
            }

            return count == 0
                ? new Count
                {
                    Kind = Count.Types.Kind.Unavailable,
                }
                : new Count
                {
                    Kind = Count.Types.Kind.Estimate,
                    Value = count
                };
        }
    }
}