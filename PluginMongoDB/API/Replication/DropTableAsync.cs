using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        public static async Task DropCollectionAsync(IClientFactory clientFactory, ReplicationTable table)
        {
            var client = clientFactory.GetClient();

            var db = client.GetDatabase(table.SchemaName);
            var filter = new BsonDocument("name", table.TableName);
            var collections = await db.ListCollectionNamesAsync(new ListCollectionNamesOptions {Filter = filter});

            var collectionExists = await collections.AnyAsync();
            
            if(collectionExists)
            {
                await db.DropCollectionAsync(table.TableName);
            }
        }
    }
}