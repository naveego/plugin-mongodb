using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {

        public static async Task DeleteRecordAsync(IClientFactory clientFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var client = clientFactory.GetClient();
            var db = client.GetDatabase(table.SchemaName);
            var collection = db.GetCollection<BsonDocument>(table.TableName);
            
            var filter = new BsonDocument(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, primaryKeyValue);

            try
            {
                await collection.DeleteOneAsync(filter);
            }
            finally
            {
                //noop
            }
            
            
        }
    }
}