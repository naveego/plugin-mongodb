using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Naveego.Sdk.Logging;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {

        public static async Task<Dictionary<string, object>> GetRecordAsync(IClientFactory clientFactory,
            ReplicationTable table,
            string primaryKeyValue)
        {
            var client = clientFactory.GetClient();

            var db = client.GetDatabase(table.SchemaName);
            var collection = db.GetCollection<BsonDocument>(table.TableName);
            var filter = new BsonDocument(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, primaryKeyValue);
            var result = collection.Find(filter).FirstOrDefault();

            Dictionary<string, object> recordMap = new Dictionary<string, object>();

            foreach (var element in result.Elements)
            {
                try
                {
                    recordMap[element.Name] = element.Value.ToString();
                }
                catch(Exception e)
                {
                    Logger.Error(e, $"No column with column name: {element.Name}");
                    Logger.Error(e, e.Message);
                    recordMap[element.Name] = "";
                }
            }
            return recordMap;
        }
    }
}