using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string RecordExistsQuery = @"SELECT COUNT(*) as c
FROM (
SELECT * FROM {0}.{1}
WHERE {2} = '{3}'    
) as q";

        public static async Task<bool> RecordExistsAsync(IClientFactory clientFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var client = clientFactory.GetClient();
            var db = client.GetDatabase(table.SchemaName);
            
            var filterCollection = new BsonDocument("name", table.TableName);
            var collections = await db.ListCollectionNamesAsync(new ListCollectionNamesOptions {Filter = filterCollection});

            var collectionExists = await collections.AnyAsync();

            if (collectionExists == false)
            {
                return false;
            }
            
            var collection = db.GetCollection<BsonDocument>(table.TableName);
            var filterRow = Builders<BsonDocument>.Filter.Eq(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, primaryKeyValue);
            var result = collection.Find(filterRow).FirstOrDefault();

            if (result == null)
            {
                return false;
            }
            
            return true;
        }
    }
}
