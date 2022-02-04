using System.Threading.Tasks;
using Naveego.Sdk.Plugins;

using MongoDB.Bson;
using MongoDB.Driver;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        public static async Task<Schema> GetCollectionSchema(IClient client, IMongoCollection<BsonDocument> collection, Schema schema, int sampleSize)
        { 
            var firstRow = collection.Find(new BsonDocument()).FirstOrDefault();
            
            foreach (var col in firstRow)
            {
                var property = new Property
                {
                    Id = col.Name,
                    Name = col.Name,
                    IsKey = col.Name == "_id",
                    IsNullable = col.Name != "_id",
                    Type = GetType(col.GetType().ToString()),
                    TypeAtSource = GetTypeAtSource(col.GetType().ToString(), 0)
                };
                schema?.Properties.Add(property);
            }
                 
            //get sample and count
            return await AddSampleAndCount(client, collection, schema, sampleSize);
        }
    }
}