using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;
using MongoDB;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        public static async IAsyncEnumerable<Schema> GetAllSchemas(IClientFactory clientFactory, int sampleSize = 5)
        {
            var client = clientFactory.GetClient();

            var databaseNames = await client.ListDatabaseNamesAsync();
            await databaseNames.MoveNextAsync();

            foreach (var databaseName in databaseNames.Current)
            {
                var db = client.GetDatabase(databaseName);

                var collectionNames = await db.ListCollectionNamesAsync();
                await collectionNames.MoveNextAsync();

                foreach (var collectionName in collectionNames.Current)
                {
                    if (collectionName.StartsWith("system."))
                    {
                        continue;
                    }
                    var currentSchemaId = $"{databaseName}.{collectionName}";
                    var schema = new Schema
                    {
                        Id = currentSchemaId,
                        Name = currentSchemaId,
                        Properties = { },
                        DataFlowDirection = Schema.Types.DataFlowDirection.Read
                    };
                    
                    
                    var collection = db.GetCollection<BsonDocument>(collectionName);

                    var firstRow = collection.Find(new BsonDocument()).FirstOrDefault();
                    
                    var flattenDepth = client.GetFlattenDepth();
                    
                    if(firstRow != null)
                    {
                        var thisBsonDocument = BsonSerializer.Deserialize<BsonDocument>(firstRow);
                        
                        foreach (var childElement in thisBsonDocument)
                        {
                            FlattenAndAddBsonElement(schema, childElement, flattenDepth);
                        }
                    }
                    
                     if (schema != null)
                    {
                        //get sample and count
                        yield return await AddSampleAndCount(client, collection, schema, sampleSize);
                    }
                }
            }
        }

        
        private static async Task<Schema> AddSampleAndCount(IClient client, IMongoCollection<BsonDocument> collection, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadCollectionRecords(client, collection, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(collection, schema);

            return schema;
        }

        public static void FlattenAndAddBsonElement(Schema schema, BsonElement bson, int depth, string runningColumnName = "")
        {
            if (depth == 0)
            {
                
                schema.Properties.Add( new Property
                {
                    Id = $"{runningColumnName}",
                    Name = $"{runningColumnName}",
                    IsKey = bson.Name == "_id",
                    IsNullable = bson.Name != "_id",
                    Type = GetType(bson.GetType().ToString()),
                    TypeAtSource = GetTypeAtSource(bson.GetType().ToString(), 0)
                });
                return;
            }
            try
            {
                var bsonObj = BsonSerializer.Deserialize<BsonDocument>(bson.Value.ToString());
                
                if (string.IsNullOrWhiteSpace(runningColumnName))
                {
                    runningColumnName = bson.Name;   
                }
                
                foreach (var child in bsonObj)
                {
                    FlattenAndAddBsonElement(schema, child, depth - 1, $"{runningColumnName}.{child.Name}");
                }
            }
            catch
            {
                if (string.IsNullOrWhiteSpace(runningColumnName))
                {
                    runningColumnName = bson.Name;   
                }
                
                schema.Properties.Add(new Property
                {
                    Id = $"{runningColumnName}",
                    Name = $"{runningColumnName}",
                    IsKey = bson.Name == "_id",
                    IsNullable = bson.Name != "_id",
                    Type = GetType(bson.GetType().ToString()),
                    TypeAtSource = GetTypeAtSource(bson.GetType().ToString(), 0)
                });
            }
        }

        public static PropertyType GetType(string dataType)
        {
            return PropertyType.String;
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}({maxLength})" : dataType;
        }
    }
}