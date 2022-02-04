using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginBigQuery.API.Discover;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Read
{
    public static partial class Read
    {
        public static async IAsyncEnumerable<Record> ReadRecords(IClientFactory clientFactory, Schema schema)
        {
            var client = clientFactory.GetClient();

            var schemaParts = schema.Id.Split('.');

            var dbName = "";
            var collectionName = "";
            
            foreach (var part in schemaParts)
            {
                if (part == schemaParts[0])
                {
                    dbName = part;
                }
                else
                {
                    collectionName += $"{part}.";
                }
            }

            collectionName = collectionName.TrimEnd('.');
            
            var db = client.GetDatabase(dbName);

            var collection = db.GetCollection<BsonDocument>(collectionName);

            var rows = await collection.Find(new BsonDocument()).ToListAsync();
            
            foreach (var row in rows)
            {
                var recordMap = new Dictionary<string, object>();
                    
                foreach (var property in schema.Properties)
                {
                    try
                    {
                        switch (property.Type)
                        {
                            case PropertyType.String:
                            case PropertyType.Text:
                            case PropertyType.Decimal:
                                recordMap[property.Id] = row[property.Id].ToString();
                                break;
                            default:
                                recordMap[property.Id] = row[property.Id];
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.Error(e, $"No column with property Id: {property.Id}");
                        Logger.Error(e, e.Message);
                        recordMap[property.Id] = null;
                    }
                }
                var record = new Record
                {
                    Action = Record.Types.Action.Upsert,
                    DataJson = JsonConvert.SerializeObject(recordMap)
                };

                yield return record;
            }
        }
        
        public static async IAsyncEnumerable<Record> ReadCollectionRecords(IClient client, IMongoCollection<BsonDocument> collection, Schema schema)
        {
            var flattenDepth = client.GetFlattenDepth();
            
            var rows = await collection.Find(new BsonDocument()).ToListAsync();

            foreach (var row in rows)
            {
                var recordMap = new Dictionary<string, object>();
                    
                var thisBsonDocument = BsonSerializer.Deserialize<BsonDocument>(row);
                        
                foreach (var childElement in thisBsonDocument)
                {
                    FlattenAndReadBsonElement(recordMap, childElement, flattenDepth);
                }
                
                var record = new Record
                {
                    Action = Record.Types.Action.Upsert,
                    DataJson = JsonConvert.SerializeObject(recordMap)
                };
                
                yield return record;
                
                //**Use below code if nesting is not desired**
                //
                // foreach (var property in schema.Properties)
                // {
                //     try
                //     {
                //         switch (property.Type)
                //         {
                //             case PropertyType.String:
                //             case PropertyType.Text:
                //             case PropertyType.Decimal:
                //                 recordMap[property.Id] = row[property.Id].ToString();
                //                 break;
                //             default:
                //                 recordMap[property.Id] = row[property.Id];
                //                 break;
                //         }
                //     }
                //     catch (Exception e)
                //     {
                //         Logger.Error(e, $"No column with property Id: {property.Id}");
                //         Logger.Error(e, e.Message);
                //         recordMap[property.Id] = null;
                //     }
                // }
                // var record = new Record
                // {
                //     Action = Record.Types.Action.Upsert,
                //     DataJson = JsonConvert.SerializeObject(recordMap)
                // };
                //
                // yield return record;
            }
        }
        public static void FlattenAndReadBsonElement(Dictionary<string, object> recordMap, BsonElement bson, int depth, string runningColumnName = "")
        {
            if (depth == 0)
            {
                recordMap[runningColumnName] = bson.Value.ToString();
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
                    FlattenAndReadBsonElement(recordMap, child, depth - 1, $"{runningColumnName}.{child.Name}");
                }
            }
            catch
            {
                if (string.IsNullOrWhiteSpace(runningColumnName))
                {
                    runningColumnName = bson.Name;
                }
                recordMap[runningColumnName] = bson.Value.ToString();
            }
        }
    }
}