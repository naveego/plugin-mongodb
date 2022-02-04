using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        public static async Task UpsertRecordAsync(IClientFactory clientFactory,
            ReplicationTable table,
            Dictionary<string, object> recordMap)
        {
            var client = clientFactory.GetClient();
            var db = client.GetDatabase(table.SchemaName);
            var collection = db.GetCollection<BsonDocument>(table.TableName);
            // delete from table
            // where
            
            try
            {
                var primaryKey = table.Columns.Find(c => c.PrimaryKey);
                var primaryValue = recordMap[primaryKey.ColumnName];
                if (primaryKey.Serialize)
                {
                    primaryValue = JsonConvert.SerializeObject(primaryValue);
                }

                if (!await RecordExistsAsync(clientFactory, table, primaryValue.ToString()))
                {
                    var bsonDocument = new BsonDocument() { };
                    foreach (var record in recordMap)
                    {
                        bsonDocument.Add(new BsonElement(record.Key, record.Value.ToString()));
                    }

                    await collection.InsertOneAsync(bsonDocument);
                }
                else
                {
                    var filter = Builders<BsonDocument>.Filter.Eq(primaryKey.ColumnName, primaryValue);

                    foreach (var record in recordMap)
                    {
                        var update = Builders<BsonDocument>.Update.Set(record.Key, record.Value.ToString());
                        await collection.UpdateManyAsync(filter, update);
                    }
                    
                    // var querySb =
                    //     new StringBuilder(
                    //         $"UPDATE {Utility.Utility.GetSafeName(table.SchemaName, '`', true)}.{Utility.Utility.GetSafeName(table.TableName, '`', true)} SET ");
                    // foreach (var column in table.Columns)
                    // {
                    //     if (!column.PrimaryKey)
                    //     {
                    //         if (recordMap.ContainsKey(column.ColumnName))
                    //         {
                    //             var rawValue = recordMap[column.ColumnName];
                    //             if (column.Serialize)
                    //             {
                    //                 rawValue = JsonConvert.SerializeObject(rawValue);
                    //             }
                    //             
                    //             querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)}=");
                    //             
                    //             switch (column.DataType.ToLower())
                    //             {
                    //                         
                    //                 case "string":
                    //                 case "datetime":
                    //                 case "date":
                    //                 case "time":
                    //                 case "timestamp":
                    //                     querySb.Append(rawValue != null
                    //                         ? $"'{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")}',"
                    //                         : $"NULL,");
                    //                     break;
                    //                 default:
                    //                     querySb.Append(rawValue != null
                    //                         ? $"{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")},"
                    //                         : $"NULL,");
                    //                     break;
                    //             }
                    //         }
                    //         else
                    //         {
                    //             querySb.Append($"NULL,");
                    //         }
                    //     }
                    // }
                    //
                    // querySb.Length--;
                    //
                    // querySb.Append($" WHERE {primaryKey.ColumnName} = '{primaryValue}'");
                    //
                    // var query = querySb.ToString();
                    //
                    // await client.ExecuteReaderAsync(query);
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"Error Upsert Record: {e.Message}");
                throw;
            }
        }
    }
}