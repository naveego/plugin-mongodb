using System;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using MongoDB.Bson;
using MongoDB.Driver;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;
using Constants = PluginBigQuery.API.Utility.Constants;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        public static async Task UpsertReplicationMetaDataAsync(IClientFactory clientFactory, ReplicationTable table,
            ReplicationMetaData metaData)
        {

            var client = clientFactory.GetClient();

            try
            {
                await EnsureCollectionAsync(clientFactory, table);
                    
                var db = client.GetDatabase(table.SchemaName);
                var collection = db.GetCollection<BsonDocument>(table.TableName);
                
                if (!await RecordExistsAsync(clientFactory, table, metaData.Request.DataVersions.JobId))
                {
                    //write to table

                    await collection.InsertOneAsync(new BsonDocument
                    {
                        {Constants.ReplicationMetaDataJobId, metaData.Request.DataVersions.JobId},
                        {Constants.ReplicationMetaDataRequest, JsonConvert.SerializeObject(metaData.Request)},
                        {Constants.ReplicationMetaDataReplicatedShapeId, metaData.ReplicatedShapeId},
                        {Constants.ReplicationMetaDataReplicatedShapeName, metaData.ReplicatedShapeName},
                        {Constants.ReplicationMetaDataTimestamp, metaData.Timestamp}
                    });
                    
                }
                else
                {
                    // update if found
                    var filter = Builders<BsonDocument>.Filter.Eq(Constants.ReplicationMetaDataJobId,
                        metaData.Request.DataVersions.JobId);

                    var updateRecord = new BsonDocument
                    {
                        {Constants.ReplicationMetaDataJobId, metaData.Request.DataVersions.JobId},
                        {Constants.ReplicationMetaDataRequest, JsonConvert.SerializeObject(metaData.Request)},
                        {Constants.ReplicationMetaDataReplicatedShapeId, metaData.ReplicatedShapeId},
                        {Constants.ReplicationMetaDataReplicatedShapeName, metaData.ReplicatedShapeName},
                        {Constants.ReplicationMetaDataTimestamp, metaData.Timestamp}
                    };

                    collection.FindOneAndUpdate(filter, updateRecord);
                    
                }
            }
            catch (Exception e)
            {
                try
                {
                    // update if it failed
                    
                    var filter = Builders<BsonDocument>.Filter.Eq(Constants.ReplicationMetaDataJobId,
                        metaData.Request.DataVersions.JobId);

                    var updateRecord = new BsonDocument
                    {
                        {Constants.ReplicationMetaDataJobId, metaData.Request.DataVersions.JobId},
                        {Constants.ReplicationMetaDataRequest, JsonConvert.SerializeObject(metaData.Request)},
                        {Constants.ReplicationMetaDataReplicatedShapeId, metaData.ReplicatedShapeId},
                        {Constants.ReplicationMetaDataReplicatedShapeName, metaData.ReplicatedShapeName},
                        {Constants.ReplicationMetaDataTimestamp, metaData.Timestamp}
                    };
                    var db = client.GetDatabase(table.SchemaName);
                    var collection = db.GetCollection<BsonDocument>(table.TableName);
                    collection.FindOneAndUpdate(filter, updateRecord);
                }
                catch (Exception exception)
                {
                    Logger.Error(e, $"Error Insert: {e.Message}");
                    Logger.Error(exception, $"Error Update: {exception.Message}");
                    throw;
                }
                finally
                {
                    //noop
                }
            }
            finally
            {
                //noop
            }
        }
    }
}