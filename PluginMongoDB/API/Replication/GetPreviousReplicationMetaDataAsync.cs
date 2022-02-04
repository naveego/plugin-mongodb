using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Constants = PluginBigQuery.API.Utility.Constants;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaDataAsync(
            IClientFactory clientFactory,
            string jobId,
            ReplicationTable table)
        {
            var client = clientFactory.GetClient();

            try
            {
                ReplicationMetaData replicationMetaData = null;

                // ensure replication metadata table
                await EnsureCollectionAsync(clientFactory, table);
                
                // check if metadata exists
                var db = client.GetDatabase(table.SchemaName);
                var collection = db.GetCollection<BsonDocument>(table.TableName);
                var filter = Builders<BsonDocument>.Filter.Eq(Constants.ReplicationMetaDataJobId, jobId);
                var result = collection.Find(filter).FirstOrDefault();

                
                try
                {
                    replicationMetaData = new ReplicationMetaData
                    {
                        Request = JsonConvert.DeserializeObject<PrepareWriteRequest>(
                            result[Constants.ReplicationMetaDataRequest].ToString()),
                        ReplicatedShapeName = result[Constants.ReplicationMetaDataReplicatedShapeName].ToString(),
                        ReplicatedShapeId = result[Constants.ReplicationMetaDataReplicatedShapeId].ToString(),
                        Timestamp = DateTime.Parse(result[Constants.ReplicationMetaDataTimestamp].ToString())
                    };
                }
                catch (Exception e)
                {
                    //Empty result or some field does not exist - return null obj
                    if (e.Message == "Object reference not set to an instance of an object.")
                    {
                        return replicationMetaData;
                    }
                    Logger.Error(e, e.Message);
                    throw;
                }
                
                return replicationMetaData;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                throw;
            }
        }
    }
}