using System;
using System.Collections.Generic;
using System.Data;
using Google.Apis.Bigquery.v2.Data;
using Google.Protobuf.Collections;
using MongoDB.Bson;
using MongoDB.Driver;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        public static async IAsyncEnumerable<Schema> GetRefreshSchemas(IClientFactory clientFactory,
            RepeatedField<Schema> refreshSchemas, int sampleSize = 5)
        {
            var client = clientFactory.GetClient();

            foreach(var schema in refreshSchemas)
            {
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

                yield return await GetCollectionSchema(client, collection, schema, sampleSize);
            }
        }
    }
}