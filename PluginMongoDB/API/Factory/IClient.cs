using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace PluginBigQuery.API.Factory
{
    public interface IClient
    {
        string GetDefaultDatabase();
        
        
        Task<bool> PingAsync();

        public MongoDB.Driver.IAsyncCursor<BsonDocument> ListDatabases();
        public Task<MongoDB.Driver.IAsyncCursor<BsonDocument>> ListDatabasesAsync();
        public Task<IAsyncCursor<string>> ListDatabaseNamesAsync();

        public IMongoDatabase GetDatabase(string db);
        public Int32 GetFlattenDepth();

    }
}