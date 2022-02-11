using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using PluginBigQuery.Helper;

namespace PluginBigQuery.API.Factory
{
    public class Client : IClient
    {
        private readonly MongoClient _client;

        public string _defaultDatabase;

        public Int32 _nestedObjectDepth;
        
        public Client(Settings settings)
        {
            //Initialize client
            _client = new MongoClient(
                settings.ConnectionString
            );
            
            _defaultDatabase = settings.DefaultDatabase;
            _nestedObjectDepth = Int32.Parse(settings.NestedObjectDepth);
        }

        public async Task<IAsyncCursor<string>> ListDatabaseNamesAsync()
        {
            return await _client.ListDatabaseNamesAsync();
        }
        public async Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync()
        {
            return await _client.ListDatabasesAsync();
        }
        public IMongoDatabase GetDatabase(string db)
        {
            return _client.GetDatabase(db);
        }

        public IAsyncCursor<BsonDocument> ListDatabases()
        {
            return _client.ListDatabases();
        }
        
        
        public async Task<bool> PingAsync()
        {
            throw new NotImplementedException();
            // await _client.ExecuteQueryAsync("SELECT 1;", parameters: null);
            // return true;
        }
        
        public string GetProjectId()
        {
            throw new NotImplementedException();
        }

        public Int32 GetFlattenDepth()
        {
            return _nestedObjectDepth;
        }
        
        public string GetDefaultDatabase()
        {
            return _defaultDatabase;
        }
    }
}