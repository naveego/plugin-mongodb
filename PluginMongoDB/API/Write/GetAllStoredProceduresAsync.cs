using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Write
{
    public static partial class Write
    {
        private const string SchemaName = "ROUTINE_SCHEMA";
        private const string RoutineName = "ROUTINE_NAME";
        private const string SpecificName = "SPECIFIC_NAME";

        private static string GetAllStoredProceduresQuery = @"
SELECT ROUTINE_SCHEMA, ROUTINE_NAME, SPECIFIC_NAME
FROM {0}.INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA != 'sys'";

        public static async Task<List<WriteStoredProcedure>> GetAllStoredProceduresAsync(IClientFactory clientFactory)
        {
            throw new NotImplementedException();
        }
    }
}