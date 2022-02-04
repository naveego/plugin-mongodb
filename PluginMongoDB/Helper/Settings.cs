using System;
using System.IO;
using PluginBigQuery.API.Discover;

namespace PluginBigQuery.Helper
{
    public class Settings
    {
        public string ConnectionString { get; set; }
        public string DefaultDatabase { get; set; }
        public string NestedObjectDepth { get; set; }
        
        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(ConnectionString))
            {
                throw new Exception("The ConnectionString property must be set");
            }
            
            if (String.IsNullOrEmpty(DefaultDatabase))
            {
                throw new Exception("The Default Database property must be set");
            }
            
            if (String.IsNullOrEmpty(NestedObjectDepth))
            {
                throw new Exception("The Nested Object Depth property must be set. Use 0 if unsure");
            }
            else
            {
                int temp;
                if (!Int32.TryParse(NestedObjectDepth, out temp))
                {
                    throw new Exception("The Nested Object Depth property could not be parsed to an integer");
                }
            }
        }
    }
}