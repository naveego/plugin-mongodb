using System;
using PluginBigQuery.Helper;
using Xunit;

namespace PluginBigQueryTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateValidTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = ""
            };

            // act
            settings.Validate();

            // assert
        }

        [Fact]
        public void ValidateNoDefaultDatabaseTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = null
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Default Database property must be set", e.Message);
        }
        [Fact]
        public void ValidateNoConnectionStringTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = null
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Connection String property must be set", e.Message);
        }
    }
}