using Naveego.Sdk.Plugins;

namespace PluginBigQuery.Helper
{
    public class ServerStatus
    {
        public ConfigureRequest Config { get; set; }
        public Settings Settings { get; set; }
        public bool Connected { get; set; }
        public WriteSettings WriteSettings { get; set; }
        public bool WriteConfigured { get; set; }
    }
}