package main

import (
	"github.com/flanksource/config-db-azure/scraper"
	pluginapi "github.com/flanksource/config-db/api/plugin"
	"github.com/hashicorp/go-plugin"
)

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: pluginapi.Handshake,
		Plugins: map[string]plugin.Plugin{
			pluginapi.PluginName: &pluginapi.GRPCPlugin{
				Impl: scraper.NewAzurePlugin(),
			},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
