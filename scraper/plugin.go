package scraper

import (
	"context"
	"encoding/json"

	v1 "github.com/flanksource/config-db/api"
	pluginapi "github.com/flanksource/config-db/api/plugin"
	pb "github.com/flanksource/config-db/api/plugin/proto"
)

const (
	PluginName    = "azure"
	PluginVersion = "1.0.0"
)

type AzurePlugin struct {
	hostClient *pluginapi.GRPCHostClient
}

func NewAzurePlugin() *AzurePlugin {
	return &AzurePlugin{}
}

func (p *AzurePlugin) SetHostClient(client *pluginapi.GRPCHostClient) {
	p.hostClient = client
}

func (p *AzurePlugin) GetInfo(ctx context.Context) (*pluginapi.PluginInfo, error) {
	return &pluginapi.PluginInfo{
		Name:           PluginName,
		Version:        PluginVersion,
		SupportedTypes: []string{"Azure"},
	}, nil
}

func (p *AzurePlugin) CanScrape(ctx context.Context, specJSON []byte) (bool, error) {
	var spec v1.ScraperSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return false, err
	}
	return len(spec.Azure) > 0, nil
}

func (p *AzurePlugin) Scrape(ctx context.Context, req *pb.ScrapeRequest) (*pb.ScrapeResponse, error) {
	var spec v1.ScraperSpec
	if err := json.Unmarshal(req.SpecJson, &spec); err != nil {
		return &pb.ScrapeResponse{Error: err.Error()}, nil
	}

	scraper := &Scraper{
		hostClient: p.hostClient,
		scraperID:  req.ScraperId,
		namespace:  req.Namespace,
	}

	results, err := scraper.Scrape(ctx, spec.Azure)
	if err != nil {
		return &pb.ScrapeResponse{Error: err.Error()}, nil
	}

	var protoResults []*pb.ScrapeResultProto
	for _, r := range results {
		pr, err := pluginapi.ScrapeResultToProto(r)
		if err != nil {
			return &pb.ScrapeResponse{Error: err.Error()}, nil
		}
		protoResults = append(protoResults, pr)
	}

	return &pb.ScrapeResponse{Results: protoResults}, nil
}
