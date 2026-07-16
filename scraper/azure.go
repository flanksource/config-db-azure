package scraper

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/dns/armdns"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/subscription/armsubscription"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/trafficmanager/armtrafficmanager"
	"github.com/flanksource/commons/collections"
	"github.com/flanksource/commons/logger"
	"github.com/flanksource/commons/utils"
	"github.com/flanksource/duty/models"
	"github.com/flanksource/duty/types"
	msgraphsdkgo "github.com/microsoftgraph/msgraph-sdk-go"
	"github.com/samber/lo"

	v1 "github.com/flanksource/config-db/api"
	pluginapi "github.com/flanksource/config-db/api/plugin"
)

const (
	ConfigTypePrefix         = "Azure::"
	defaultActivityLogMaxage = time.Hour * 24 * 7
	ResourceTypeSubscription = "Subscription"
)

const (
	IncludeActivityLogs        = "activityLogs"
	IncludeAdvisor             = "advisor"
	IncludeAppServices         = "appServices"
	IncludeContainerRegistries = "containerRegistries"
	IncludeDatabases           = "databases"
	IncludeDNS                 = "dns"
	IncludeFirewalls           = "firewalls"
	IncludeK8s                 = "k8s"
	IncludeLoadBalancers       = "loadBalancers"
	IncludePrivateDNS          = "privateDNS"
	IncludePublicIPs           = "publicIPs"
	IncludeResourceGroups      = "resourceGroups"
	IncludeSecurityGroups      = "securityGroups"
	IncludeStorageAccounts     = "storageAccounts"
	IncludeTrafficManager      = "trafficManager"
	IncludeVirtualMachines     = "virtualMachines"
	IncludeVirtualNetworks     = "virtualNetworks"
)

var activityLogLastRecordTime = sync.Map{}

var activityLogFilter = strings.Join([]string{
	"authorization",
	"correlationId",
	"description",
	"eventDataId",
	"eventName",
	"eventTimestamp",
	"httpRequest",
	"level",
	"operationId",
	"operationName",
	"properties",
	"resourceGroupName",
	"resourceId",
	"resourceProviderName",
	"resourceType",
	"status",
	"submissionTimestamp",
	"subscriptionId",
	"subStatus",
}, ",")

var defaultExcludes = []v1.ConfigFieldExclusion{
	{JSONPath: "$..etag"},
}

type Scraper struct {
	ctx        context.Context
	cred       *azidentity.ClientSecretCredential
	config     *v1.Azure
	hostClient *pluginapi.GRPCHostClient
	scraperID  string
	namespace  string

	graphClient    *msgraphsdkgo.GraphServiceClient
	resourceGroups []string
	subscriptionName string
}

func (s *Scraper) Scrape(ctx context.Context, configs []v1.Azure) (v1.ScrapeResults, error) {
	s.ctx = ctx
	var results v1.ScrapeResults

	for _, _config := range configs {
		config, err := s.hydrateConnection(_config)
		if err != nil {
			results.Errorf(err, "failed to populate connection")
			continue
		}

		config.Transform.Exclude = append(config.Transform.Exclude, defaultExcludes...)

		cred, err := azidentity.NewClientSecretCredential(config.TenantID, config.ClientID.ValueStatic, config.ClientSecret.ValueStatic, nil)
		if err != nil {
			results.Errorf(err, "failed to get credentials for azure")
			continue
		}

		s.config = &config
		s.cred = cred

		if err := s.setGraphClient(); err != nil {
			results.Errorf(err, "failed to create graph client for tenant %s", config.TenantID)
			continue
		}

		tenantResult, err := s.fetchTenantDetails()
		if err != nil {
			results.Errorf(err, "failed to fetch tenant details")
			return results, nil
		}
		results = append(results, tenantResult)

		results = append(results, s.fetchResourceGroups()...)
		results = append(results, s.fetchVirtualMachines()...)
		results = append(results, s.fetchLoadBalancers()...)
		results = append(results, s.fetchVirtualNetworks()...)
		results = append(results, s.fetchContainerRegistries()...)
		results = append(results, s.fetchFirewalls()...)
		results = append(results, s.fetchDatabases()...)
		results = append(results, s.fetchK8s()...)
		results = append(results, s.fetchSubscriptions()...)
		results = append(results, s.fetchStorageAccounts()...)
		results = append(results, s.fetchAppServices()...)
		results = append(results, s.fetchDNS()...)
		results = append(results, s.fetchPrivateDNSZones()...)
		results = append(results, s.fetchTrafficManagerProfiles()...)
		results = append(results, s.fetchNetworkSecurityGroups()...)
		results = append(results, s.fetchPublicIPAddresses()...)
		results = append(results, s.fetchAdvisorAnalysis()...)
		results = append(results, s.fetchActivityLogs()...)

		for i := range results {
			if results[i].ID == "" || results[i].ScraperLess {
				continue
			}
			if len(results[i].Tags) == 0 {
				results[i].Tags = v1.JSONStringMap{}
			}
			results[i].Tags["subscriptionID"] = s.config.SubscriptionID
			if s.subscriptionName != "" {
				results[i].Tags["subscriptionName"] = s.subscriptionName
			}

			for j, parent := range results[i].Parents {
				if parent.ExternalID == "" {
					switch parent.Type {
					case getARMType(lo.ToPtr(ResourceTypeSubscription)):
						continue
					default:
						subscriptionID := strings.Split(results[i].ID, "/")[2]
						results[i].Parents[j].ExternalID = getARMID(lo.ToPtr("/subscriptions/" + subscriptionID))
						results[i].Parents[j].Type = getARMType(lo.ToPtr(ResourceTypeSubscription))
					}
				}
			}
		}

		adResults, err := s.scrapeEntra()
		if err != nil {
			results.Errorf(err, "failed to scrape Entra")
		}
		results = append(results, adResults...)

		for i := range results {
			if results[i].ID == "" || results[i].ScraperLess {
				continue
			}
			if len(results[i].Tags) == 0 {
				results[i].Tags = v1.JSONStringMap{}
			}
			results[i].Tags["Tenant"] = tenantResult.Name
			for _, t := range config.Tags {
				results[i].Tags[t.Name] = t.Value
			}
		}
	}

	for i, r := range results {
		if r.ID == "" {
			continue
		}

		var relateSubscription, relateResourceGroup bool
		switch strings.ToLower(r.Type) {
		case strings.ToLower(ConfigTypePrefix + "SUBSCRIPTION"):
			continue
		case strings.ToLower(ConfigTypePrefix + "Tenant"):
			continue
		case strings.ToLower(ConfigTypePrefix + "MICROSOFT.RESOURCES/RESOURCEGROUPS"):
			relateSubscription = true
		default:
			relateSubscription = true
			relateResourceGroup = true
		}

		if relateSubscription {
			results[i].RelationshipResults = append(results[i].RelationshipResults, v1.RelationshipResult{
				ConfigExternalID:  v1.ExternalID{ExternalID: "/subscriptions/" + s.config.SubscriptionID, ConfigType: ConfigTypePrefix + "SUBSCRIPTION"},
				RelatedExternalID: v1.ExternalID{ExternalID: r.ID, ConfigType: r.Type},
				Relationship:      "Subscription" + strings.TrimPrefix(r.Type, ConfigTypePrefix),
			})
		}

		if relateResourceGroup && extractResourceGroup(r.ID) != "" {
			results[i].RelationshipResults = append(results[i].RelationshipResults, v1.RelationshipResult{
				RelatedExternalID: v1.ExternalID{ExternalID: r.ID, ConfigType: r.Type},
				ConfigExternalID: v1.ExternalID{
					ConfigType: ConfigTypePrefix + "MICROSOFT.RESOURCES/RESOURCEGROUPS",
					ExternalID: fmt.Sprintf("/subscriptions/%s/resourcegroups/%s", s.config.SubscriptionID, extractResourceGroup(r.ID)),
				},
				Relationship: "Resourcegroup" + strings.TrimPrefix(r.Type, ConfigTypePrefix),
			})
			if len(results[i].Tags) == 0 {
				results[i].Tags = v1.JSONStringMap{}
			}
			results[i].Tags["resourceGroup"] = extractResourceGroup(r.ID)
		}
	}

	return results, nil
}

func (s *Scraper) hydrateConnection(t v1.Azure) (v1.Azure, error) {
	if t.ConnectionName != "" {
		conn, err := s.hostClient.HydrateConnection(s.ctx, t.ConnectionName, s.namespace)
		if err != nil {
			return t, fmt.Errorf("could not hydrate connection: %w", err)
		}
		t.ClientID.ValueStatic = conn.Username
		t.ClientSecret.ValueStatic = conn.Password
		t.TenantID = conn.Properties["tenant"]
		return t, nil
	}

	var err error
	if t.ClientID.ValueStatic == "" {
		ev := pluginapi.EnvVarFromDuty(t.ClientID)
		t.ClientID.ValueStatic, err = s.hostClient.GetEnvValue(s.ctx, ev, s.namespace)
		if err != nil {
			return t, fmt.Errorf("failed to get client id: %w", err)
		}
	}

	if t.ClientSecret.ValueStatic == "" {
		ev := pluginapi.EnvVarFromDuty(t.ClientSecret)
		t.ClientSecret.ValueStatic, err = s.hostClient.GetEnvValue(s.ctx, ev, s.namespace)
		if err != nil {
			return t, fmt.Errorf("failed to get client secret: %w", err)
		}
	}

	return t, nil
}

func (s *Scraper) fetchActivityLogs() v1.ScrapeResults {
	if !s.config.Includes(IncludeActivityLogs) {
		return nil
	}

	logger.Tracef("fetching activity logs for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults

	clientFactory, err := armmonitor.NewClientFactory(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate arm monitor client: %w", err)})
	}

	var corelatedActivities = map[string][]activityChangeRecord{}

	var recordSince = time.Now().Add(-defaultActivityLogMaxage)
	if v, ok := activityLogLastRecordTime.Load(s.config.SubscriptionID); ok {
		recordSince = v.(time.Time)
	}

	filter := fmt.Sprintf("eventTimestamp ge '%s'", recordSince.Format(time.RFC3339))
	pager := clientFactory.NewActivityLogsClient().NewListPager(filter, &armmonitor.ActivityLogsClientListOptions{Select: &activityLogFilter})
	for pager.More() {
		page, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read activity logs next page: %w", err)})
		}
		logger.Tracef("Fetched %d activity logs", len(page.Value))

		for _, v := range page.Value {
			if recordSince.Before(utils.Deref(v.EventTimestamp)) {
				recordSince = *v.EventTimestamp
			}

			if s.config.Exclusions != nil && collections.Contains(s.config.Exclusions.ActivityLogs, utils.Deref(v.OperationName.Value)) {
				continue
			}

			change := v1.ChangeResult{
				ChangeType:       utils.Deref(v.OperationName.Value),
				CreatedAt:        v.EventTimestamp,
				Details:          v1.NewJSON(*v),
				ExternalChangeID: utils.Deref(v.CorrelationID),
				ExternalID:       strings.ToLower(utils.Deref(v.ResourceID)),
				ConfigType:       getARMType(v.ResourceType.Value),
				Severity:         string(getSeverityFromReason(v)),
				Source:           ConfigTypePrefix + "ActivityLog",
				Summary:          utils.Deref(v.OperationName.LocalizedValue),
			}
			corelatedActivities[utils.Deref(v.CorrelationID)] = append(corelatedActivities[utils.Deref(v.CorrelationID)], activityChangeRecord{
				Result:    change,
				EventData: v,
			})
		}
		break
	}

	activityLogLastRecordTime.Store(s.config.SubscriptionID, recordSince)

	for _, changeRecords := range corelatedActivities {
		change := changeRecords[0].Result
		change.UpdateExisting = true

		statusTimestamps := map[string]*time.Time{}
		for i := range changeRecords {
			change.CreatedAt = changeRecords[i].EventData.EventTimestamp

			status := utils.Deref(changeRecords[i].EventData.Status.Value)
			if status == "" {
				continue
			}
			statusTimestamps[strings.ToLower(status)] = changeRecords[i].EventData.EventTimestamp

			if _, ok := change.Details["httpRequest"]; !ok {
				change.Details["httpRequest"] = changeRecords[i].EventData
			}
		}

		change.Details["timestamps"] = statusTimestamps
		results = append(results, v1.ScrapeResult{Changes: []v1.ChangeResult{change}})
	}

	return results
}

type activityChangeRecord struct {
	Result    v1.ChangeResult
	EventData *armmonitor.EventData
}

func getSeverityFromReason(v *armmonitor.EventData) models.Severity {
	switch utils.Deref(v.Level) {
	case "Critical":
		return models.SeverityCritical
	case "Error":
		return models.SeverityHigh
	case "Warning":
		return models.SeverityMedium
	case "Verbose":
		return models.SeverityLow
	default:
		return models.SeverityInfo
	}
}

func (s *Scraper) fetchDatabases() v1.ScrapeResults {
	if !s.config.Includes(IncludeDatabases) {
		return nil
	}

	logger.Tracef("fetching databases for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	databases, err := armresources.NewClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate database client: %w", err)})
	}
	options := &armresources.ClientListOptions{
		Filter: to.Ptr(`
            ResourceType eq 'Microsoft.DBforPostgreSQL/servers' or
            ResourceType eq 'Microsoft.Sql/servers/databases'
        `),
	}
	dbs := databases.NewListPager(options)
	for dbs.More() {
		nextPage, err := dbs.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read database page: %w", err)})
		}
		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "RelationalDatabase",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}
	return results
}

func (s *Scraper) fetchK8s() v1.ScrapeResults {
	if !s.config.Includes(IncludeK8s) {
		return nil
	}

	logger.Tracef("fetching k8s for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	managedClustersClient, err := armcontainerservice.NewManagedClustersClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate k8s client: %w", err)})
	}

	k8sPager := managedClustersClient.NewListPager(nil)
	for k8sPager.More() {
		nextPage, err := k8sPager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read k8s page: %w", err)})
		}
		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "KubernetesCluster",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}
	return results
}

func (s *Scraper) fetchFirewalls() v1.ScrapeResults {
	if !s.config.Includes(IncludeFirewalls) {
		return nil
	}

	logger.Tracef("fetching firewalls for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	firewallClient, err := armnetwork.NewAzureFirewallsClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate firewall client: %w", err)})
	}

	firewallsPager := firewallClient.NewListAllPager(nil)
	for firewallsPager.More() {
		nextPage, err := firewallsPager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read firewall page: %w", err)})
		}
		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "Firewall",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}
	return results
}

func (s *Scraper) fetchContainerRegistries() v1.ScrapeResults {
	if !s.config.Includes(IncludeContainerRegistries) {
		return nil
	}

	logger.Tracef("fetching container registries for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	registriesClient, err := armcontainerregistry.NewRegistriesClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate container registries client: %w", err)})
	}
	registriesPager := registriesClient.NewListPager(nil)
	for registriesPager.More() {
		nextPage, err := registriesPager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read container registries page: %w", err)})
		}
		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "ContainerRegistry",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}
	return results
}

func (s *Scraper) fetchVirtualNetworks() v1.ScrapeResults {
	if !s.config.Includes(IncludeVirtualNetworks) {
		return nil
	}

	logger.Tracef("fetching virtual networks for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	virtualNetworksClient, err := armnetwork.NewVirtualNetworksClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate virtual network client: %w", err)})
	}

	virtualNetworksPager := virtualNetworksClient.NewListAllPager(nil)
	for virtualNetworksPager.More() {
		nextPage, err := virtualNetworksPager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read virtual network page: %w", err)})
		}
		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				ConfigID:    v.Properties.ResourceGUID,
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "VirtualNetwork",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}
	return results
}

func (s *Scraper) fetchLoadBalancers() v1.ScrapeResults {
	if !s.config.Includes(IncludeLoadBalancers) {
		return nil
	}

	logger.Tracef("fetching load balancers for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	lbClient, err := armnetwork.NewLoadBalancersClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate load balancer client: %w", err)})
	}

	loadBalancersPager := lbClient.NewListAllPager(nil)
	for loadBalancersPager.More() {
		nextPage, err := loadBalancersPager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read load balancer page: %w", err)})
		}
		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				ConfigID:    v.Properties.ResourceGUID,
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "LoadBalancer",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}
	return results
}

func (s *Scraper) fetchVirtualMachines() v1.ScrapeResults {
	if !s.config.Includes(IncludeVirtualMachines) {
		return nil
	}

	logger.Tracef("fetching virtual machines for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	virtualMachineClient, err := armcompute.NewVirtualMachinesClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate virtual machine client: %w", err)})
	}

	virtualMachinePager := virtualMachineClient.NewListAllPager(nil)
	for virtualMachinePager.More() {
		nextPage, err := virtualMachinePager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed read virtual machine page: %w", err)})
		}
		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				ConfigID:    v.Properties.VMID,
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: models.ConfigClassVirtualMachine,
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	vmScaleSetClient, err := armcompute.NewVirtualMachineScaleSetsClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate virtual machine scale set client: %w", err)})
	}

	vmScaleSetVMsClient, err := armcompute.NewVirtualMachineScaleSetVMsClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate virtual machine scale set vms client: %w", err)})
	}

	vmScaleSetPager := vmScaleSetClient.NewListAllPager(nil)
	for vmScaleSetPager.More() {
		nextPage, err := vmScaleSetPager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed read virtual machine scale set page: %w", err)})
		}

		for _, vmScaleSet := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				ConfigID:    vmScaleSet.Properties.UniqueID,
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(vmScaleSet.ID),
				Name:        deref(vmScaleSet.Name),
				Config:      vmScaleSet,
				ConfigClass: models.ConfigClassNode,
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(vmScaleSet.ID), getARMType(vmScaleSet.Type))},
				Type:        getARMType(vmScaleSet.Type),
			})

			for _, rg := range s.resourceGroups {
				scaleSetVMsPager := vmScaleSetVMsClient.NewListPager(rg, deref(vmScaleSet.Name), nil)
				for scaleSetVMsPager.More() {
					nextPage, err := scaleSetVMsPager.NextPage(s.ctx)
					if err != nil {
						break
					}
					for _, v := range nextPage.Value {
						results = append(results, v1.ScrapeResult{
							ConfigID:    v.Properties.VMID,
							BaseScraper: s.config.BaseScraper,
							ID:          getARMID(v.ID),
							Name:        deref(v.Name),
							Config:      v,
							ConfigClass: models.ConfigClassVirtualMachine,
							Type:        getARMType(v.Type),
							Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
							Aliases:     []string{*v.Properties.OSProfile.ComputerName},
						})
					}
				}
			}
		}
	}

	return results
}

func (s *Scraper) fetchResourceGroups() v1.ScrapeResults {
	if !s.config.Includes(IncludeResourceGroups) {
		return nil
	}

	logger.Tracef("fetching resource groups for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	resourceClient, err := armresources.NewResourceGroupsClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate resource group client: %w", err)})
	}

	resourcePager := resourceClient.NewListPager(nil)
	for resourcePager.More() {
		nextPage, err := resourcePager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed reading resource group page: %w", err)})
		}

		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "ResourceGroup",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})

			s.resourceGroups = append(s.resourceGroups, deref(v.Name))
		}
	}
	return results
}

func (s *Scraper) fetchSubscriptions() v1.ScrapeResults {
	logger.Tracef("fetching subscriptions")

	var results v1.ScrapeResults
	client, err := armsubscription.NewSubscriptionsClient(s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate subscriptions client: %w", err)})
	}

	pager := client.NewListPager(nil)
	for pager.More() {
		respPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read subscription next page: %w", err)})
		}

		for _, v := range respPage.Value {
			s.subscriptionName = deref(v.DisplayName)

			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        *v.DisplayName,
				Config:      v,
				ConfigClass: "Subscription",
				Type:        getARMType(utils.Ptr("Subscription")),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(utils.Ptr("Subscription")))},
			})
		}
	}

	return results
}

func (s *Scraper) fetchStorageAccounts() v1.ScrapeResults {
	if !s.config.Includes(IncludeStorageAccounts) {
		return nil
	}

	logger.Tracef("fetching storage accounts for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	client, err := armstorage.NewAccountsClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate storage account client: %w", err)})
	}

	pager := client.NewListPager(nil)
	for pager.More() {
		respPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read storage account next page: %w", err)})
		}

		for _, v := range respPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "StorageAccount",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	return results
}

func (s *Scraper) fetchAppServices() v1.ScrapeResults {
	if !s.config.Includes(IncludeAppServices) {
		return nil
	}

	logger.Tracef("fetching web services for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	client, err := armappservice.NewWebAppsClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate app services client: %w", err)})
	}

	pager := client.NewListPager(nil)
	for pager.More() {
		respPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read app services next page: %w", err)})
		}

		for _, v := range respPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "AppService",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	return results
}

func (s *Scraper) fetchDNS() v1.ScrapeResults {
	if !s.config.Includes(IncludeDNS) {
		return nil
	}

	logger.Tracef("fetching dns zones for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	client, err := armdns.NewZonesClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate dns zone client: %w", err)})
	}

	pager := client.NewListPager(nil)
	for pager.More() {
		respPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read dns zone next page: %w", err)})
		}

		for _, v := range respPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "DNSZone",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	return results
}

func (s *Scraper) fetchPrivateDNSZones() v1.ScrapeResults {
	if !s.config.Includes(IncludePrivateDNS) {
		return nil
	}

	logger.Tracef("fetching private DNS zones for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	client, err := armprivatedns.NewPrivateZonesClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate private DNS zones client: %w", err)})
	}

	pager := client.NewListPager(nil)
	for pager.More() {
		nextPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read private DNS zones page: %w", err)})
		}

		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "PrivateDNSZone",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	return results
}

func (s *Scraper) fetchTrafficManagerProfiles() v1.ScrapeResults {
	if !s.config.Includes(IncludeTrafficManager) {
		return nil
	}

	logger.Tracef("fetching traffic manager profiles for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	client, err := armtrafficmanager.NewProfilesClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate traffic manager profile client: %w", err)})
	}

	pager := client.NewListBySubscriptionPager(nil)
	for pager.More() {
		respPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read traffic manager profile next page: %w", err)})
		}

		for _, v := range respPage.Value {
			results = append(results, v1.ScrapeResult{
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "TrafficManagerProfile",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	return results
}

func (s *Scraper) fetchNetworkSecurityGroups() v1.ScrapeResults {
	if !s.config.Includes(IncludeSecurityGroups) {
		return nil
	}

	logger.Tracef("fetching network security groups for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	client, err := armnetwork.NewSecurityGroupsClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate network security groups client: %w", err)})
	}

	pager := client.NewListAllPager(nil)
	for pager.More() {
		nextPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read network security groups page: %w", err)})
		}

		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				ConfigID:    v.Properties.ResourceGUID,
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "SecurityGroup",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	return results
}

func (s *Scraper) fetchPublicIPAddresses() v1.ScrapeResults {
	if !s.config.Includes(IncludePublicIPs) {
		return nil
	}

	logger.Tracef("fetching public IP addresses for subscription %s", s.config.SubscriptionID)

	var results v1.ScrapeResults
	client, err := armnetwork.NewPublicIPAddressesClient(s.config.SubscriptionID, s.cred, nil)
	if err != nil {
		return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to initiate public IP addresses client: %w", err)})
	}

	pager := client.NewListAllPager(nil)
	for pager.More() {
		nextPage, err := pager.NextPage(s.ctx)
		if err != nil {
			return append(results, v1.ScrapeResult{Error: fmt.Errorf("failed to read public IP addresses page: %w", err)})
		}

		for _, v := range nextPage.Value {
			results = append(results, v1.ScrapeResult{
				ConfigID:    v.Properties.ResourceGUID,
				BaseScraper: s.config.BaseScraper,
				ID:          getARMID(v.ID),
				Name:        deref(v.Name),
				Config:      v,
				ConfigClass: "PublicIPAddress",
				Type:        getARMType(v.Type),
				Properties:  []*types.Property{getConsoleLink(lo.FromPtr(v.ID), getARMType(v.Type))},
			})
		}
	}

	return results
}

func (s *Scraper) setGraphClient() error {
	if s.graphClient != nil {
		return nil
	}

	graphCred, err := azidentity.NewClientSecretCredential(s.config.TenantID, s.config.ClientID.ValueStatic, s.config.ClientSecret.ValueStatic, nil)
	if err != nil {
		return fmt.Errorf("failed to create graph credentials: %w", err)
	}

	client, err := msgraphsdkgo.NewGraphServiceClientWithCredentials(graphCred, []string{"https://graph.microsoft.com/.default"})
	if err != nil {
		return fmt.Errorf("failed to create graph client: %w", err)
	}

	s.graphClient = client
	return nil
}

func (s *Scraper) fetchTenantDetails() (v1.ScrapeResult, error) {
	logger.Tracef("fetching tenant details for tenant %s", s.config.TenantID)

	orgCollection, err := s.graphClient.Organization().Get(s.ctx, nil)
	if err != nil {
		return v1.ScrapeResult{}, fmt.Errorf("failed to fetch organization details: %w", err)
	}

	if len(orgCollection.GetValue()) == 0 {
		return v1.ScrapeResult{}, fmt.Errorf("no organization details found for tenant %s", s.config.TenantID)
	}

	for _, org := range orgCollection.GetValue() {
		if lo.FromPtr(org.GetId()) != s.config.TenantID {
			continue
		}

		return v1.ScrapeResult{
			BaseScraper: s.config.BaseScraper,
			ID:          s.config.TenantID,
			ConfigID:    lo.ToPtr(s.config.TenantID),
			Name:        deref(org.GetDisplayName()),
			Config:      org.GetBackingStore().Enumerate(),
			ConfigClass: "Tenant",
			Type:        ConfigTypePrefix + "Tenant",
			Properties: []*types.Property{
				getConsoleLink("/tenants/"+s.config.TenantID, ConfigTypePrefix+"Tenant"),
			},
		}, nil
	}

	return v1.ScrapeResult{}, fmt.Errorf("no organization details found for tenant %s", s.config.TenantID)
}

func getConsoleLink(resourceID, resourceType string) *types.Property {
	return &types.Property{
		Name: "URL",
		Icon: resourceType,
		Links: []types.Link{
			{
				Text: types.Text{Label: "Console"},
				URL:  fmt.Sprintf("https://portal.azure.com/#resource%s", resourceID),
			},
		},
	}
}

func getARMID(id *string) string {
	return strings.ToLower(deref(id))
}

func getARMType(rType *string) string {
	return "Azure::" + deref(rType)
}

func extractResourceGroup(resourceID string) string {
	resourceID = strings.Trim(resourceID, " ")
	resourceID = strings.TrimPrefix(resourceID, "/")

	segments := strings.Split(resourceID, "/")
	if len(segments) < 4 {
		return ""
	}

	if segments[2] != "resourcegroups" {
		return ""
	}

	return segments[3]
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
