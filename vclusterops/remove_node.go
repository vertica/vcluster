/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package vclusterops

import (
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VRemoveNodeOptions represents the available options to remove one or more nodes from
// the database.
type VRemoveNodeOptions struct {
	DatabaseOptions
	HostsToRemove []string // Hosts to remove from database
	Initiator     string   // A primary up host that will be used to execute remove_node operations.
	ForceDelete   *bool    // whether force delete directories
}

func VRemoveNodeOptionsFactory() VRemoveNodeOptions {
	opt := VRemoveNodeOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (o *VRemoveNodeOptions) setDefaultValues() {
	o.DatabaseOptions.setDefaultValues()

	o.ForceDelete = new(bool)
	*o.ForceDelete = true
}

// ParseHostToRemoveList converts a comma-separated string list of hosts into a slice of host names
// to remove from the database. During parsing, the hosts are converted to lowercase.
// It returns any parsing error encountered.
func (o *VRemoveNodeOptions) ParseHostToRemoveList(hosts string) error {
	inputHostList, err := util.SplitHosts(hosts)
	if err != nil {
		if len(inputHostList) == 0 {
			return fmt.Errorf("must specify at least one host to remove")
		}
	}

	o.HostsToRemove = inputHostList
	return nil
}

func (o *VRemoveNodeOptions) validateRequiredOptions(log vlog.Printer) error {
	err := o.validateBaseOptions("db_remove_node", log)
	if err != nil {
		return err
	}
	return nil
}

func (o *VRemoveNodeOptions) validateExtraOptions() error {
	if !*o.HonorUserInput {
		return nil
	}
	// data prefix
	return util.ValidateRequiredAbsPath(o.DataPrefix, "data path")
}

func (o *VRemoveNodeOptions) validateParseOptions(log vlog.Printer) error {
	// batch 1: validate required params
	err := o.validateRequiredOptions(log)
	if err != nil {
		return err
	}
	// batch 2: validate all other params
	return o.validateExtraOptions()
}

func (o *VRemoveNodeOptions) analyzeOptions() (err error) {
	o.HostsToRemove, err = util.ResolveRawHostsToAddresses(o.HostsToRemove, o.Ipv6.ToBool())
	if err != nil {
		return err
	}

	// we analyze host names when HonorUserInput is set, otherwise we use hosts in yaml config
	if *o.HonorUserInput {
		// resolve RawHosts to be IP addresses
		o.Hosts, err = util.ResolveRawHostsToAddresses(o.RawHosts, o.Ipv6.ToBool())
		if err != nil {
			return err
		}
		o.normalizePaths()
	}
	return nil
}

func (o *VRemoveNodeOptions) validateAnalyzeOptions(log vlog.Printer) error {
	if err := o.validateParseOptions(log); err != nil {
		return err
	}
	err := o.analyzeOptions()
	if err != nil {
		return err
	}
	return o.setUsePassword(log)
}

func (vcc *VClusterCommands) VRemoveNode(options *VRemoveNodeOptions) (VCoordinationDatabase, error) {
	vdb := makeVCoordinationDatabase()

	// validate and analyze options
	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return vdb, err
	}

	// get db name and hosts from config file and options.
	dbName, hosts, err := options.getNameAndHosts(options.Config)
	if err != nil {
		return vdb, err
	}

	options.DBName = &dbName
	options.Hosts = hosts
	// get depot, data and catalog prefix from config file or options
	*options.DepotPrefix, *options.DataPrefix, err = options.getDepotAndDataPrefix(options.Config)
	if err != nil {
		return vdb, err
	}
	options.CatalogPrefix, err = options.getCatalogPrefix(options.Config)
	if err != nil {
		return vdb, err
	}

	err = vcc.getVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return vdb, err
	}

	err = options.completeVDBSetting(&vdb)
	if err != nil {
		return vdb, err
	}

	// remove_node is aborted if requirements are not met.
	err = checkRemoveNodeRequirements(&vdb)
	if err != nil {
		return vdb, err
	}
	// Figure out if the nodes to remove exist in the catalog. We follow
	// *normal* remove node logic if it still exists in the catalog. We tolerate
	// requests for nodes that aren't in the catalog because the caller may not
	// know (e.g. previous attempt to remove node didn't come back successful).
	// We have a simplified remove process for those requests to remove state
	// that the caller may be checking.
	var hostsNotInCatalog []string
	options.HostsToRemove, hostsNotInCatalog = vdb.containNodes(options.HostsToRemove)

	vdb, err = vcc.removeNodesInCatalog(options, &vdb)
	if err != nil || len(hostsNotInCatalog) == 0 {
		return vdb, err
	}

	return vcc.handleRemoveNodeForHostsNotInCatalog(&vdb, options, hostsNotInCatalog)
}

// removeNodesInCatalog will perform the steps to remove nodes. The node list in
// options.HostsToRemove has already been verified that each node is in the
// catalog.
func (vcc *VClusterCommands) removeNodesInCatalog(options *VRemoveNodeOptions, vdb *VCoordinationDatabase) (VCoordinationDatabase, error) {
	if len(options.HostsToRemove) == 0 {
		vcc.Log.Info("Exit early because there are no hosts to remove")
		return *vdb, nil
	}
	vcc.Log.V(1).Info("validated input hosts", "HostsToRemove", options.HostsToRemove)

	err := options.setInitiator(vdb.PrimaryUpNodes)
	if err != nil {
		return *vdb, err
	}

	instructions, err := vcc.produceRemoveNodeInstructions(vdb, options)
	if err != nil {
		return *vdb, fmt.Errorf("fail to produce remove node instructions, %w", err)
	}

	remainingHosts := util.SliceDiff(vdb.HostList, options.HostsToRemove)

	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.run(vcc.Log); runError != nil {
		// If the machines of the to-be-removed nodes crashed or get killed,
		// the run error may be ignored.
		// Here we check whether the to-be-removed nodes are still in the catalog.
		// If they have been removed from catalog, we let remove_node succeed.
		if vcc.findRemovedNodesInCatalog(options, remainingHosts) {
			return *vdb, fmt.Errorf("fail to complete remove node operation, %w", runError)
		}
		// If the target nodes have already been removed from catalog,
		// show a warning about the run error for users to trouble shoot their machines
		vcc.Log.PrintWarning("Nodes have been successfully removed, but encountered the following problems: %v",
			runError)
	}

	// we return a vdb that contains only the remaining hosts
	return vdb.copy(remainingHosts), nil
}

// handleRemoveNodeForHostsNotInCatalog will build and execute a list of
// instructions to do remove of hosts that aren't present in the catalog. We
// will do basic cleanup logic for this needed by the operator.
func (vcc *VClusterCommands) handleRemoveNodeForHostsNotInCatalog(vdb *VCoordinationDatabase, options *VRemoveNodeOptions,
	missingHosts []string) (VCoordinationDatabase, error) {
	vcc.Log.Info("Doing cleanup of hosts missing from database", "hostsNotInCatalog", missingHosts)

	// We need to find the paths for the hosts we are removing.
	nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(missingHosts, *options.DBName, *options.CatalogPrefix,
		false /* report all errors */, vdb)
	instructions := []clusterOp{&nmaGetNodesInfoOp}
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	opEng := makeClusterOpEngine(instructions, &certs)
	err := opEng.run(vcc.Log)
	if err != nil {
		return *vdb, fmt.Errorf("failed to get node info for missing hosts: %w", err)
	}

	// Make a vdb of just the missing hosts. The host list for
	// nmaDeleteDirectoriesOp uses the host list from the vdb.
	vdbForDeleteDir := vdb.copy(missingHosts)
	err = options.completeVDBSetting(&vdbForDeleteDir)
	if err != nil {
		return *vdb, err
	}

	// Using the paths fetched earlier, we can now build the list of directories
	// that the NMA should remove.
	nmaDeleteDirectoriesOp, err := makeNMADeleteDirectoriesOp(&vdbForDeleteDir, *options.ForceDelete)
	if err != nil {
		return *vdb, err
	}
	instructions = []clusterOp{&nmaDeleteDirectoriesOp}
	opEng = makeClusterOpEngine(instructions, &certs)
	err = opEng.run(vcc.Log)
	if err != nil {
		return *vdb, fmt.Errorf("failed to delete directories for missing hosts: %w", err)
	}

	remainingHosts := util.SliceDiff(vdb.HostList, missingHosts)
	return vdb.copy(remainingHosts), nil
}

// checkRemoveNodeRequirements validates any remove_node requirements. It will
// return an error if a requirement isn't met.
func checkRemoveNodeRequirements(vdb *VCoordinationDatabase) error {
	if !vdb.IsEon {
		if vdb.hasAtLeastOneDownNode() {
			return errors.New("all nodes must be up or standby")
		}
	}
	return nil
}

// completeVDBSetting sets some VCoordinationDatabase fields we cannot get yet
// from the https endpoints. We set those fields from options.
func (o *VRemoveNodeOptions) completeVDBSetting(vdb *VCoordinationDatabase) error {
	vdb.DataPrefix = *o.DataPrefix

	if *o.DepotPrefix == "" {
		return nil
	}
	if *o.HonorUserInput && vdb.IsEon {
		// checking this here because now we have got eon value from
		// the running db. This will be removed once we are able to get
		// the depot path from db through an https endpoint(VER-88122).
		err := util.ValidateRequiredAbsPath(o.DepotPrefix, "depot path")
		if err != nil {
			return err
		}
	}
	vdb.DepotPrefix = *o.DepotPrefix
	hostNodeMap := makeVHostNodeMap()
	// we set the depot path manually because there is not yet an https endpoint for
	// that(VER-88122). This is useful for nmaDeleteDirectoriesOp.
	for h, vnode := range vdb.HostNodeMap {
		vnode.DepotPath = vdb.genDepotPath(vnode.Name)
		hostNodeMap[h] = vnode
	}
	vdb.HostNodeMap = hostNodeMap
	return nil
}

// produceRemoveNodeInstructions will build a list of instructions to execute for
// the remove node operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful remove_node:
//   - Update ksafety if needed
//   - Mark nodes to remove as ephemeral
//   - Rebalance cluster for Enterprise mode, rebalance shards for Eon mode
//   - Poll subscription state, wait for all subscrptions ACTIVE for Eon mode
//   - Remove secondary nodes from spread
//   - Drop Nodes
//   - Reload spread
//   - Delete catalog and data directories
//   - Sync catalog (eon only)
func (vcc *VClusterCommands) produceRemoveNodeInstructions(vdb *VCoordinationDatabase, options *VRemoveNodeOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	var initiatorHost []string
	initiatorHost = append(initiatorHost, options.Initiator)

	username := *options.UserName
	usePassword := options.usePassword
	password := options.Password

	if (len(vdb.HostList) - len(options.HostsToRemove)) < ksafetyThreshold {
		httpsMarkDesignKSafeOp, e := makeHTTPSMarkDesignKSafeOp(initiatorHost, usePassword, username,
			password, ksafeValueZero)
		if e != nil {
			return instructions, e
		}
		instructions = append(instructions, &httpsMarkDesignKSafeOp)
	}

	err := vcc.produceMarkEphemeralNodeOps(&instructions, options.HostsToRemove, initiatorHost,
		usePassword, username, password, vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}

	// this is a copy of the original that only
	// contains the hosts to remove.
	v := vdb.copy(options.HostsToRemove)
	if vdb.IsEon {
		// we pass the set of subclusters of the nodes to remove.
		err = vcc.produceRebalanceSubclusterShardsOps(&instructions, initiatorHost, v.getSCNames(),
			usePassword, username, password)
		if err != nil {
			return instructions, err
		}

		// for Eon DB, we check whethter all subscriptions are ACTIVE
		// after rebalance shards
		httpsPollSubscriptionStateOp, e := makeHTTPSPollSubscriptionStateOp(initiatorHost,
			usePassword, username, password)
		if e != nil {
			return instructions, e
		}
		instructions = append(instructions, &httpsPollSubscriptionStateOp)
	} else {
		var httpsRBCOp httpsRebalanceClusterOp
		httpsRBCOp, err = makeHTTPSRebalanceClusterOp(initiatorHost, usePassword, username,
			password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsRBCOp)
	}

	// only remove secondary nodes from spread
	err = vcc.produceSpreadRemoveNodeOp(&instructions, options.HostsToRemove,
		usePassword, username, password,
		initiatorHost, vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}

	err = vcc.produceDropNodeOps(&instructions, options.HostsToRemove, initiatorHost,
		usePassword, username, password, vdb.HostNodeMap, vdb.IsEon)
	if err != nil {
		return instructions, err
	}

	httpsReloadSpreadOp, err := makeHTTPSReloadSpreadOpWithInitiator(initiatorHost, true, username, password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &httpsReloadSpreadOp)

	nmaDeleteDirectoriesOp, err := makeNMADeleteDirectoriesOp(&v, *options.ForceDelete)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &nmaDeleteDirectoriesOp)

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(initiatorHost, true, username, password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}

// produceMarkEphemeralNodeOps gets a slice of target hosts and for each of them
// produces an HTTPSMarkEphemeralNodeOp.
func (vcc *VClusterCommands) produceMarkEphemeralNodeOps(instructions *[]clusterOp, targetHosts, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	hostNodeMap vHostNodeMap) error {
	for _, host := range targetHosts {
		httpsMarkEphemeralNodeOp, err := makeHTTPSMarkEphemeralNodeOp(hostNodeMap[host].Name, hosts,
			useHTTPPassword, userName, httpsPassword)
		if err != nil {
			return err
		}
		*instructions = append(*instructions, &httpsMarkEphemeralNodeOp)
	}
	return nil
}

// produceRebalanceSubclusterShardsOps gets a slice of subclusters and for each of them
// produces an HTTPSRebalanceSubclusterShardsOp.
func (vcc *VClusterCommands) produceRebalanceSubclusterShardsOps(instructions *[]clusterOp, initiatorHost, scNames []string,
	useHTTPPassword bool, userName string, httpsPassword *string) error {
	for _, scName := range scNames {
		op, err := makeHTTPSRebalanceSubclusterShardsOp(
			initiatorHost, useHTTPPassword, userName, httpsPassword, scName)
		if err != nil {
			return err
		}
		*instructions = append(*instructions, &op)
	}

	return nil
}

// produceDropNodeOps produces an HTTPSDropNodeOp for each node to drop.
// This is because we must drop node one by one to avoid losing quorum.
func (vcc *VClusterCommands) produceDropNodeOps(instructions *[]clusterOp, targetHosts, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	hostNodeMap vHostNodeMap, isEon bool) error {
	for _, host := range targetHosts {
		httpsDropNodeOp, err := makeHTTPSDropNodeOp(hostNodeMap[host].Name, hosts,
			useHTTPPassword, userName, httpsPassword,
			isEon && hostNodeMap[host].State == util.NodeDownState)
		if err != nil {
			return err
		}
		*instructions = append(*instructions, &httpsDropNodeOp)
	}

	return nil
}

// produceSpreadRemoveNodeOp calls HTTPSSpreadRemoveNodeOp
// when there is at least one secondary node to remove
func (vcc *VClusterCommands) produceSpreadRemoveNodeOp(instructions *[]clusterOp, hostsToRemove []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	initiatorHost []string, hostNodeMap vHostNodeMap) error {
	// find secondary nodes from HostsToRemove
	var secondaryHostsToRemove []string
	for _, h := range hostsToRemove {
		vnode, ok := hostNodeMap[h]
		if !ok {
			return fmt.Errorf("cannot find host %s from vdb.HostNodeMap", h)
		}
		if !vnode.IsPrimary {
			secondaryHostsToRemove = append(secondaryHostsToRemove, h)
		}
	}

	// only call HTTPSSpreadRemoveNodeOp for secondary nodes to remove
	if len(secondaryHostsToRemove) > 0 {
		httpsSpreadRemoveNodeOp, err := makeHTTPSSpreadRemoveNodeOp(secondaryHostsToRemove, initiatorHost,
			useHTTPPassword, userName, httpsPassword, hostNodeMap)
		if err != nil {
			return err
		}
		*instructions = append(*instructions, &httpsSpreadRemoveNodeOp)
	}

	return nil
}

// setInitiator sets the initiator as the first primary up node that is not
// in the list of hosts to remove.
func (o *VRemoveNodeOptions) setInitiator(primaryUpNodes []string) error {
	initiatorHost, err := getInitiatorHost(primaryUpNodes, o.HostsToRemove)
	if err != nil {
		return err
	}
	o.Initiator = initiatorHost
	return nil
}

// findRemovedNodesInCatalog checks whether the to-be-removed nodes are still in catalog.
// Return true if they are still in catalog.
func (vcc *VClusterCommands) findRemovedNodesInCatalog(options *VRemoveNodeOptions,
	remainingHosts []string) bool {
	fetchNodeStateOpt := VFetchNodeStateOptionsFactory()
	fetchNodeStateOpt.DBName = options.DBName
	fetchNodeStateOpt.RawHosts = remainingHosts
	fetchNodeStateOpt.Ipv6 = options.Ipv6
	fetchNodeStateOpt.UserName = options.UserName
	fetchNodeStateOpt.Password = options.Password
	*fetchNodeStateOpt.HonorUserInput = true

	var nodesInformation nodesInfo
	res, err := vcc.VFetchNodeState(&fetchNodeStateOpt)
	if err != nil {
		vcc.Log.PrintWarning("Fail to fetch states of the nodes, detail: %v", err)
		return false
	}
	nodesInformation.NodeList = res

	// return true if the target (to-be-removed) nodes are still in catalog
	return nodesInformation.findHosts(options.HostsToRemove)
}
