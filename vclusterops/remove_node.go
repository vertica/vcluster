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
	"path/filepath"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VRemoveNodeOptions are the option arguments for the VRemoveNode API
type VRemoveNodeOptions struct {
	DatabaseOptions
	// Hosts to remove from database
	HostsToRemove []string
	// A primary up host that will be used to execute
	// remove_node operations.
	Initiator   string
	ForceDelete *bool
	NodeStates  []NodeStateInfo
}

func VRemoveNodeOptionsFactory() VRemoveNodeOptions {
	opt := VRemoveNodeOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (o *VRemoveNodeOptions) SetDefaultValues() {
	o.DatabaseOptions.SetDefaultValues()

	o.ForceDelete = new(bool)
	*o.ForceDelete = true
}

// ParseHostToRemoveList converts the string list of hosts, to remove, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
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

func (o *VRemoveNodeOptions) validateRequiredOptions() error {
	err := o.ValidateBaseOptions("db_remove_node")
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

func (o *VRemoveNodeOptions) validateParseOptions() error {
	// batch 1: validate required params
	err := o.validateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate all other params
	err = o.validateExtraOptions()
	return err
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

func (o *VRemoveNodeOptions) validateAnalyzeOptions() error {
	if err := o.validateParseOptions(); err != nil {
		return err
	}
	err := o.analyzeOptions()
	if err != nil {
		return err
	}
	return o.SetUsePassword()
}

func (vcc *VClusterCommands) VRemoveNode(options *VRemoveNodeOptions) (VCoordinationDatabase, error) {
	vdb := MakeVCoordinationDatabase()

	config, err := options.GetDBConfig()
	if err != nil {
		return vdb, err
	}

	// validate and analyze options
	err = options.validateAnalyzeOptions()
	if err != nil {
		return vdb, err
	}

	// get db name and hosts from config file and options.
	// this, as well as all the config file related parts,
	// will be moved to cmd_remove_node.go after VER-88122,
	// as the operator does not support config file.
	dbName, hosts := options.GetNameAndHosts(config)
	options.Name = &dbName
	options.Hosts = hosts
	// get depot and data prefix from config file or options
	*options.DepotPrefix, *options.DataPrefix = options.getDepotAndDataPrefix(config)

	err = getVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return vdb, err
	}

	err = completeVDBSetting(&vdb, options)
	if err != nil {
		return vdb, err
	}

	// remove_node is aborted if requirements are not met
	err = checkRemoveNodeRequirements(&vdb, options.HostsToRemove)
	if err != nil {
		return vdb, err
	}

	err = options.setInitiator(vdb.PrimaryUpNodes)
	if err != nil {
		return vdb, err
	}

	instructions, err := produceRemoveNodeInstructions(&vdb, options)
	if err != nil {
		vlog.LogPrintError("failed to produce remove node instructions, %s", err)
		return vdb, err
	}

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.Run(); runError != nil {
		vlog.LogPrintError("failed to complete remove node operation, %s", runError)
		return vdb, runError
	}

	remainingHosts := util.SliceDiff(vdb.HostList, options.HostsToRemove)
	// we return a vdb that contains only the remaining hosts
	return vdb.Copy(remainingHosts), nil
}

// checkRemoveNodeRequirements validates  the following remove_node requirements:
//   - Check the existence of the nodes to remove
//   - Check if all nodes are up or standby (enterprise only)
func checkRemoveNodeRequirements(vdb *VCoordinationDatabase, hostsToRemove []string) error {
	if !vdb.doNodesExist(hostsToRemove) {
		return errors.New("some of the nodes to remove do not exist in the database")
	}
	if !vdb.IsEon {
		if vdb.hasAtLeastOneDownNode() {
			return errors.New("all nodes must be up or standby")
		}
	}

	return nil
}

// completeVDBSetting sets some VCoordinationDatabase fields we cannot get yet
// from the https endpoints. We set those fields from options.
func completeVDBSetting(vdb *VCoordinationDatabase, options *VRemoveNodeOptions) error {
	vdb.DataPrefix = *options.DataPrefix

	if *options.DepotPrefix == "" {
		return nil
	}
	if *options.HonorUserInput && vdb.IsEon {
		// checking this here because now we have got eon value from
		// the running db. This will be removed once we are able to get
		// the depot path from db through an https endpoint(VER-88122).
		err := util.ValidateRequiredAbsPath(options.DepotPrefix, "depot path")
		if err != nil {
			return err
		}
	}
	vdb.DepotPrefix = *options.DepotPrefix
	hostNodeMap := make(map[string]VCoordinationNode)
	// we set the depot path manually because there is not yet an https endpoint for
	// that(VER-88122). This is useful for NMADeleteDirectoriesOp.
	for h := range vdb.HostNodeMap {
		vnode := vdb.HostNodeMap[h]
		depotSuffix := fmt.Sprintf("%s_depot", vnode.Name)
		vnode.DepotPath = filepath.Join(vdb.DepotPrefix, vdb.Name, depotSuffix)
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
//   - Remove nodes from Spread
//   - Drop Nodes
//   - Delete catalog and data directories
//   - Reload spread
//   - Sync catalog (eon only)
func produceRemoveNodeInstructions(vdb *VCoordinationDatabase, options *VRemoveNodeOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

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

	err := produceMarkEphemeralNodeOps(&instructions, options.HostsToRemove, initiatorHost,
		usePassword, username, password, vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}

	// this is a copy of the original that only
	// contains the hosts to remove.
	v := vdb.Copy(options.HostsToRemove)
	if vdb.IsEon {
		// We pass the set of subclusters of the nodes to remove.
		err = produceRebalanceSubclusterShardsOps(&instructions, initiatorHost, v.getSCNames(),
			usePassword, username, password)
		if err != nil {
			return instructions, err
		}
	} else {
		var httpsRebalanceClusterOp HTTPSRebalanceClusterOp
		httpsRebalanceClusterOp, err = makeHTTPSRebalanceClusterOp(initiatorHost, usePassword, username,
			password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsRebalanceClusterOp)
	}

	httpsSpreadRemoveNodeOp, err := makeHTTPSSpreadRemoveNodeOp(options.HostsToRemove, initiatorHost, usePassword,
		username, password, vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &httpsSpreadRemoveNodeOp)

	err = produceDropNodeOps(&instructions, options.HostsToRemove, initiatorHost,
		usePassword, username, password, vdb.HostNodeMap, vdb.IsEon)
	if err != nil {
		return instructions, err
	}

	nmaDeleteDirectoriesOp, err := makeNMADeleteDirectoriesOp(&v, *options.ForceDelete)
	if err != nil {
		return instructions, err
	}
	httpsReloadSpreadOp, err := makeHTTPSReloadSpreadOp(initiatorHost, true, username, password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaDeleteDirectoriesOp,
		&httpsReloadSpreadOp)

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(initiatorHost, true, username, password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}

// produceRebalanceSubclusterShardsOps gets a slice of subclusters and for each of them
// produces an HTTPSRebalanceSubclusterShardsOp.
func produceRebalanceSubclusterShardsOps(instructions *[]ClusterOp, initiatorHost, scNames []string,
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

// produceMarkEphemeralNodeOps gets a slice of target hosts and for each of them
// produces an HTTPSMarkEphemeralNodeOp.
func produceMarkEphemeralNodeOps(instructions *[]ClusterOp, targetHosts, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	hostNodeMap map[string]VCoordinationNode) error {
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

// produceDropNodeOps produces an HTTPSDropNodeOp for each node to drop.
// This is because we must drop node one by one to avoid losing quorum.
func produceDropNodeOps(instructions *[]ClusterOp, targetHosts, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	hostNodeMap map[string]VCoordinationNode, isEon bool) error {
	for _, host := range targetHosts {
		httpsDropNodeOp, err := makeHTTPSDropNodeOp(hostNodeMap[host].Name, hosts,
			useHTTPPassword, userName, httpsPassword, isEon)
		if err != nil {
			return err
		}
		*instructions = append(*instructions, &httpsDropNodeOp)
	}

	return nil
}

// setInitiator sets the initiator as the first primary up node that is not
// in the list of hosts to remove.
func (o *VRemoveNodeOptions) setInitiator(primaryUpNodes []string) error {
	initiatorHost := getInitiatorHost(primaryUpNodes, o.HostsToRemove)
	if initiatorHost == "" {
		return fmt.Errorf("could not find any primary up nodes that is not to be removed")
	}
	o.Initiator = initiatorHost
	return nil
}
