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
	"fmt"
	"strings"

	"github.com/vertica/vcluster/rfc7807"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VRemoveScOptions are the option arguments for the VRemoveSubcluster API
type VRemoveScOptions struct {
	DatabaseOptions
	// Subcluster to remove from database
	SubclusterToRemove *string
	ForceDelete        *bool
}

func VRemoveScOptionsFactory() VRemoveScOptions {
	opt := VRemoveScOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (o *VRemoveScOptions) setDefaultValues() {
	o.DatabaseOptions.SetDefaultValues()
	o.SubclusterToRemove = new(string)
	o.ForceDelete = new(bool)
}

func (o *VRemoveScOptions) validateRequiredOptions(log vlog.Printer) error {
	err := o.ValidateBaseOptions("db_remove_subcluster", log)
	if err != nil {
		return err
	}

	if o.SubclusterToRemove == nil || *o.SubclusterToRemove == "" {
		return fmt.Errorf("must specify a subcluster name")
	}
	return nil
}

func (o *VRemoveScOptions) validatePathOptions() error {
	// VER-88096 will get data path and depot path from /nodes
	// so the validation below may be removed
	// data prefix
	err := util.ValidateRequiredAbsPath(o.DataPrefix, "data path")
	if err != nil {
		return err
	}

	// depot path
	return util.ValidateRequiredAbsPath(o.DepotPrefix, "depot path")
}

func (o *VRemoveScOptions) validateParseOptions(log vlog.Printer) error {
	err := o.validateRequiredOptions(log)
	if err != nil {
		return err
	}

	return o.validatePathOptions()
}

func (o *VRemoveScOptions) analyzeOptions() (err error) {
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

func (o *VRemoveScOptions) validateAnalyzeOptions(log vlog.Printer) error {
	if err := o.validateParseOptions(log); err != nil {
		return err
	}
	err := o.analyzeOptions()
	if err != nil {
		return err
	}
	return o.SetUsePassword(log)
}

/*
VRemoveSubcluster has three major phases:
 1. pre-check (check the subcluster name and get nodes for the subcluster)
 2. run VRemoveNode (refer to the instructions in VRemoveNode; Optional: if there are any nodes still associated with the subcluster)
 3. run drop subcluster (i.e., remove the subcluster name from catalog)
*/
func (vcc *VClusterCommands) VRemoveSubcluster(removeScOpt *VRemoveScOptions) (VCoordinationDatabase, error) {
	vdb := MakeVCoordinationDatabase()

	// VER-88594: read config file (may move this part to cmd_remove_subcluster)

	// validate and analyze options
	err := removeScOpt.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return vdb, err
	}

	// pre-check: should not remove the default subcluster
	vcc.Log.PrintInfo("Performing db_remove_subcluster pre-checks")
	hostsToRemove, err := vcc.removeScPreCheck(&vdb, removeScOpt)
	if err != nil {
		return vdb, err
	}

	// proceed to run db_remove_node only if
	// the number of nodes to remove is greater than zero
	var needRemoveNodes bool
	vcc.Log.V(1).Info("Nodes to be removed: %+v", hostsToRemove)
	if len(hostsToRemove) == 0 {
		vcc.Log.PrintInfo("no node found in subcluster %s",
			*removeScOpt.SubclusterToRemove)
		needRemoveNodes = false
	} else {
		needRemoveNodes = true
	}

	if needRemoveNodes {
		// Remove nodes from the target subcluster
		removeNodeOpt := VRemoveNodeOptionsFactory()
		removeNodeOpt.DatabaseOptions = removeScOpt.DatabaseOptions
		removeNodeOpt.HostsToRemove = hostsToRemove
		removeNodeOpt.ForceDelete = removeScOpt.ForceDelete

		vcc.Log.PrintInfo("Removing nodes %q from subcluster %s",
			hostsToRemove, *removeScOpt.SubclusterToRemove)
		vdb, err = vcc.VRemoveNode(&removeNodeOpt)
		if err != nil {
			return vdb, err
		}
	}

	// drop subcluster (i.e., remove the sc name from catalog)
	vcc.Log.PrintInfo("Removing the subcluster name from catalog")
	err = vcc.dropSubcluster(&vdb, removeScOpt)
	if err != nil {
		return vdb, err
	}

	return vdb, nil
}

type RemoveDefaultSubclusterError struct {
	Name string
}

func (e *RemoveDefaultSubclusterError) Error() string {
	return fmt.Sprintf("cannot remove the default subcluster '%s'", e.Name)
}

// removeScPreCheck will build a list of instructions to perform
// db_remove_subcluster pre-checks
//
// The generated instructions will later perform the following operations necessary
// for a successful remove_node:
//   - Get cluster and nodes info (check if the target DB is Eon and get to-be-removed node list)
//   - Get the subcluster info (check if the target sc exists and if it is the default sc)
func (vcc *VClusterCommands) removeScPreCheck(vdb *VCoordinationDatabase, options *VRemoveScOptions) ([]string, error) {
	var hostsToRemove []string
	const preCheckErrMsg = "while performing db_remove_subcluster pre-checks"

	// get cluster and nodes info
	err := vcc.getVDBFromRunningDB(vdb, &options.DatabaseOptions)
	if err != nil {
		return hostsToRemove, err
	}

	// db_remove_subcluster only works with Eon database
	if !vdb.IsEon {
		return hostsToRemove, fmt.Errorf(`cannot remove subcluster from an enterprise database '%s'`,
			*options.DBName)
	}

	// get default subcluster
	httpsFindSubclusterOp, err := makeHTTPSFindSubclusterOp(vcc.Log, options.Hosts,
		options.usePassword, *options.UserName, options.Password,
		*options.SubclusterToRemove,
		false /*do not ignore not found*/)
	if err != nil {
		return hostsToRemove, fmt.Errorf("fail to get default subcluster %s, details: %w",
			preCheckErrMsg, err)
	}

	var instructions []ClusterOp
	instructions = append(instructions,
		&httpsFindSubclusterOp,
	)

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.Run(vcc.Log)
	if err != nil {
		// VER-88585 will improve this rfc error flow
		if strings.Contains(err.Error(), "does not exist in the database") {
			vcc.Log.PrintError("fail to get subclusters' information %s, %v", preCheckErrMsg, err)
			rfcErr := rfc7807.New(rfc7807.SubclusterNotFound).WithHost(options.Hosts[0])
			return hostsToRemove, rfcErr
		}
		return hostsToRemove, err
	}

	// the default subcluster should not be removed
	if *options.SubclusterToRemove == clusterOpEngine.execContext.defaultSCName {
		return hostsToRemove, &RemoveDefaultSubclusterError{Name: *options.SubclusterToRemove}
	}

	// get nodes of the to-be-removed subcluster
	for h, vnode := range vdb.HostNodeMap {
		if vnode.Subcluster == *options.SubclusterToRemove {
			hostsToRemove = append(hostsToRemove, h)
		}
	}

	return hostsToRemove, nil
}

func (vcc *VClusterCommands) dropSubcluster(vdb *VCoordinationDatabase, options *VRemoveScOptions) error {
	dropScErrMsg := fmt.Sprintf("fail to drop subcluster %s", *options.SubclusterToRemove)

	// the initiator is a list of one primary up host
	// that will call the https /v1/subclusters/{scName}/drop endpoint
	// as the endpoint will drop a subcluster, we only need one host to do so
	initiator, err := getInitiatorHost(vdb.PrimaryUpNodes, []string{})
	if err != nil {
		return err
	}

	httpsDropScOp, err := makeHTTPSDropSubclusterOp([]string{initiator},
		*options.SubclusterToRemove,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		vcc.Log.Error(err, "details: %v", dropScErrMsg)
		return err
	}

	var instructions []ClusterOp
	instructions = append(instructions, &httpsDropScOp)

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.Run(vcc.Log)
	if err != nil {
		vcc.Log.Error(err, "fail to drop subcluster, details: %v", dropScErrMsg)
		return err
	}

	return nil
}
