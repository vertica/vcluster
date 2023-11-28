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
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VReIPOptions struct {
	DatabaseOptions

	// re-ip list
	ReIPList []ReIPInfo

	/* hidden option */

	// whether trim re-ip list based on the catalog info
	TrimReIPList bool
}

func VReIPFactory() VReIPOptions {
	opt := VReIPOptions{}
	// set default values to the params
	opt.setDefaultValues()
	opt.TrimReIPList = false

	return opt
}

func (opt *VReIPOptions) validateParseOptions(logger vlog.Printer) error {
	err := util.ValidateRequiredAbsPath(opt.CatalogPrefix, "catalog path")
	if err != nil {
		return err
	}

	if *opt.CommunalStorageLocation != "" {
		return util.ValidateCommunalStorageLocation(*opt.CommunalStorageLocation)
	}

	return opt.validateBaseOptions("re_ip", logger)
}

func (opt *VReIPOptions) analyzeOptions() error {
	hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, opt.Ipv6.ToBool())
	if err != nil {
		return err
	}

	opt.Hosts = hostAddresses
	return nil
}

func (opt *VReIPOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := opt.validateParseOptions(logger); err != nil {
		return err
	}
	if err := opt.analyzeOptions(); err != nil {
		return err
	}

	// the re-ip list must not be empty
	if len(opt.ReIPList) == 0 {
		return errors.New("the re-ip list is not provided")
	}

	// address check
	ipv6 := opt.Ipv6.ToBool()
	nodeAddresses := make(map[string]struct{})
	for _, info := range opt.ReIPList {
		// the addresses must be valid IPs
		if err := util.AddressCheck(info.TargetAddress, ipv6); err != nil {
			return err
		}
		if info.TargetControlAddress != "" {
			if err := util.AddressCheck(info.TargetControlAddress, ipv6); err != nil {
				return err
			}
		}
		if info.TargetControlBroadcast != "" {
			if err := util.AddressCheck(info.TargetControlBroadcast, ipv6); err != nil {
				return err
			}
		}

		// the target node addresses in the re-ip list must not be empty or duplicate
		addr := info.TargetAddress
		if addr == "" {
			return errors.New("the new node address should not be empty")
		}
		if _, ok := nodeAddresses[addr]; ok {
			return fmt.Errorf("the provided node address %s is duplicate", addr)
		}
		nodeAddresses[addr] = struct{}{}
	}
	return nil
}

// VReIP changes nodes addresses (node address, control address, and control broadcast)
func (vcc *VClusterCommands) VReIP(options *VReIPOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return err
	}

	var pVDB *VCoordinationDatabase
	// retrieve database information from cluster_config.json for EON databases
	if options.IsEon.ToBool() {
		if *options.CommunalStorageLocation != "" {
			vdb, e := options.getVDBWhenDBIsDown(vcc)
			if e != nil {
				return e
			}
			pVDB = &vdb
		} else {
			// When communal storage location is missing, we only log a debug message
			// because re-ip only fails in between revive_db and first start_db.
			// We should not ran re-ip in that case because revive_db has already done the re-ip work.
			vcc.Log.V(1).Info("communal storage location is not specified for an eon database," +
				" re_ip after revive_db could fail because we cannot retrieve the correct database information")
		}
	}

	// produce re-ip instructions
	instructions, err := vcc.produceReIPInstructions(options, pVDB)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to re-ip: %w", runError)
	}

	return nil
}

// The generated instructions will later perform the following operations necessary
// for a successful re_ip:
//   - Check NMA connectivity
//   - Read database info from catalog editor
//     (now we should know which hosts have the latest catalog)
//   - Run re-ip on the target nodes
func (vcc *VClusterCommands) produceReIPInstructions(options *VReIPOptions, vdb *VCoordinationDatabase) ([]clusterOp, error) {
	var instructions []clusterOp

	if len(options.ReIPList) == 0 {
		return instructions, errors.New("the re-ip information is not provided")
	}

	hosts := options.Hosts

	nmaHealthOp := makeNMAHealthOp(vcc.Log, hosts)

	// get network profiles of the new addresses
	var newAddresses []string
	for _, info := range options.ReIPList {
		newAddresses = append(newAddresses, info.TargetAddress)
	}
	nmaNetworkProfileOp := makeNMANetworkProfileOp(vcc.Log, newAddresses)

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaNetworkProfileOp,
	)

	vdbWithPrimaryNodes := new(VCoordinationDatabase)
	// When we cannot get db info from cluster_config.json, we will fetch it from NMA /nodes endpoint.
	if vdb == nil {
		vdb = new(VCoordinationDatabase)
		nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(vcc.Log, options.Hosts, *options.DBName, *options.CatalogPrefix,
			false /* report all errors */, vdb)
		// read catalog editor to get hosts with latest catalog
		nmaReadCatEdOp, err := makeNMAReadCatalogEditorOp(vcc.Log, vdb)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions,
			&nmaGetNodesInfoOp,
			&nmaReadCatEdOp,
		)
	} else {
		// use a copy of vdb because we want to keep secondary nodes in vdb for next nmaReIPOP
		*vdbWithPrimaryNodes = *vdb
		vdbWithPrimaryNodes.filterPrimaryNodes()
		// read catalog editor to get hosts with latest catalog
		nmaReadCatEdOp, err := makeNMAReadCatalogEditorOp(vcc.Log, vdbWithPrimaryNodes)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &nmaReadCatEdOp)
	}

	// re-ip
	// at this stage the re-ip info should either by provided by
	// the re-ip file (for vcluster CLI) or the Kubernetes operator
	nmaReIPOP := makeNMAReIPOp(vcc.Log, options.ReIPList, vdb, options.TrimReIPList)

	instructions = append(instructions, &nmaReIPOP)

	return instructions, nil
}

type reIPRow struct {
	CurrentAddress      string `json:"from_address"`
	NewAddress          string `json:"to_address"`
	NewControlAddress   string `json:"to_control_address,omitempty"`
	NewControlBroadcast string `json:"to_control_broadcast,omitempty"`
}

// ReadReIPFile reads the re-ip file and build a list of ReIPInfo
func (opt *VReIPOptions) ReadReIPFile(path string) error {
	if err := util.AbsPathCheck(path); err != nil {
		return fmt.Errorf("must specify an absolute path for the re-ip file")
	}

	var reIPRows []reIPRow
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("fail to read the re-ip file %s, details: %w", path, err)
	}
	err = json.Unmarshal(fileBytes, &reIPRows)
	if err != nil {
		return fmt.Errorf("fail to unmarshal the re-ip file, details: %w", err)
	}

	addressCheck := func(address string, ipv6 bool) error {
		checkPassed := false
		if ipv6 {
			checkPassed = util.IsIPv6(address)
		} else {
			checkPassed = util.IsIPv4(address)
		}

		if !checkPassed {
			ipVersion := "IPv4"
			if ipv6 {
				ipVersion = "IPv6"
			}
			return fmt.Errorf("%s in the re-ip file is not a valid %s address", address, ipVersion)
		}

		return nil
	}

	ipv6 := opt.Ipv6.ToBool()
	for _, row := range reIPRows {
		var info ReIPInfo
		info.NodeAddress = row.CurrentAddress
		if e := addressCheck(row.CurrentAddress, ipv6); e != nil {
			return e
		}

		info.TargetAddress = row.NewAddress
		info.TargetControlAddress = row.NewControlAddress
		info.TargetControlBroadcast = row.NewControlBroadcast

		opt.ReIPList = append(opt.ReIPList, info)
	}

	return nil
}
