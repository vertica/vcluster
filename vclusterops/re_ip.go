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

	ReIPList []ReIPInfo
}

func VReIPFactory() VReIPOptions {
	opt := VReIPOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (opt *VReIPOptions) validateParseOptions() error {
	err := util.ValidateRequiredAbsPath(opt.CatalogPrefix, "catalog path")
	if err != nil {
		return err
	}

	return opt.ValidateBaseOptions("re_ip")
}

func (opt *VReIPOptions) analyzeOptions() error {
	hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, opt.Ipv6.ToBool())
	if err != nil {
		return err
	}

	opt.Hosts = hostAddresses
	return nil
}

func (opt *VReIPOptions) ValidateAnalyzeOptions() error {
	if err := opt.validateParseOptions(); err != nil {
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

	err := options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	// produce re-ip instructions
	instructions, err := produceReIPInstructions(options)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %v", err)
		return err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		vlog.LogPrintError("fail to re-ip: %v", runError)
		return runError
	}

	return nil
}

// The generated instructions will later perform the following operations necessary
// for a successful re_ip:
//   - Check NMA connectivity
//   - Check Vertica versions
//   - Read database info from catalog editor
//     (now we should know which hosts have the latest catalog)
//   - Run re-ip on the target nodes
func produceReIPInstructions(options *VReIPOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	if len(options.ReIPList) == 0 {
		return instructions, errors.New("the re-ip information is not provided")
	}

	hosts := options.Hosts

	nmaHealthOp := makeNMAHealthOp(hosts)
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(hosts, true)

	// get network profiles of the new addresses
	var newAddresses []string
	for _, info := range options.ReIPList {
		newAddresses = append(newAddresses, info.TargetAddress)
	}
	nmaNetworkProfileOp := makeNMANetworkProfileOp(newAddresses)

	// build a VCoordinationDatabase (vdb) object by reading the nodes' information from /v1/nodes
	vdb := VCoordinationDatabase{}
	nmaNodesOp := makeNMAGetNodesInfoOp(hosts, *options.Name, *options.CatalogPrefix, &vdb)

	// read catalog editor to get hosts with latest catalog
	nmaReadCatEdOp, err := makeNMAReadCatalogEditorOp([]string{}, &vdb)
	if err != nil {
		return instructions, err
	}

	// re-ip
	// at this stage the re-ip info should either by provided by
	// the re-ip file (for vcluster CLI) or the Kubernetes operator
	nmaReIPOP := makeNMAReIPOp(options.ReIPList, &vdb)

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&nmaNetworkProfileOp,
		&nmaNodesOp,
		&nmaReadCatEdOp,
		&nmaReIPOP,
	)

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
