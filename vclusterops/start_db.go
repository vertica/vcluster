package vclusterops

import (
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
)

// Normal strings are easier and safer to use in Go.
type VStartDatabaseOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: hidden info
	UsePassword bool
}

type VStartDatabaseInfo struct {
	DBName          string
	Hosts           []string
	UserName        string
	Password        *string
	CatalogPath     string
	HostCatalogPath map[string]string
}

func VStartDatabaseOptionsFactory() VStartDatabaseOptions {
	opt := VStartDatabaseOptions{}

	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (options *VStartDatabaseOptions) SetDefaultValues() {
	options.DatabaseOptions.SetDefaultValues()
}

func (options *VStartDatabaseOptions) validateRequiredOptions() error {
	err := options.ValidateBaseOptions("start_db")
	if err != nil {
		return err
	}

	if *options.HonorUserInput {
		err = options.ValidateCatalogPath()
		if err != nil {
			return err
		}
	}

	return nil
}

func (options *VStartDatabaseOptions) validateParseOptions(config *ClusterConfig) error {
	return options.validateRequiredOptions()
}

func (options *VStartDatabaseOptions) analyzeOptions() (err error) {
	// we analyze hostnames when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
	}
	return nil
}

func (options *VStartDatabaseOptions) ValidateAnalyzeOptions(config *ClusterConfig) error {
	if err := options.validateParseOptions(config); err != nil {
		return err
	}
	if err := options.analyzeOptions(); err != nil {
		return err
	}
	return nil
}

func (vcc *VClusterCommands) VStartDatabase(options *VStartDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// load vdb info from the YAML config file
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig()
	if err != nil {
		return err
	}

	err = options.ValidateAnalyzeOptions(config)
	if err != nil {
		return err
	}

	// build startDBInfo from config file and options
	startDBInfo := new(VStartDatabaseInfo)
	startDBInfo.DBName, startDBInfo.Hosts = options.GetNameAndHosts(config)
	startDBInfo.HostCatalogPath = make(map[string]string)
	startDBInfo.CatalogPath = options.GetCatalogPrefix(config)

	// TODO: call getCatalogPath endpoint (VER-88084)
	// we build up the catalog paths for the hosts temporarily
	// After having getCatalogPath endpoint, we will remove it
	startDBInfo.HostCatalogPath = util.GetHostCatalogPath(startDBInfo.Hosts, *options.Name, startDBInfo.CatalogPath)

	// produce start_db instructions
	instructions, err := produceStartDBInstructions(startDBInfo, options)
	if err != nil {
		err = fmt.Errorf("fail to production instructions: %w", err)
		return err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		runError = fmt.Errorf("fail to start database: %w", runError)
		return runError
	}

	return nil
}

// produceStartDBInstructions will build a list of instructions to execute for
// the start_db command.
//
// The generated instructions will later perform the following operations necessary
// for a successful start_db:
//   - Check NMA connectivity
//   - Check to see if any dbs running
//   - Check Vertica versions
//   - Use NMA /catalog/database to get the best source node for spread.conf and vertica.conf
//   - Sync the confs to the rest of nodes who have lower catalog version (results from the previous step)
//   - Start all nodes of the database
//   - Poll node startup
//   - Sync catalog (Eon mode only)
func produceStartDBInstructions(startDBInfo *VStartDatabaseInfo, options *VStartDatabaseOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	nmaHealthOp := MakeNMAHealthOp(startDBInfo.Hosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp(startDBInfo.Hosts, true)
	// need username for https operations
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.ValidateUserName()
		if err != nil {
			return instructions, err
		}
	}

	checkDBRunningOp := MakeHTTPCheckRunningDBOp("HTTPCheckDBRunningOp", startDBInfo.Hosts,
		usePassword, *options.UserName, options.Password, StartDB)

	nmaReadCatalogEditorOp, err := MakeNMAReadCatalogEditorOp(startDBInfo.HostCatalogPath, []string{})
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaReadCatalogEditorOp,
	)

	// sourceConfHost is set to nil value in upload and download step
	// we use information from catalog editor operation to update the sourceConfHost value
	// after we find host with the highest catalog and hosts that need to synchronize the catalog
	produceTransferConfigOps(&instructions, nil, startDBInfo.Hosts, nil)

	nmaStartNewNodesOp := makeNMAStartNodeOp(startDBInfo.Hosts, nil)
	httpsPollNodeStateOp := MakeHTTPSPollNodeStateOp(startDBInfo.Hosts,
		usePassword, *options.UserName, options.Password)

	instructions = append(instructions,
		&nmaStartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	if options.IsEon.ToBool() {
		httpsSyncCatalogOp := MakeHTTPSSyncCatalogOp(startDBInfo.Hosts, true, *options.UserName, options.Password, nil)
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}
