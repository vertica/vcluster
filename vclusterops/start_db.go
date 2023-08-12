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

func (options *VStartDatabaseOptions) validateParseOptions() error {
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

func (options *VStartDatabaseOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	err := options.analyzeOptions()
	return err
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

	err = options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	// build startDBInfo from config file and options
	startDBInfo := new(VStartDatabaseInfo)
	startDBInfo.DBName, startDBInfo.Hosts = options.GetNameAndHosts(config)
	startDBInfo.HostCatalogPath = make(map[string]string)
	startDBInfo.CatalogPath = options.GetCatalogPrefix(config)

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

	nmaHealthOp := makeNMAHealthOp(startDBInfo.Hosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(startDBInfo.Hosts, true)
	// need username for https operations
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.ValidateUserName()
		if err != nil {
			return instructions, err
		}
	}

	checkDBRunningOp, err := makeHTTPCheckRunningDBOp(startDBInfo.Hosts,
		usePassword, *options.UserName, options.Password, StartDB)
	if err != nil {
		return instructions, err
	}

	vdb := VCoordinationDatabase{}
	nmaNodesOp := makeNMAGetNodesInfoOp(startDBInfo.Hosts, *options.Name, startDBInfo.CatalogPath, &vdb)

	nmaReadCatalogEditorOp, err := makeNMAReadCatalogEditorOp([]string{}, &vdb)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaNodesOp,
		&nmaReadCatalogEditorOp,
	)

	// sourceConfHost is set to nil value in upload and download step
	// we use information from catalog editor operation to update the sourceConfHost value
	// after we find host with the highest catalog and hosts that need to synchronize the catalog
	// we will remove the nil parameters in VER-88401 by adding them in execContext
	produceTransferConfigOps(&instructions,
		nil, /*source hosts for transferring configuration files*/
		startDBInfo.Hosts,
		nil, /*new hosts which will be added to the db*/
		nil /*db configurations retrieved from a running db*/)

	nmaStartNewNodesOp := makeNMAStartNodeOp(startDBInfo.Hosts)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOp(startDBInfo.Hosts,
		usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaStartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	if options.IsEon.ToBool() {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(startDBInfo.Hosts, true, *options.UserName, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}
