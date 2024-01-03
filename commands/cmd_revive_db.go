package commands

import (
	"flag"
	"strconv"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdReviveDB
 *
 * Implements ClusterCommand interface
 */
type CmdReviveDB struct {
	CmdBase
	reviveDBOptions     *vclusterops.VReviveDatabaseOptions
	configurationParams *string // raw input from user, need further processing
}

func makeCmdReviveDB() *CmdReviveDB {
	// CmdReviveDB
	newCmd := &CmdReviveDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("revive_db", flag.ExitOnError)
	reviveDBOptions := vclusterops.VReviveDBOptionsFactory()

	// require flags
	reviveDBOptions.DBName = newCmd.parser.String("db-name", "", "The name of the database to revive")
	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated hosts that participate in the database")
	reviveDBOptions.CommunalStorageLocation = newCmd.parser.String("communal-storage-location", "",
		util.GetEonFlagMsg("Location of communal storage"))

	// optional flags
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Revive database with IPv6 hosts"))
	newCmd.configurationParams = newCmd.parser.String("config-param", "", util.GetOptionalFlagMsg(
		"Comma-separated list of NAME=VALUE pairs for configuration parameters"))
	reviveDBOptions.ForceRemoval = newCmd.parser.Bool("force-removal", false,
		util.GetOptionalFlagMsg("Force removal of existing database directories(exclude user storage directories) before reviving the database"))
	reviveDBOptions.LoadCatalogTimeout = newCmd.parser.Uint("load-catalog-timeout", util.DefaultLoadCatalogTimeoutSeconds,
		util.GetOptionalFlagMsg("Set a timeout (in seconds) for loading remote catalog operation, default timeout is "+
			strconv.Itoa(util.DefaultLoadCatalogTimeoutSeconds)+"seconds"))
	reviveDBOptions.DisplayOnly = newCmd.parser.Bool("display-only", false,
		util.GetOptionalFlagMsg("Describe the database on communal storage, and exit"))
	reviveDBOptions.IgnoreClusterLease = newCmd.parser.Bool("ignore-cluster-lease", false,
		util.GetOptionalFlagMsg("Ignore the check of other clusters running on the same communal storage."+
			" The communal storage can be corrupted when two clusters modified it at the same time. Proceed with caution"))
	reviveDBOptions.RestorePoint.Archive = newCmd.parser.String("restore-point-archive", "", util.GetOptionalFlagMsg(
		"Name of the restore archive to use for bootstrapping"))
	reviveDBOptions.RestorePoint.Index = newCmd.parser.Int("restore-point-index", 0, util.GetOptionalFlagMsg(
		"The (1-based) index of the restore point in the restore archive to restore from"))
	reviveDBOptions.RestorePoint.ID = newCmd.parser.String("restore-point-id", "", util.GetOptionalFlagMsg(
		"The identifier of the restore point in the restore archive to restore from"))

	newCmd.reviveDBOptions = &reviveDBOptions

	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "revive_db")
	}
	return newCmd
}

func (c *CmdReviveDB) CommandType() string {
	return "revive_db"
}

func (c *CmdReviveDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseMaskedArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse(logger)
}

func (c *CmdReviveDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	// check the format of configuration params string, and parse it into configParams
	configurationParams, err := util.ParseConfigParams(*c.configurationParams)
	if err != nil {
		return err
	}
	if configurationParams != nil {
		c.reviveDBOptions.ConfigurationParameters = configurationParams
	}

	// when --display-only is provided, we do not need to parse some base options like hostListStr
	if *c.reviveDBOptions.DisplayOnly {
		return nil
	}

	// will remove this after we refined config file read
	*c.reviveDBOptions.HonorUserInput = true

	return c.ValidateParseBaseOptions(&c.reviveDBOptions.DatabaseOptions)
}

func (c *CmdReviveDB) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdReviveDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")
	dbInfo, err := vcc.VReviveDatabase(c.reviveDBOptions)
	if err != nil {
		vcc.Log.Error(err, "fail to revive database", "DBName", *c.reviveDBOptions.DBName)
		return err
	}

	if *c.reviveDBOptions.DisplayOnly {
		vcc.Log.PrintInfo("database details:\n%s", dbInfo)
		return nil
	}

	vcc.Log.PrintInfo("Successfully revived database %s", *c.reviveDBOptions.DBName)

	return nil
}
