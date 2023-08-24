package commands

import (
	"flag"
	"strconv"

	"github.com/go-logr/logr"
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
	reviveDBOptions       *vclusterops.VReviveDatabaseOptions
	communalStorageParams *string // raw input from user, need further processing
}

func makeCmdReviveDB() *CmdReviveDB {
	// CmdReviveDB
	newCmd := &CmdReviveDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("revive_db", flag.ExitOnError)
	reviveDBOptions := vclusterops.VReviveDBOptionsFactory()

	// require flags
	reviveDBOptions.Name = newCmd.parser.String("db-name", "", "The name of the database to revive")
	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated hosts that participate in the database")
	reviveDBOptions.CommunalStorageLocation = newCmd.parser.String("communal-storage-location", "",
		util.GetEonFlagMsg("Location of communal storage"))

	// optional flags
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Revive database with IPv6 hosts"))
	newCmd.communalStorageParams = newCmd.parser.String("communal-storage-params", "", util.GetOptionalFlagMsg(
		"Comma-separated list of NAME=VALUE pairs for communal storage parameters"))
	reviveDBOptions.ForceRemoval = newCmd.parser.Bool("force-removal", false,
		util.GetOptionalFlagMsg("Force removal of existing database directories(exclude user storage directories) before reviving the database"))
	reviveDBOptions.LoadCatalogTimeout = newCmd.parser.Uint("load-catalog-timeout", util.DefaultLoadCatalogTimeoutSeconds,
		util.GetOptionalFlagMsg("Set a timeout (in seconds) for loading remote catalog operation, default timeout is "+
			strconv.Itoa(util.DefaultLoadCatalogTimeoutSeconds)+"seconds"))

	newCmd.reviveDBOptions = &reviveDBOptions

	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "revive_db")
	}
	return newCmd
}

func (c *CmdReviveDB) CommandType() string {
	return "revive_db"
}

func (c *CmdReviveDB) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse()
}

func (c *CmdReviveDB) validateParse() error {
	vlog.LogInfo("[%s] Called validateParse()", c.CommandType())

	// check the format of communal storage params string, and parse it into configParams
	communalStorageParams, err := util.ParseConfigParams(*c.communalStorageParams)
	if err != nil {
		return err
	}
	if communalStorageParams != nil {
		c.reviveDBOptions.CommunalStorageParameters = communalStorageParams
	}

	// will remove this after we refined config file read
	*c.reviveDBOptions.HonorUserInput = true

	return c.ValidateParseBaseOptions(&c.reviveDBOptions.DatabaseOptions)
}

func (c *CmdReviveDB) Analyze() error {
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdReviveDB) Run(log logr.Logger) error {
	vcc := vclusterops.VClusterCommands{
		Log: log.WithName(c.CommandType()),
	}
	vcc.Log.V(1).Info("Called method Run()")
	err := vcc.VReviveDatabase(c.reviveDBOptions)
	if err != nil {
		vcc.Log.Error(err, "fail to revive database %s", *c.reviveDBOptions.Name)
		return err
	}

	vlog.LogPrintInfo("Successfully revived database %s", *c.reviveDBOptions.Name)

	return nil
}
