package commands

import (
	"flag"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdDropDB
 *
 * Implements ClusterCommand interface
 */
type CmdDropDB struct {
	dropDBOptions *vclusterops.VDropDatabaseOptions

	CmdBase
}

func makeCmdDropDB() *CmdDropDB {
	newCmd := &CmdDropDB{}
	newCmd.oldParser = flag.NewFlagSet("create_db", flag.ExitOnError)

	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", "Comma-separated list of hosts to participate in database")

	dropDBOptions := vclusterops.VDropDatabaseOptionsFactory()
	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, "Drop database with IPv6 hosts")
	dropDBOptions.ForceDelete = newCmd.oldParser.Bool("force-delete", false, "Whether force delete directories if they are not empty")
	newCmd.oldParser.StringVar(&dropDBOptions.ConfigPath, "config", "", util.GetOptionalFlagMsg("Path to the config file"))

	dropDBOptions.HonorUserInput = newCmd.oldParser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))

	// TODO: the following options will be processed later
	dropDBOptions.DBName = newCmd.oldParser.String("db-name", "", "The name of the database to be dropped")
	dropDBOptions.CatalogPrefix = newCmd.oldParser.String("catalog-path", "", "The catalog path of the database")
	dropDBOptions.DataPrefix = newCmd.oldParser.String("data-path", "", "The data path of the database")
	dropDBOptions.DepotPrefix = newCmd.oldParser.String("depot-path", "", "The depot path of the database")

	newCmd.dropDBOptions = &dropDBOptions

	return newCmd
}

func (c *CmdDropDB) CommandType() string {
	return "drop_db"
}

func (c *CmdDropDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.oldParser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse(logger)
}

func (c *CmdDropDB) validateParse(logger vlog.Printer) error {
	if !util.IsOptionSet(c.oldParser, "password") {
		c.dropDBOptions.Password = nil
	}
	logger.Info("Called validateParse()")
	return c.OldValidateParseBaseOptions(&c.dropDBOptions.DatabaseOptions)
}

func (c *CmdDropDB) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdDropDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	err := vcc.VDropDatabase(c.dropDBOptions)
	if err != nil {
		vcc.Log.Error(err, "failed do drop the database")
		return err
	}

	vcc.Log.PrintInfo("Successfully dropped database %s", *c.dropDBOptions.DBName)
	return nil
}
