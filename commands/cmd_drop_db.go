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

func MakeCmdDropDB() *CmdDropDB {
	newCmd := &CmdDropDB{}
	newCmd.parser = flag.NewFlagSet("create_db", flag.ExitOnError)

	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated list of hosts to participate in database")

	dropDBOptions := vclusterops.VDropDatabaseOptionsFactory()
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "Drop database with IPv6 hosts")
	dropDBOptions.ForceDelete = newCmd.parser.Bool("force-delete", false, "Whether force delete directories if they are not empty")
	dropDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "", "Directory where "+vclusterops.ConfigFileName+" is located")

	dropDBOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))

	// TODO: the following options will be processed later
	dropDBOptions.Name = newCmd.parser.String("name", "", "The name of the database to be dropped")
	dropDBOptions.CatalogPrefix = newCmd.parser.String("catalog-path", "", "The catalog path of the database")
	dropDBOptions.DataPrefix = newCmd.parser.String("data-path", "", "The data path of the database")
	dropDBOptions.DepotPrefix = newCmd.parser.String("depot-path", "", "The depot path of the database")

	newCmd.dropDBOptions = &dropDBOptions

	return newCmd
}

func (c *CmdDropDB) CommandType() string {
	return "drop_db"
}

func (c *CmdDropDB) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.dropDBOptions.Password = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.dropDBOptions.ConfigDirectory = nil
	}
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse()
}

func (c *CmdDropDB) validateParse() error {
	vlog.LogInfo("[%s] Called validateParse()", c.CommandType())
	return c.ValidateParseBaseOptions(&c.dropDBOptions.DatabaseOptions)
}

func (c *CmdDropDB) Analyze() error {
	return nil
}

func (c *CmdDropDB) Run() error {
	vlog.LogInfo("[%s] Called method Run()", c.CommandType())

	err := vclusterops.VDropDatabase(c.dropDBOptions)
	if err != nil {
		return err
	}

	vlog.LogPrintInfo("Successfully dropped database %s\n", *c.dropDBOptions.Name)
	return nil
}
