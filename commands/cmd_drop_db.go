package commands

import (
	"flag"
	"fmt"
	"os"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdDropDB
 *
 * Implements ClusterCommand interface
 */
type CmdDropDB struct {
	argv          []string
	parser        *flag.FlagSet
	dropDBOptions *vclusterops.VDropDatabaseOptions

	hostListStr *string // raw string from user input, need further processing
}

func MakeCmdDropDB() CmdDropDB {
	newCmd := CmdDropDB{}
	newCmd.parser = flag.NewFlagSet("create_db", flag.ExitOnError)

	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated list of hosts to participate in database")

	dropDBOptions := vclusterops.VDropDatabaseOptions{}
	dropDBOptions.Ipv6 = newCmd.parser.Bool("ipv6", false, "Drop database with IPv6 hosts")
	dropDBOptions.ForceDelete = newCmd.parser.Bool("force-delete", false, "Whether force delete directories if they are not empty")
	dropDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "", "Directory where "+vclusterops.ConfigFileName+" is located")

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
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv

	// TODO: if len(c.argv) == 0 { c.PrintUsage() ... }
	// when using options and no args provided, print the usage

	parserError := c.parser.Parse(c.argv)
	if parserError != nil {
		return parserError
	}

	// use password if user provides it
	if util.IsOptionSet(c.parser, "password") {
		c.dropDBOptions.UsePassword = true
	}

	// handle options that are not passed in
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.dropDBOptions.ConfigDirectory = nil
	}

	return c.validateParse()
}

func (c *CmdDropDB) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// TODO: if HonorUserInput set to be true, process options here

	return nil
}

func (c *CmdDropDB) Analyze() error {
	return nil
}

func (c *CmdDropDB) Run() error {
	vlog.LogInfoln("Called method Run()")

	err := vclusterops.VDropDatabase(c.dropDBOptions)
	if err != nil {
		return err
	}

	vlog.LogPrintInfo("Successfully dropped database %s\n", *c.dropDBOptions.Name)
	return nil
}

func (c *CmdDropDB) PrintUsage() {
	thisCommand := c.CommandType()
	fmt.Fprintf(os.Stderr,
		"Please refer the usage of \"vcluster %s\" using \"vcluster %s --help\"\n",
		thisCommand, thisCommand)
}
