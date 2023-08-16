package commands

import (
	"flag"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStartDB
 *
 * Implements ClusterCommand interface
 */
type CmdStartDB struct {
	CmdBase
	startDBOptions *vclusterops.VStartDatabaseOptions

	Force               *bool // force cleanup to start the database
	AllowFallbackKeygen *bool // Generate spread encryption key from Vertica. Use under support guidance only
	IgnoreClusterLease  *bool // ignore the cluster lease in communal storage
	Unsafe              *bool // Start database unsafely, skipping recovery.
	Fast                *bool // Attempt fast startup database
	Timeout             *int  // Timeout for starting the database
}

func makeCmdStartDB() *CmdStartDB {
	// CmdStartDB
	newCmd := &CmdStartDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("start_db", flag.ExitOnError)
	startDBOptions := vclusterops.VStartDatabaseOptionsFactory()

	// require flags
	startDBOptions.Name = newCmd.parser.String("name", "", util.GetOptionalFlagMsg("The name of the database to be started."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))

	// optional flags
	startDBOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	startDBOptions.CatalogPrefix = newCmd.parser.String("catalog-path", "", "The catalog path of the database")
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts to participate in database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "start database with with IPv6 hosts")

	startDBOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	startDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	// eon flags
	newCmd.isEon = newCmd.parser.Bool("eon-mode", false, util.GetEonFlagMsg("Indicate if the database is an Eon database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))

	// hidden options
	// TODO: the following options will be processed later
	newCmd.Unsafe = newCmd.parser.Bool("unsafe", false, util.SuppressHelp)
	newCmd.Force = newCmd.parser.Bool("force", false, util.SuppressHelp)
	newCmd.AllowFallbackKeygen = newCmd.parser.Bool("allow_fallback_keygen", false, util.SuppressHelp)
	newCmd.IgnoreClusterLease = newCmd.parser.Bool("ignore_cluster_lease", false, util.SuppressHelp)
	newCmd.Fast = newCmd.parser.Bool("fast", false, util.SuppressHelp)
	newCmd.Timeout = newCmd.parser.Int("timeout", util.DefaultDrainSeconds, util.SuppressHelp)

	newCmd.startDBOptions = &startDBOptions
	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "start_db")
	}
	return newCmd
}

func (c *CmdStartDB) CommandType() string {
	return "start_db"
}

func (c *CmdStartDB) Parse(inputArgv []string) error {
	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.CmdBase.isEon = nil
	}

	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	if util.IsOptionSet(c.parser, "password") {
		c.startDBOptions.UsePassword = true
	}

	if !util.IsOptionSet(c.parser, "config-directory") {
		c.startDBOptions.ConfigDirectory = nil
	}

	return c.validateParse()
}

func (c *CmdStartDB) validateParse() error {
	vlog.LogInfo("[%s] Called validateParse()", c.CommandType())
	return c.ValidateParseBaseOptions(&c.startDBOptions.DatabaseOptions)
}

func (c *CmdStartDB) Analyze() error {
	// Analyze() is needed to fulfill an interface
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdStartDB) Run(log logr.Logger) error {
	vcc := vclusterops.VClusterCommands{
		Log: log.WithName(c.CommandType()),
	}
	vcc.Log.V(1).Info("Called method Run()")
	err := vcc.VStartDatabase(c.startDBOptions)
	if err != nil {
		vcc.Log.Error(err, "failed to start the database")
		return err
	}

	vlog.LogPrintInfo("Successfully start the database %s\n", *c.startDBOptions.Name)
	return nil
}
