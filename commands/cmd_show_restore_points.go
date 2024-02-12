package commands

import (
	"flag"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdShowRestorePoints
 *
 * Implements ClusterCommand interface
 */
type CmdShowRestorePoints struct {
	CmdBase
	showRestorePointsOptions *vclusterops.VShowRestorePointsOptions
	configurationParams      *string // raw input from user, need further processing

}

func makeCmdShowRestorePoints() *CmdShowRestorePoints {
	// CmdShowRestorePoints
	newCmd := &CmdShowRestorePoints{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("show_restore_points", flag.ExitOnError)
	showRestorePointsOptions := vclusterops.VShowRestorePointsFactory()

	// require flags
	showRestorePointsOptions.DBName = newCmd.parser.String("db-name", "", "The name of the database to show restore points")
	showRestorePointsOptions.CommunalStorageLocation = newCmd.parser.String("communal-storage-location", "",
		util.GetEonFlagMsg("Location of communal storage"))

	// optional flags
	newCmd.configurationParams = newCmd.parser.String("config-param", "", util.GetOptionalFlagMsg(
		"Comma-separated list of NAME=VALUE pairs for configuration parameters"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts to participate in database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "Whether the database hosts use IPv6 addresses")

	showRestorePointsOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	showRestorePointsOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	showRestorePointsOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	showRestorePointsOptions.FilterOptions.ArchiveName = newCmd.parser.String("restore-point-archive", "",
		util.GetOptionalFlagMsg("Archive name to filter restore points with"))
	showRestorePointsOptions.FilterOptions.ArchiveID = newCmd.parser.String("restore-point-id", "",
		util.GetOptionalFlagMsg("ID to filter restore points with"))
	showRestorePointsOptions.FilterOptions.ArchiveIndex = newCmd.parser.String("restore-point-index", "",
		util.GetOptionalFlagMsg("Index to filter restore points with"))
	showRestorePointsOptions.FilterOptions.StartTimestamp = newCmd.parser.String("start-timestamp", "",
		util.GetOptionalFlagMsg("Only show restores points created no earlier than this"))
	showRestorePointsOptions.FilterOptions.EndTimestamp = newCmd.parser.String("end-timestamp", "",
		util.GetOptionalFlagMsg("Only show restores points created no later than this"))

	newCmd.showRestorePointsOptions = &showRestorePointsOptions

	return newCmd
}

func (c *CmdShowRestorePoints) CommandType() string {
	return "show_restore_points"
}

func (c *CmdShowRestorePoints) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseMaskedArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	if !util.IsOptionSet(c.parser, "config-directory") {
		c.showRestorePointsOptions.ConfigDirectory = nil
	}

	return c.validateParse(logger)
}

func (c *CmdShowRestorePoints) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	// check the format of configuration params string, and parse it into configParams
	configurationParams, err := util.ParseConfigParams(*c.configurationParams)
	if err != nil {
		return err
	}
	if configurationParams != nil {
		c.showRestorePointsOptions.ConfigurationParameters = configurationParams
	}

	return c.ValidateParseBaseOptions(&c.showRestorePointsOptions.DatabaseOptions)
}

func (c *CmdShowRestorePoints) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdShowRestorePoints) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	options := c.showRestorePointsOptions
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	restorePoints, err := vcc.VShowRestorePoints(options)
	if err != nil {
		vcc.Log.Error(err, "fail to show restore points", "DBName", *options.DBName)
		return err
	}

	vcc.Log.PrintInfo("Successfully show restore points %v in database %s", restorePoints, *options.DBName)
	return nil
}
