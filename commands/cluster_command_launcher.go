/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package commands

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	vclusterConfigEnv = "VCLUSTER_CONFIG"
	// If no config file was provided, we will pick a default one. This is the
	// default file name that we'll use.
	defConfigFileName = "vertica_cluster.yaml"
)

/* ClusterCommandLauncher
 * A class that encapsulates the data and steps
 * of a CLI program for run cluster operations. Similar
 * to commandLineCtrl.py from Admintools.
 *
 * Main programs instaniate ClusterCommandLauncher with
 * makeClusterCommandLauncher. The main program calls the Run() method
 * with command line arguments. Run() returns errors.
 *
 */
type ClusterCommandLauncher struct {
	argv     []string
	commands map[string]ClusterCommand
}

/* minArgs - the minimum number of arguments
 * that the program expects.
 *
 * The command lines that we expect to parse
 * look like:
 * vcluster <subcommand> [options] [args]
 *
 * Examples:
 * vcluster help
 * vcluster create_db --db-name db1
 *
 *
 */
const minArgs = 2
const helpString = "help"
const defaultLogPath = "/opt/vertica/log/vcluster.log"

const CLIVersion = "1.2.0"

var (
	dbOptions = vclusterops.DatabaseOptionsFactory()
	logPath   string
	verbose   bool
	rootCmd   = &cobra.Command{
		Use:   "vcluster",
		Short: "Administer a Vertica cluster",
		Long: `This CLI is used to manage a Vertica cluster with a REST API. The REST API endpoints are
exposed by the following services:
- Node Management Agent (NMA)
- Embedded HTTPS service

This CLI tool combines REST calls to provide an interface so that you can
perform the following administrator operations:
- Create a database
- Scale a cluster up and down
- Restart a database
- Stop a database
- Drop a database
- Revive an Eon database
- Add/Remove a subcluster
- Sandbox/Unsandbox a subcluster
- Scrutinize a database
- View the state of a database
- Install packages on a database`,
		Version: CLIVersion,
	}
)

// cmdInterface is an interface that every vcluster command needs to implement
// for making a basic cobra command
type cmdInterface interface {
	Parse(inputArgv []string, logger vlog.Printer) error
	Run(vcc vclusterops.VClusterCommands) error
	SetDatabaseOptions(opt *vclusterops.DatabaseOptions)
	SetParser(parser *pflag.FlagSet)
	SetIPv6(cmd *cobra.Command)
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Printf("Error during execution: %s\n", err)
		os.Exit(1)
	}
}

// initVcc will initialize a vclusterops.VClusterCommands which contains a logger
func initVcc(cmd *cobra.Command) vclusterops.VClusterCommands {
	// setup logs
	logger := vlog.Printer{ForCli: true}
	logger.SetupOrDie(logPath)

	vcc := vclusterops.VClusterCommands{
		Log: logger.WithName(cmd.CalledAs()),
	}
	vcc.Log.Info("New VCluster command initialization")

	return vcc
}

// makeBasicCobraCmd can make a basic cobra command for all vcluster commands.
// It will be called inside cmd_create_db.go, cmd_stop_db.go, ...
func makeBasicCobraCmd(i cmdInterface, use, short, long string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if verbose {
				fmt.Println("---{VCluster begin}---")
				defer fmt.Println("---{VCluster end}---")
			}
			initConfig()
			vcc := initVcc(cmd)
			i.SetParser(cmd.Flags())
			i.SetDatabaseOptions(&dbOptions)
			parseError := i.Parse(os.Args[2:], vcc.Log)
			if parseError != nil {
				vcc.Log.Error(parseError, "fail to parse command")
				return parseError
			}
			runError := i.Run(vcc)
			if runError != nil {
				vcc.Log.Error(runError, "fail to run command")
			}
			return runError
		},
	}
	i.SetIPv6(cmd)

	return cmd
}

// constructCmds returns a list of commands that will be executed
// by the cluster command launcher.
func constructCmds() []*cobra.Command {
	return []*cobra.Command{
		// db-scope cmds
		makeCmdCreateDB(),
		makeCmdStopDB(),
		makeCmdDropDB(),
		makeCmdReviveDB(),
		makeCmdReIP(),
		// sc-scope cmds
		// node-scope cmds
		// others
	}
}

// hideLocalFlags can hide help and usage of local flags in a command
func hideLocalFlags(cmd *cobra.Command, flags []string) {
	for _, flag := range flags {
		err := cmd.Flags().MarkHidden(flag)
		if err != nil {
			fmt.Printf("Warning: fail to hide flag %q, details: %v\n", flag, err)
		}
	}
}

// remove this function in VER-92224
// requireHonorUserInputOrConfDir can force the users to pass in at least
// one flag from ("--honor-user-input", "--config")
func requireHonorUserInputOrConfDir(cmd *cobra.Command) {
	cmd.MarkFlagsOneRequired("honor-user-input", "config")
}

// markFlagsRequired will mark local flags as required
func markFlagsRequired(cmd *cobra.Command, flags []string) {
	for _, flag := range flags {
		err := cmd.MarkFlagRequired(flag)
		if err != nil {
			fmt.Printf("Warning: fail to mark flag %q required, details: %v\n", flag, err)
		}
	}
}

// markFlagsDirName will require some local flags to be dir name
func markFlagsDirName(cmd *cobra.Command, flags []string) {
	for _, flag := range flags {
		err := cmd.MarkFlagDirname(flag)
		if err != nil {
			fmt.Printf("Warning: fail to mark flag %q to be a dir name, details: %v\n", flag, err)
		}
	}
}

// markFlagsFileName will require some local flags to be file name
func markFlagsFileName(cmd *cobra.Command, flagsWithExts map[string][]string) {
	for flag, ext := range flagsWithExts {
		err := cmd.MarkFlagFilename(flag, ext...)
		if err != nil {
			fmt.Printf("Warning: fail to mark flag %q to be a file name, details: %v\n", flag, err)
		}
	}
}

// setCommonFlags is a help function to let subcommands set some shared flags among them
func setCommonFlags(cmd *cobra.Command, flags []string) {
	if util.StringInArray("db-name", flags) {
		cmd.Flags().StringVarP(
			dbOptions.DBName,
			"db-name",
			"d",
			"",
			"The name of the database",
		)
	}
	// remove this flag in VER-92224
	if util.StringInArray("honor-user-input", flags) {
		cmd.Flags().BoolVar(
			dbOptions.HonorUserInput,
			"honor-user-input",
			false,
			util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName),
		)
	}
	if util.StringInArray("config", flags) {
		cmd.Flags().StringVarP(
			&dbOptions.ConfigPath,
			"config",
			"c",
			"",
			util.GetOptionalFlagMsg("Path to the config file"),
		)
		markFlagsFileName(cmd, map[string][]string{"config": {"yaml"}})
	}
	if util.StringInArray("password", flags) {
		cmd.Flags().StringVarP(
			dbOptions.Password,
			"password",
			"p",
			"",
			util.GetOptionalFlagMsg("Database password in single quotes"),
		)
	}
	if util.StringInArray("hosts", flags) {
		cmd.Flags().StringSliceVar(
			&dbOptions.RawHosts,
			"hosts",
			[]string{},
			util.GetOptionalFlagMsg("Comma-separated list of hosts in database."+
				" Use it when you do not trust "+vclusterops.ConfigFileName),
		)
	}
	if util.StringInArray("catalog-path", flags) {
		cmd.Flags().StringVar(
			dbOptions.CatalogPrefix,
			"catalog-path",
			"",
			"Path of catalog directory",
		)
	}
	if util.StringInArray("data-path", flags) {
		cmd.Flags().StringVar(
			dbOptions.DataPrefix,
			"data-path",
			"",
			"Path of data directory",
		)
	}
	if util.StringInArray("communal-storage-location", flags) {
		cmd.Flags().StringVar(
			dbOptions.CommunalStorageLocation,
			"communal-storage-location",
			"",
			util.GetEonFlagMsg("Location of communal storage"),
		)
	}
	if util.StringInArray("depot-path", flags) {
		cmd.Flags().StringVar(
			dbOptions.DepotPrefix,
			"depot-path",
			"",
			util.GetEonFlagMsg("Path to depot directory"),
		)
	}

	// log-path is a flag that all the subcommands need
	cmd.Flags().StringVarP(
		&logPath,
		"log-path",
		"l",
		defaultLogPath,
		util.GetOptionalFlagMsg("Path location used for the debug logs"),
	)
	markFlagsFileName(cmd, map[string][]string{"log-path": {"log"}})

	// verbose is a flag that all the subcommands need
	cmd.Flags().BoolVar(
		&verbose,
		"verbose",
		false,
		util.GetOptionalFlagMsg("Show the details of VCluster run in the console"),
	)
}

// remove this function in VER-92222
/* ClusterCommandLauncherFactory()
 * Returns a new instance of a ClusterCommandLauncher
 * with some reasonable defaults.
 */
func MakeClusterCommandLauncher() (ClusterCommandLauncher, vclusterops.VClusterCommands) {
	// setup logs for command launcher initialization
	userCommandString := os.Args[1]
	logger := vlog.Printer{ForCli: true}
	oldLogPath := parseLogPathArg(os.Args, defaultLogPath)
	logger.SetupOrDie(oldLogPath)
	vcc := vclusterops.VClusterCommands{
		Log: logger.WithName(userCommandString),
	}
	vcc.Log.Info("New vcluster command initialization")
	newLauncher := ClusterCommandLauncher{}
	allCommands := constructOldCmds(vcc.Log)

	newLauncher.commands = map[string]ClusterCommand{}
	for _, c := range allCommands {
		_, existsInMap := newLauncher.commands[c.CommandType()]
		if existsInMap {
			// shout loud if there's a programmer error
			vcc.Log.PrintError("Programmer Error: tried to add command %s to the commands index twice. Check cluster_command_launcher.go",
				c.CommandType())
			os.Exit(1)
		}
		newLauncher.commands[c.CommandType()] = c
	}

	return newLauncher, vcc
}

// remove this function in VER-92222
// constructCmds returns a list of commands that will be executed
// by the cluster command launcher.
func constructOldCmds(_ vlog.Printer) []ClusterCommand {
	return []ClusterCommand{
		// db-scope cmds
		makeCmdStartDB(),
		makeListAllNodes(),
		makeCmdShowRestorePoints(),
		makeCmdInstallPackages(),
		// sc-scope cmds
		makeCmdAddSubcluster(),
		makeCmdRemoveSubcluster(),
		makeCmdSandboxSubcluster(),
		makeCmdUnsandboxSubcluster(),
		// node-scope cmds
		makeCmdAddNode(),
		makeCmdRemoveNode(),
		makeCmdRestartNodes(),
		// others
		makeCmdScrutinize(),
		makeCmdHelp(),
		makeCmdInit(),
		makeCmdConfig(),
	}
}

// remove this function in VER-92222
/* Run is expected be called by a CLI program
 * Run executes the following algorithm:
 *     + Identifies the appropriate sub-command
 *     + Asks the sub-command to parse the command line
 *     + Loads the configuration file using the vconfig library. (not implemented)
 *     + Asks the sub-command to analyze the command line given the
 *       context from the config file.
 *     + Calls Run() for the sub-command
 *     + Returns any errors to the caller after writing the error to the log
 */
func (c ClusterCommandLauncher) Run(inputArgv []string, vcc vclusterops.VClusterCommands) error {
	userCommandString := os.Args[1]
	c.argv = inputArgv
	minArgsError := checkMinimumInput(c.argv)

	if minArgsError != nil {
		vcc.Log.Error(minArgsError, "fail to check minimum argument")
		return minArgsError
	}

	subCommand, idError := identifySubcommand(c.commands, userCommandString, vcc.Log)

	if idError != nil {
		vcc.Log.Error(idError, "fail to recognize command")
		return idError
	}

	parseError := subCommand.Parse(inputArgv[2:], vcc.Log)
	if parseError != nil {
		vcc.Log.Error(parseError, "fail to parse command")
		return parseError
	}

	/* Special case: help
	 * If the user asked for help, handle that now
	 * and then exit. Avoid doing anything else that
	 * might fail and prevent access to help.
	 */
	if subCommand.CommandType() == helpString {
		subCommand.PrintUsage(subCommand.CommandType())
		return nil
	}

	/* TODO: this is where we would read a
	 * configuration file. Not currently implemented.
	 */
	analyzeError := subCommand.Analyze(vcc.Log)

	if analyzeError != nil {
		vcc.Log.Error(analyzeError, "fail to analyze command")
		return analyzeError
	}

	runError := subCommand.Run(vcc)
	if runError != nil {
		vcc.Log.Error(runError, "fail to run command")
	}
	return runError
}

// remove this function in VER-92222
func identifySubcommand(commands map[string]ClusterCommand, userCommandString string,
	logger vlog.Printer) (ClusterCommand, error) {
	command, ok := commands[userCommandString]
	if !ok {
		return nil, fmt.Errorf("unrecognized command '%s'",
			userCommandString)
	}

	logger.Log.Info("Recognized command", "cmd", userCommandString)
	return command, nil
}

// remove this function in VER-92222
func checkMinimumInput(inputArgv []string) error {
	if len(inputArgv) >= minArgs {
		return nil
	}
	return fmt.Errorf("expected at least %d arguments but found only %d",
		minArgs,
		len(inputArgv))
}

// remove this function in VER-92222
func parseLogPathArg(argInput []string, defaultPath string) (logPath string) {
	for idx, arg := range argInput {
		if arg == "--log-path" {
			return argInput[idx+1]
		}
	}
	return defaultPath
}

// initConfig will initialize the dbOptions.ConfigPath field for the vcluster exe.
func initConfig() {
	vclusterExePath, err := os.Executable()
	cobra.CheckErr(err)
	// If running vcluster from /opt/vertica/bin, we will ensure
	// /opt/vertica/config exists before using it.
	const ensureOptVerticaConfigExists = true
	// If using the user config director ($HOME/.config), we will ensure the necessary dir exists.
	const ensureUserConfigDirExists = true
	initConfigImpl(vclusterExePath, ensureOptVerticaConfigExists, ensureUserConfigDirExists)
}

// initConfigImpl will initialize the dbOptions.ConfigPath field. It will make an
// attempt to figure out the best value. In certain circumstances, it may fail
// to have a config path at all. In that case dbOptions.ConfigPath will be left
// as an empty string.
func initConfigImpl(vclusterExePath string, ensureOptVerticaConfigExists, ensureUserConfigDirExists bool) {
	// We need to find the path to the config. The order of precedence is as follows:
	// 1. Option
	// 2. Environment variable
	// 3. Default locations
	//   a. /opt/vertica/config/vertica_config.yaml if running vcluster in /opt/vertica/bin
	//   b. $HOME/.config/vcluster/vertica_config.yaml otherwise
	//
	// If none of these things are true, then we run the cli without a config file.

	// If option is set, nothing else to do in here
	if dbOptions.ConfigPath != "" {
		return
	}

	// Check environment variable
	if dbOptions.ConfigPath == "" {
		val, ok := os.LookupEnv(vclusterConfigEnv)
		if ok && val != "" {
			dbOptions.ConfigPath = val
			return
		}
	}

	// Pick a default config file.

	// If we are running vcluster from /opt/vertica/bin, we'll assume we
	// have installed the vertica package on this machine and so can assume
	// /opt/vertica/config exists too.
	if vclusterExePath == "/opt/vertica/bin/vcluster" {
		const rpmConfDir = "/opt/vertica/config"
		_, err := os.Stat(rpmConfDir)
		if ensureOptVerticaConfigExists && err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			cobra.CheckErr(err)
		} else {
			dbOptions.ConfigPath = fmt.Sprintf("%s/%s", rpmConfDir, defConfigFileName)
			return
		}
	}

	// Finally default to the .config directory in the users home. This is used
	// by many CLI applications.
	cfgDir, err := os.UserConfigDir()
	cobra.CheckErr(err)

	// Ensure the config directory exists.
	path := filepath.Join(cfgDir, "vcluster")
	if ensureUserConfigDirExists {
		const configDirPerm = 0755
		err = os.MkdirAll(path, configDirPerm)
		if err != nil {
			// Just abort if we don't have write access to the config path
			return
		}
	}
	dbOptions.ConfigPath = fmt.Sprintf("%s/%s", path, defConfigFileName)
}
