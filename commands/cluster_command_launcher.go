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

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const defaultLogPath = "/opt/vertica/log/vcluster.log"
const defaultExecutablePath = "/opt/vertica/bin/vcluster"

const CLIVersion = "1.2.0"
const vclusterLogPathEnv = "VCLUSTER_LOG_PATH"
const vclusterKeyFileEnv = "VCLUSTER_KEY_FILE"
const vclusterCertFileEnv = "VCLUSTER_CERT_FILE"

// *Flag is for the flag name, *Key is for viper key name
// They are bound together
const (
	dbNameFlag                  = "db-name"
	dbNameKey                   = "dbName"
	dbUserFlag                  = "db-user"
	dbUserKey                   = "dbUser"
	hostsFlag                   = "hosts"
	hostsKey                    = "hosts"
	catalogPathFlag             = "catalog-path"
	catalogPathKey              = "catalogPath"
	depotPathFlag               = "depot-path"
	depotPathKey                = "depotPath"
	dataPathFlag                = "data-path"
	dataPathKey                 = "dataPath"
	communalStorageLocationFlag = "communal-storage-location"
	communalStorageLocationKey  = "communalStorageLocation"
	ipv6Flag                    = "ipv6"
	ipv6Key                     = "ipv6"
	eonModeFlag                 = "eon-mode"
	eonModeKey                  = "eonMode"
	configParamFlag             = "config-param"
	configParamKey              = "configParam"
	logPathFlag                 = "log-path"
	logPathKey                  = "logPath"
	keyFileFlag                 = "key-file"
	keyFileKey                  = "keyFile"
	certFileFlag                = "cert-file"
	certFileKey                 = "certFile"
	passwordFlag                = "password"
	passwordKey                 = "password"
	passwordFileFlag            = "password-file"
	passwordFileKey             = "passwordFile"
	readPasswordFromPromptFlag  = "read-password-from-prompt"
	readPasswordFromPromptKey   = "readPasswordFromPrompt"
	configFlag                  = "config"
	configKey                   = "config"
	verboseFlag                 = "verbose"
	verboseKey                  = "verbose"
	outputFileFlag              = "output-file"
	outputFileKey               = "outputFile"
	subclusterFlag              = "subcluster"
	addNodeFlag                 = "new-hosts"
	sandboxFlag                 = "sandbox"
)

// Flag and key for database replication
const (
	targetDBNameFlag       = "target-db-name"
	targetDBNameKey        = "targetDBName"
	targetHostsFlag        = "target-hosts"
	targetHostsKey         = "targetHosts"
	targetUserNameFlag     = "target-db-user"
	targetUserNameKey      = "targetDBUser"
	targetPasswordFileFlag = "target-password-file"
	targetPasswordFileKey  = "targetPasswordFile"
	targetConnFlag         = "target-conn"
	targetConnKey          = "targetConn"
	sourceTLSConfigFlag    = "source-tlsconfig"
	sourceTLSConfigKey     = "sourceTLSConfig"
)

// flags to viper key map
var flagKeyMap = map[string]string{
	dbNameFlag:                  dbNameKey,
	dbUserFlag:                  dbUserKey,
	hostsFlag:                   hostsKey,
	catalogPathFlag:             catalogPathKey,
	depotPathFlag:               depotPathKey,
	dataPathFlag:                dataPathKey,
	communalStorageLocationFlag: communalStorageLocationKey,
	ipv6Flag:                    ipv6Key,
	eonModeFlag:                 eonModeKey,
	configParamFlag:             configParamKey,
	logPathFlag:                 logPathKey,
	keyFileFlag:                 keyFileKey,
	certFileFlag:                certFileKey,
	passwordFlag:                passwordKey,
	passwordFileFlag:            passwordFileKey,
	readPasswordFromPromptFlag:  readPasswordFromPromptKey,
	configFlag:                  configKey,
	verboseFlag:                 verboseKey,
	outputFileFlag:              outputFileKey,
	targetDBNameFlag:            targetDBNameKey,
	targetHostsFlag:             targetHostsKey,
	targetUserNameFlag:          targetUserNameKey,
	targetPasswordFileFlag:      targetPasswordFileKey,
	sourceTLSConfigFlag:         sourceTLSConfigKey,
}

const (
	createDBSubCmd          = "create_db"
	stopDBSubCmd            = "stop_db"
	reviveDBSubCmd          = "revive_db"
	manageConfigSubCmd      = "manage_config"
	configRecoverSubCmd     = "recover"
	configShowSubCmd        = "show"
	replicationSubCmd       = "replication"
	startReplicationSubCmd  = "start"
	listAllNodesSubCmd      = "list_allnodes"
	startDBSubCmd           = "start_db"
	dropDBSubCmd            = "drop_db"
	addSCSubCmd             = "db_add_subcluster"
	removeSCSubCmd          = "db_remove_subcluster"
	stopSCSubCmd            = "stop_subcluster"
	addNodeSubCmd           = "db_add_node"
	removeNodeSubCmd        = "db_remove_node"
	restartNodeSubCmd       = "restart_node"
	reIPSubCmd              = "re_ip"
	sandboxSubCmd           = "sandbox_subcluster"
	unsandboxSubCmd         = "unsandbox_subcluster"
	scrutinizeSubCmd        = "scrutinize"
	showRestorePointsSubCmd = "show_restore_points"
	installPkgSubCmd        = "install_packages"
)

// cmdGlobals holds global variables shared by multiple
// commands
type cmdGlobals struct {
	verbose  bool
	file     *os.File
	keyFile  string
	certFile string
}

var (
	dbOptions = vclusterops.DatabaseOptionsFactory()
	globals   = cmdGlobals{}
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

var logPath = defaultLogPath

// cmdInterface is an interface that every vcluster command needs to implement
// for making a basic cobra command
type cmdInterface interface {
	Parse(inputArgv []string, logger vlog.Printer) error
	Run(vcc vclusterops.ClusterCommands) error
	SetDatabaseOptions(opt *vclusterops.DatabaseOptions)
	SetParser(parser *pflag.FlagSet)
	setCommonFlags(cmd *cobra.Command, flags []string)
	initCmdOutputFile() (*os.File, error)
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
	logger.SetupOrDie(dbOptions.LogPath)

	vcc := vclusterops.VClusterCommands{
		VClusterCommandsLogger: vclusterops.VClusterCommandsLogger{
			Log: logger.WithName(cmd.CalledAs()),
		},
	}
	vcc.LogInfo("New VCluster command initialization")

	return vcc
}

// setDBOptionsUsingViper can set the value of flag using the relevant key in viper
func setDBOptionsUsingViper(flag string) error {
	switch flag {
	case dbNameFlag:
		dbOptions.DBName = viper.GetString(dbNameKey)
	case hostsFlag:
		dbOptions.RawHosts = viper.GetStringSlice(hostsKey)
	case catalogPathFlag:
		dbOptions.CatalogPrefix = viper.GetString(catalogPathKey)
	case depotPathFlag:
		dbOptions.DepotPrefix = viper.GetString(depotPathKey)
	case dataPathFlag:
		dbOptions.DataPrefix = viper.GetString(dataPathKey)
	case communalStorageLocationFlag:
		dbOptions.CommunalStorageLocation = viper.GetString(communalStorageLocationKey)
	case ipv6Flag:
		dbOptions.IPv6 = viper.GetBool(ipv6Key)
	case eonModeFlag:
		dbOptions.IsEon = viper.GetBool(eonModeKey)
	case configParamFlag:
		dbOptions.ConfigurationParameters = viper.GetStringMapString(configParamKey)
	case logPathFlag:
		dbOptions.LogPath = viper.GetString(logPathKey)
	case keyFileFlag:
		globals.keyFile = viper.GetString(keyFileKey)
	case certFileFlag:
		globals.certFile = viper.GetString(certFileKey)
	case verboseFlag:
		globals.verbose = viper.GetBool(verboseKey)
	default:
		return fmt.Errorf("cannot find the relevant database option for flag %q", flag)
	}

	return nil
}

// configViper configures viper to load database options using this order:
// user input -> environment variables -> vcluster config file
func configViper(cmd *cobra.Command, flagsInConfig []string) error {
	// initialize config file
	initConfig()

	// log-path is a flag that all the subcommands need
	flagsInConfig = append(flagsInConfig, logPathFlag)
	// cert-file and key-file are not available for
	// - manage_config
	// - manage_config show
	if cmd.CalledAs() != manageConfigSubCmd &&
		cmd.CalledAs() != configShowSubCmd {
		flagsInConfig = append(flagsInConfig, certFileFlag, keyFileFlag)
	}

	// bind viper keys to cobra flags
	for _, flag := range flagsInConfig {
		if _, ok := flagKeyMap[flag]; !ok {
			return fmt.Errorf("cannot find a relevant field in configuration file for flag %q", flag)
		}
		err := viper.BindPFlag(flagKeyMap[flag], cmd.Flags().Lookup(flag))
		if err != nil {
			return fmt.Errorf("fail to bind viper key %q to flag %q: %w", flagKeyMap[flag], flag, err)
		}
	}

	// bind viper keys to env vars
	err := viper.BindEnv(logPathKey, vclusterLogPathEnv)
	if err != nil {
		return fmt.Errorf("fail to bind viper key %q to environment variable %q: %w", logPathKey, vclusterLogPathEnv, err)
	}
	err = viper.BindEnv(keyFileKey, vclusterKeyFileEnv)
	if err != nil {
		return fmt.Errorf("fail to bind viper key %q to environment variable %q: %w", keyFileKey, vclusterKeyFileEnv, err)
	}
	err = viper.BindEnv(certFileKey, vclusterCertFileEnv)
	if err != nil {
		return fmt.Errorf("fail to bind viper key %q to environment variable %q: %w", certFileKey, vclusterCertFileEnv, err)
	}

	// load db options from config file to viper
	// note: config file is not available for create_db and revive_db
	//       manage_config does not need viper to load config file info
	if cmd.CalledAs() != createDBSubCmd &&
		cmd.CalledAs() != reviveDBSubCmd &&
		cmd.CalledAs() != configRecoverSubCmd &&
		cmd.CalledAs() != configShowSubCmd {
		err = loadConfigToViper()
		if err != nil {
			return err
		}
	}

	return handleViperUserInput(flagsInConfig)
}

func handleViperUserInput(flagsInConfig []string) error {
	// if a flag is set in viper through user input, env var or config file, we assign its viper value
	// to database options. viper can automatically retrieve the correct value following below order:
	// 1. user input
	// 2. environment variable
	// 3. config file
	// if the flag is not set in viper, the default value of it will be used
	for _, flag := range flagsInConfig {
		if _, ok := flagKeyMap[flag]; !ok {
			fmt.Printf("Warning: cannot find a relevant viper key for flag %q\n", flag)
			continue
		}
		if viper.IsSet(flagKeyMap[flag]) {
			err := setDBOptionsUsingViper(flag)
			if err != nil {
				return fmt.Errorf("fail to set flag %q using viper: %w", flag, err)
			}
		}
	}

	return nil
}

// filterFlagsInConfig can filter the flags that have a relevant field in vcluster config file
func filterFlagsInConfig(flags []string) []string {
	flagsAccepted := mapset.NewSet(flags...)
	allFlagsInConfig := mapset.NewSet([]string{dbNameFlag, hostsFlag, catalogPathFlag, depotPathFlag,
		dataPathFlag, communalStorageLocationFlag, ipv6Flag, eonModeFlag}...)
	return flagsAccepted.Intersect(allFlagsInConfig).ToSlice()
}

// makeBasicCobraCmd can make a basic cobra command for all vcluster commands.
// It will be called inside cmd_create_db.go, cmd_stop_db.go, ...
func makeBasicCobraCmd(i cmdInterface, use, short, long string, commonFlags []string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if globals.verbose {
				fmt.Println("---{VCluster begin}---")
			}
			flagsInConfig := filterFlagsInConfig(commonFlags)
			return configViper(cmd, flagsInConfig)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			vcc := initVcc(cmd)
			i.SetParser(cmd.Flags())
			f, err := i.initCmdOutputFile()
			if err != nil {
				return err
			}
			defer closeFile(globals.file)
			globals.file = f
			i.SetDatabaseOptions(&dbOptions)
			// parseError and runError will be printed by the command invoker.
			// we silence them in cobra for not printing duplicate error messages.
			cmd.SilenceErrors = true
			parseError := i.Parse(os.Args[2:], vcc.GetLog())
			if parseError != nil {
				vcc.LogError(parseError, "fail to parse command")
				return parseError
			}
			runError := i.Run(vcc)
			if runError != nil {
				cmd.SilenceUsage = true // don't show usage when vcluster fails and operation has started
				vcc.LogError(runError, "fail to run command")
			}

			return runError
		},
		PostRunE: func(cmd *cobra.Command, args []string) error {
			if globals.verbose {
				fmt.Println("---{VCluster end}---")
			}
			return nil
		},
	}
	i.setCommonFlags(cmd, commonFlags)

	return cmd
}

// makeSimpleCobraCmd can make a simple cobra command for some vcluster commands
// such as replication and manage_config
func makeSimpleCobraCmd(use, short, long string) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		Args:  cobra.NoArgs,
	}
}

// constructCmds returns a list of commands that will be executed
// by the cluster command launcher.
func constructCmds() []*cobra.Command {
	return []*cobra.Command{
		// db-scope cmds
		makeCmdCreateDB(),
		makeCmdStopDB(),
		makeListAllNodes(),
		makeCmdStartDB(),
		makeCmdDropDB(),
		makeCmdReviveDB(),
		makeCmdReIP(),
		makeCmdShowRestorePoints(),
		makeCmdInstallPackages(),
		// sc-scope cmds
		makeCmdAddSubcluster(),
		makeCmdRemoveSubcluster(),
		makeCmdStopSubcluster(),
		makeCmdSandboxSubcluster(),
		makeCmdUnsandboxSubcluster(),
		// node-scope cmds
		makeCmdRestartNodes(),
		makeCmdAddNode(),
		makeCmdRemoveNode(),
		// others
		makeCmdScrutinize(),
		makeCmdManageConfig(),
		makeCmdReplication(),
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

// operatingSystem is an interface for testing purpose
type operatingSystem interface {
	Executable() (string, error)
	UserConfigDir() (string, error)
	MkdirAll(path string, perm os.FileMode) error
}

type realOperatingSystem struct{}

func (realOperatingSystem) Executable() (string, error) {
	return os.Executable()
}

func (realOperatingSystem) UserConfigDir() (string, error) {
	return os.UserConfigDir()
}

func (realOperatingSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func setLogPath() {
	logPath = setLogPathImpl(realOperatingSystem{})
}

func setLogPathImpl(opsys operatingSystem) string {
	// find the executable path of vcluster
	vclusterExecutablePath, err := opsys.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cannot determine vcluster executable path:", err)
		return defaultLogPath
	}
	// log under /opt/vertica/log only if executable path is /opt/vertica/bin/vcluster
	if vclusterExecutablePath == defaultExecutablePath {
		return defaultLogPath
	}
	// log under $HOME/.config/vcluster
	cfgDir, err := opsys.UserConfigDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cannot determine user config directory path:", err)
		return defaultLogPath
	}
	// ensure the config directory exists.
	path := filepath.Join(cfgDir, "vcluster")
	const configDirPerm = 0755
	err = opsys.MkdirAll(path, configDirPerm)
	if err != nil {
		// print warning and continue execution
		// no need to error exit because user may set log path
		// which overwrites the default log path
		fmt.Fprintln(os.Stderr, "Cannot gain write access to user config directory path:", err)
	}
	return filepath.Join(path, "vcluster.log")
}

func closeFile(f *os.File) {
	if f != nil && f != os.Stdout {
		if err := f.Close(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}
}
