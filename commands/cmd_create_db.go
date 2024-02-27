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
	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"github.com/vertica/vcluster/vclusterops/vstruct"
)

/* CmdCreateDB
 *
 * Parses arguments to createDB and calls
 * the high-level function for createDB.
 *
 * Implements ClusterCommand interface
 */

type CmdCreateDB struct {
	createDBOptions    *vclusterops.VCreateDatabaseOptions
	configParamListStr *string // raw input from user, need further processing

	CmdBase
}

func makeCmdCreateDB() *cobra.Command {
	newCmd := &CmdCreateDB{}
	newCmd.ipv6 = new(bool)
	newCmd.configParamListStr = new(string)
	opt := vclusterops.VCreateDatabaseOptionsFactory()
	newCmd.createDBOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		"create_db",
		"Create a database",
		`This creates a new database on a given set of hosts.

The name of the database must be provided.

Example:
  vcluster create_db --db-name <db_name> --hosts <all_hosts_of_the_db> --catalog-path <catalog-path> 
  --data-path <data-path> --config-pram <key1=value1,key2=value2,key3=value3> --config <config_file>
`,
	)

	// common db flags
	setCommonFlags(cmd, []string{"db-name", "hosts", "catalog-path", "data-path",
		"depot-path", "password", "config", "communal-storage-location", "log-path"})

	// local flags
	newCmd.setLocalFlags(cmd)

	// check if hidden flags can be implemented/removed in VER-92259
	// hidden flags
	newCmd.setHiddenFlags(cmd)

	// require db-name
	cmd.PreRun = func(c *cobra.Command, args []string) {
		markFlagsRequired(c, []string{"db-name"})
	}

	// require the flags to be file name
	markFlagsFileName(cmd, map[string][]string{"config": {"yaml"}, "log-path": {"log"}, "sql": {"sql"}})

	// require the flags to be dir name
	markFlagsDirName(cmd, []string{"catalog-path", "data-path", "depot-path"})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdCreateDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		c.createDBOptions.LicensePathOnNode,
		"license",
		"",
		util.GetOptionalFlagMsg("Database license"),
	)
	cmd.Flags().StringVar(
		c.createDBOptions.Policy,
		"policy",
		util.DefaultRestartPolicy,
		util.GetOptionalFlagMsg("Restart policy of the database"),
	)
	cmd.Flags().StringVar(
		c.createDBOptions.SQLFile,
		"sql",
		"",
		util.GetOptionalFlagMsg("SQL file to run (as dbadmin) immediately on database creation"),
	)
	cmd.Flags().IntVar(
		c.createDBOptions.ShardCount,
		"shard-count",
		0,
		util.GetEonFlagMsg("Number of shards in the database"),
	)
	cmd.Flags().StringVar(
		c.createDBOptions.DepotSize,
		"depot-size",
		"",
		util.GetEonFlagMsg("Size of depot"),
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.GetAwsCredentialsFromEnv,
		"get-aws-credentials-from-env-vars",
		false,
		util.GetEonFlagMsg("Read AWS credentials from environment variables"),
	)
	cmd.Flags().StringVar(
		c.configParamListStr,
		"config-param",
		"",
		util.GetOptionalFlagMsg(
			"Comma-separated list of NAME=VALUE pairs for setting database configuration parameters immediately on database creation"),
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.P2p,
		"point-to-point",
		true,
		util.GetOptionalFlagMsg("Configure Spread to use point-to-point communication between all Vertica nodes"),
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.Broadcast,
		"broadcast",
		false,
		util.GetOptionalFlagMsg("Configure Spread to use UDP broadcast traffic between nodes on the same subnet"),
	)
	cmd.Flags().IntVar(
		c.createDBOptions.LargeCluster,
		"large-cluster",
		-1,
		util.GetOptionalFlagMsg("Enables a large cluster layout"),
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.SpreadLogging,
		"spread-logging",
		false,
		util.GetOptionalFlagMsg("Whether enable spread logging"),
	)
	cmd.Flags().IntVar(
		c.createDBOptions.SpreadLoggingLevel,
		"spread-logging-level",
		-1,
		util.GetOptionalFlagMsg("Spread logging level"),
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.ForceCleanupOnFailure,
		"force-cleanup-on-failure",
		false,
		util.GetOptionalFlagMsg("Force removal of existing directories on failure of command"),
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.ForceRemovalAtCreation,
		"force-removal-at-creation",
		false,
		util.GetOptionalFlagMsg("Force removal of existing directories before creating the database"),
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.SkipPackageInstall,
		"skip-package-install",
		false,
		util.GetOptionalFlagMsg("Skip the installation of packages from /opt/vertica/packages."),
	)
	cmd.Flags().IntVar(
		c.createDBOptions.TimeoutNodeStartupSeconds,
		"startup-timeout",
		util.DefaultTimeoutSeconds,
		util.GetOptionalFlagMsg("The timeout to wait for the nodes to start"),
	)
}

// setHiddenFlags will set the hidden flags the command has.
// These hidden flags will not be shown in help and usage of the command, and they will be used internally.
func (c *CmdCreateDB) setHiddenFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(
		c.createDBOptions.ClientPort,
		"client-port",
		util.DefaultClientPort,
		"",
	)
	cmd.Flags().BoolVar(
		c.createDBOptions.SkipStartupPolling,
		"skip-startup-polling",
		false,
		"",
	)
	hideLocalFlags(cmd, []string{"policy", "sql", "client-port", "skip-startup-polling"})
}

func (c *CmdCreateDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// handle options that are not passed in
	if !c.parser.Changed("log-path") {
		c.createDBOptions.LogPath = nil
	}
	if !c.parser.Changed("password") {
		c.createDBOptions.Password = nil
	}

	if !c.parser.Changed("depot-path") {
		c.createDBOptions.IsEon = vstruct.False
	} else {
		c.createDBOptions.IsEon = vstruct.True
	}

	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdCreateDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	err := c.ValidateParseBaseOptions(&c.createDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	// check the format of config param string, and parse it into configParams
	configParams, err := util.ParseConfigParams(*c.configParamListStr)
	if err != nil {
		return err
	}
	if configParams != nil {
		c.createDBOptions.ConfigurationParameters = configParams
	}
	return nil
}

func (c *CmdCreateDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")
	vdb, createError := vcc.VCreateDatabase(c.createDBOptions)
	if createError != nil {
		return createError
	}
	// write cluster information to the YAML config file
	err := vdb.WriteClusterConfig(c.createDBOptions.ConfigPath, vcc.Log)
	if err != nil {
		vcc.Log.PrintWarning("fail to write config file, details: %s", err)
	}
	vcc.Log.PrintInfo("Created a database with name [%s]", vdb.Name)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdCreateDB
func (c *CmdCreateDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.createDBOptions.DatabaseOptions = *opt
}
