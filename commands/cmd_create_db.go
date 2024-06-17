/*
 (c) Copyright [2023-2024] Open Text.
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
)

/* CmdCreateDB
 *
 * Parses arguments to createDB and calls
 * the high-level function for createDB.
 *
 * Implements ClusterCommand interface
 */

type CmdCreateDB struct {
	createDBOptions *vclusterops.VCreateDatabaseOptions
	CmdBase
}

func makeCmdCreateDB() *cobra.Command {
	newCmd := &CmdCreateDB{}
	opt := vclusterops.VCreateDatabaseOptionsFactory()
	newCmd.createDBOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		createDBSubCmd,
		"Create a database",
		`This command creates a database on a set of hosts.

You must specify the database name, host list, catalog path, and data path.

If --config is not provided, a configuration file is created in one of the
following locations, in order of precedence:
- path set in VCLUSTER_CONFIG environment variable
- /opt/vertica/config/vertica_config.yaml if running vcluster from /opt/vertica/bin
- $HOME/.config/vcluster/vertica_config.yaml

To set multiple configuration parameters when the database is created, pass
--config-param a comma-separated list of NAME=VALUE pairs.

Remove the local directories like catalog, depot, and data, with the
--force-cleanup-on-failure or --force-removal-at-creation options.
The data deleted with these options is unrecoverable.

Provide the dbadmin password with the --password-file, --read-password-from-prompt,
or --password options.

Examples:
  # Create a database and save the generated config file under custom directory
  vcluster create_db --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --catalog-path /data --data-path /data \
    --config-param HttpServerConf=/opt/vertica/config/https_certs/httpstls.json \
    --config $HOME/custom/directory/vertica_cluster.yaml

  # Read the password from file
  vcluster create_db --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --catalog-path /data --data-path /data \
    --password-file /path/to/password-file

  # Generate a random password and read it from stdin
  cat /dev/urandom | tr -dc A-Za-z0-9 | head -c 8 | tee password.txt | vcluster create_db --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --catalog-path /data --data-path /data \
    --password-file -

  # Prompt the user to enter the password
  vcluster create_db --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --catalog-path /data --data-path /data \
    --read-password-from-prompt

  # Password passed as plain text
  vcluster create_db --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --catalog-path /data --data-path /data \
    --password 12345678
`,
		[]string{dbNameFlag, hostsFlag, catalogPathFlag, dataPathFlag, depotPathFlag,
			communalStorageLocationFlag, passwordFlag, configFlag, ipv6Flag, configParamFlag},
	)
	// local flags
	newCmd.setLocalFlags(cmd)

	// check if hidden flags can be implemented/removed in VER-92259
	// hidden flags
	newCmd.setHiddenFlags(cmd)

	// require db-name
	markFlagsRequired(cmd, dbNameFlag, hostsFlag, catalogPathFlag, dataPathFlag)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdCreateDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.createDBOptions.LicensePathOnNode,
		"license",
		"",
		"Database license",
	)
	cmd.Flags().StringVar(
		&c.createDBOptions.Policy,
		"policy",
		util.DefaultRestartPolicy,
		"Restart policy of the database",
	)
	cmd.Flags().StringVar(
		&c.createDBOptions.SQLFile,
		"sql",
		"",
		"SQL file to run (as dbadmin) immediately on database creation",
	)
	markFlagsFileName(cmd, map[string][]string{"sql": {"sql"}})
	cmd.Flags().IntVar(
		&c.createDBOptions.ShardCount,
		"shard-count",
		0,
		util.GetEonFlagMsg("Number of shards in the database"),
	)
	cmd.Flags().StringVar(
		&c.createDBOptions.DepotSize,
		"depot-size",
		"",
		util.GetEonFlagMsg("Size of depot"),
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.GetAwsCredentialsFromEnv,
		"get-aws-credentials-from-env-vars",
		false,
		util.GetEonFlagMsg("Read AWS credentials from environment variables"),
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.P2p,
		"point-to-point",
		true,
		"Configure Spread to use point-to-point communication between all Vertica nodes",
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.Broadcast,
		"broadcast",
		false,
		"Configure Spread to use UDP broadcast traffic between nodes on the same subnet",
	)
	cmd.Flags().IntVar(
		&c.createDBOptions.LargeCluster,
		"large-cluster",
		-1,
		"Enables a large cluster layout",
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.SpreadLogging,
		"spread-logging",
		false,
		"Whether enable spread logging",
	)
	cmd.Flags().IntVar(
		&c.createDBOptions.SpreadLoggingLevel,
		"spread-logging-level",
		-1,
		"Spread logging level",
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.ForceCleanupOnFailure,
		"force-cleanup-on-failure",
		false,
		"Force removal of existing directories on failure of command",
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.ForceRemovalAtCreation,
		"force-removal-at-creation",
		false,
		"Force removal of existing directories before creating the database",
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.ForceOverwriteFile,
		"force-overwrite-file",
		false,
		"Force overwrite of existing config and config param files",
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.SkipPackageInstall,
		"skip-package-install",
		false,
		"Skip the installation of packages from /opt/vertica/packages.",
	)
	cmd.Flags().IntVar(
		&c.createDBOptions.TimeoutNodeStartupSeconds,
		"startup-timeout",
		util.GetEnvInt("NODE_STATE_POLLING_TIMEOUT", util.DefaultTimeoutSeconds),
		"The timeout in seconds to wait for the nodes to start",
	)
}

// setHiddenFlags will set the hidden flags the command has.
// These hidden flags will not be shown in help and usage of the command, and they will be used internally.
func (c *CmdCreateDB) setHiddenFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(
		&c.createDBOptions.ClientPort,
		"client-port",
		util.DefaultClientPort,
		"",
	)
	cmd.Flags().BoolVar(
		&c.createDBOptions.SkipStartupPolling,
		"skip-startup-polling",
		false,
		"",
	)
	hideLocalFlags(cmd, []string{"policy", "sql", "client-port", "skip-startup-polling"})
}

func (c *CmdCreateDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	if !c.parser.Changed(depotPathFlag) {
		c.createDBOptions.IsEon = false
	} else {
		c.createDBOptions.IsEon = true
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

	if !c.usePassword() {
		err = c.getCertFilesFromCertPaths(&c.createDBOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

	err = c.setDBPassword(&c.createDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.initConfigParam()
	if err != nil {
		return err
	}
	return nil
}

func (c *CmdCreateDB) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")
	vdb, createError := vcc.VCreateDatabase(c.createDBOptions)
	if createError != nil {
		vcc.LogError(createError, "fail to create database")
		return createError
	}

	vcc.DisplayInfo("Successfully created a database with name [%s]", vdb.Name)

	// write db info to vcluster config file
	err := writeConfig(&vdb, c.createDBOptions.ForceOverwriteFile)
	if err != nil {
		vcc.DisplayWarning("Fail to write config file, details: %s\n", err)
	}
	// write config parameters to vcluster config param file
	err = c.writeConfigParam(c.createDBOptions.ConfigurationParameters, c.createDBOptions.ForceOverwriteFile)
	if err != nil {
		vcc.DisplayWarning("fail to write config param file, details: %s", err)
	}
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdCreateDB
func (c *CmdCreateDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.createDBOptions.DatabaseOptions = *opt
}
