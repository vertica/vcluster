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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStartReplication
 *
 * Implements ClusterCommand interface
 */
type CmdStartReplication struct {
	startRepOptions *vclusterops.VReplicationDatabaseOptions
	CmdBase
	targetPasswordFile string
}

func makeCmdStartReplication() *cobra.Command {
	newCmd := &CmdStartReplication{}
	opt := vclusterops.VReplicationDatabaseFactory()
	newCmd.startRepOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		startReplicationSubCmd,
		"Start database replication",
		`This subcommand starts a database replication. 
		
This subcommand copies table or schema data directly from one Eon Mode 
database's communal storage to another.

The --target-conn option serves as a collection file for gathering necessary
target information for replication. You need to run vcluster create_connection
to generate this connection file in order to use this option.

The --sandbox option is used to replicate from a sandbox to a target database
or another sandbox. You must specify the hosts of the target sandbox to replicate 
to a target sandbox. You can provide the --target-hosts option or specify the 
target hosts in the connection file. 

If the source database has EnableConnectCredentialForwarding enabled, the
target username and password can be ignored. If the target database uses trust
authentication, the password can be ignored.

Examples:
  # Start database replication with config and connection file
  vcluster replication start --config /opt/vertica/config/vertica_cluster.yaml \
    --target-conn /opt/vertica/config/target_connection.yaml 

  # Replicate data from a sandbox in the source database to a target database
  # specified in the connection file.
  vcluster replication start --config /opt/vertica/config/vertica_cluster.yaml \
    --target-conn /opt/vertica/config/target_connection.yaml --sandbox sand

  # Start database replication with user input and connection file
  vcluster replication start --db-name test_db --hosts 10.20.30.40 \
    --target-conn /opt/vertica/config/target_connection.yaml 

  # Start database replication with config and connection file
  # tls option and tls-based authentication
  vcluster replication start --config /opt/vertica/config/vertica_cluster.yaml \ 
    --key-file /path/to/key-file --cert-file /path/to/cert-file \
    --target-conn /opt/vertica/config/target_connection.yaml --source-tlsconfig test_tlsconfig
  
  # Start database replication with user input
  # option and password-based authentication 
  vcluster replication start --db-name test_db --db-user dbadmin --hosts 10.20.30.40 --target-db-name platform_db \
    --target-hosts 10.20.30.43 --password-file /path/to/password-file --target-db-user dbadmin \ 
    --target-password-file /path/to/password-file
`,
		[]string{dbNameFlag, hostsFlag, ipv6Flag, configFlag, passwordFlag, dbUserFlag, eonModeFlag, connFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// either target dbname+hosts or connection file must be provided
	cmd.MarkFlagsOneRequired(targetConnFlag, targetDBNameFlag)
	cmd.MarkFlagsOneRequired(targetConnFlag, targetHostsFlag)

	// hide eon mode flag since we expect it to come from config file, not from user input
	hideLocalFlags(cmd, []string{eonModeFlag})
	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdStartReplication) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.startRepOptions.TargetDB,
		targetDBNameFlag,
		"",
		"The target database that we will replicate to",
	)
	cmd.Flags().StringVar(
		&c.startRepOptions.SandboxName,
		sandboxFlag,
		"",
		"The source sandbox that we will replicate from",
	)
	cmd.Flags().StringSliceVar(
		&c.startRepOptions.TargetHosts,
		targetHostsFlag,
		[]string{},
		"Comma-separated list of hosts in target database")
	cmd.Flags().StringVar(
		&c.startRepOptions.TargetUserName,
		targetUserNameFlag,
		"",
		"The username for connecting to the target database",
	)
	cmd.Flags().StringVar(
		&c.startRepOptions.SourceTLSConfig,
		sourceTLSConfigFlag,
		"",
		"The TLS configuration to use when connecting to the target database "+
			", must exist in the source database",
	)
	cmd.Flags().StringVar(
		&globals.connFile,
		targetConnFlag,
		"",
		"[Required] The connection file created with the create_connection command, "+
			"containing the database name, hosts, and password (if any) for the target database. "+
			"Alternatively, you can provide this information manually with --target-db-name, "+
			"--target-hosts, and --target-password-file",
	)
	markFlagsFileName(cmd, map[string][]string{targetConnFlag: {"yaml"}})
	//  password flags
	cmd.Flags().StringVar(
		&c.targetPasswordFile,
		targetPasswordFileFlag,
		"",
		"Path to the file to read the password for target database. ",
	)
}

func (c *CmdStartReplication) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.ResetUserInputOptions(&c.startRepOptions.DatabaseOptions)

	// replication only works for an Eon db
	// When eon mode cannot be found in config file, we set its value to true
	if !viper.IsSet(eonModeKey) {
		c.startRepOptions.IsEon = true
	}

	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdStartReplication) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.startRepOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}
	err := c.parseTargetHostList()
	if err != nil {
		return err
	}

	err = c.parseTargetPassword()
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.startRepOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	return c.setDBPassword(&c.startRepOptions.DatabaseOptions)
}

func (c *CmdStartReplication) parseTargetHostList() error {
	if len(c.startRepOptions.TargetHosts) > 0 {
		err := util.ParseHostList(&c.startRepOptions.TargetHosts)
		if err != nil {
			return fmt.Errorf("must specify at least one target host to replicate")
		}
	}
	return nil
}

func (c *CmdStartReplication) parseTargetPassword() error {
	options := c.startRepOptions
	if !viper.IsSet(targetPasswordFileKey) {
		// reset password option to nil if password is not provided in cli
		options.TargetPassword = nil
		return nil
	}
	if c.startRepOptions.TargetPassword == nil {
		options.TargetPassword = new(string)
	}

	if c.targetPasswordFile == "" {
		return fmt.Errorf("target password file path is empty")
	}
	password, err := c.passwordFileHelper(c.targetPasswordFile)
	if err != nil {
		return err
	}
	*options.TargetPassword = password
	return nil
}

func (c *CmdStartReplication) Run(vcc vclusterops.ClusterCommands) error {
	vcc.LogInfo("Called method Run()")

	options := c.startRepOptions

	err := vcc.VReplicateDatabase(options)
	if err != nil {
		vcc.LogError(err, "fail to replicate to database", "targetDB", options.TargetDB)
		return err
	}
	vcc.DisplayInfo("Successfully replicated to database %s", options.TargetDB)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance
func (c *CmdStartReplication) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.startRepOptions.DatabaseOptions = *opt
	c.startRepOptions.TargetUserName = globals.targetUserName
	c.startRepOptions.TargetDB = globals.targetDB
	c.startRepOptions.TargetHosts = globals.targetHosts
	c.targetPasswordFile = globals.targetPasswordFile
}
