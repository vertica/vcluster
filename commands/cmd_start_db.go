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
)

/* CmdStartDB
 *
 * Implements ClusterCommand interface
 */
type CmdStartDB struct {
	CmdBase
	startDBOptions *vclusterops.VStartDatabaseOptions

	Force               bool // force cleanup to start the database
	AllowFallbackKeygen bool // Generate spread encryption key from Vertica. Use under support guidance only
	IgnoreClusterLease  bool // ignore the cluster lease in communal storage
	Unsafe              bool // Start database unsafely, skipping recovery.
	Fast                bool // Attempt fast startup database
}

func makeCmdStartDB() *cobra.Command {
	// CmdStartDB
	newCmd := &CmdStartDB{}
	opt := vclusterops.VStartDatabaseOptionsFactory()
	newCmd.startDBOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		startDBSubCmd,
		"Start a database",
		`This subcommand starts a database on a set of hosts.

Starts Vertica on each host and establishes cluster quorum. This subcommand is 
similar to restart_node, except start_db assumes that cluster quorum
has been lost.

The IP address provided for each node name must match the current IP address
in the Vertica catalog. If the IPs do not match, you must call re_ip
before start_db.

If you pass the --hosts command a subset of all nodes in the cluster, only the
specified nodes are started. There must be a quorum of nodes for the database
to start.

Examples:
  # Start a database with config file using password authentication
  vcluster start_db --password testpassword \
    --config /opt/vertica/config/vertica_cluster.yaml
`,
		[]string{dbNameFlag, hostsFlag, communalStorageLocationFlag,
			configFlag, catalogPathFlag, passwordFlag, eonModeFlag, configParamFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// check if hidden flags can be implemented/removed in VER-92259
	// hidden flags
	newCmd.setHiddenFlags(cmd)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdStartDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(
		&c.startDBOptions.StatePollingTimeout,
		"timeout",
		util.DefaultTimeoutSeconds,
		"The timeout (in seconds) to wait for polling node state operation",
	)
}

// setHiddenFlags will set the hidden flags the command has.
// These hidden flags will not be shown in help and usage of the command, and they will be used internally.
func (c *CmdStartDB) setHiddenFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		&c.Unsafe,
		"unsafe",
		false,
		"",
	)
	cmd.Flags().BoolVar(
		&c.Force,
		"force",
		false,
		"",
	)
	cmd.Flags().BoolVar(
		&c.AllowFallbackKeygen,
		"allow_fallback_keygen",
		false,
		"",
	)
	cmd.Flags().BoolVar(
		&c.IgnoreClusterLease,
		"ignore_cluster_lease",
		false,
		"",
	)
	cmd.Flags().BoolVar(
		&c.Fast,
		"fast",
		false,
		"",
	)
	cmd.Flags().BoolVar(
		&c.startDBOptions.TrimHostList,
		"trim-hosts",
		false,
		"",
	)
	hideLocalFlags(cmd, []string{"unsafe", "force", "allow_fallback_keygen", "ignore_cluster_lease", "fast", "trim-hosts"})
}

func (c *CmdStartDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	c.ResetUserInputOptions(&c.startDBOptions.DatabaseOptions)
	return c.validateParse(logger)
}

func (c *CmdStartDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()", "command", startDBSubCmd)

	err := c.getCertFilesFromCertPaths(&c.startDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.startDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.startDBOptions.DatabaseOptions)
}

func (c *CmdStartDB) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.startDBOptions

	vdb, err := vcc.VStartDatabase(options)
	if err != nil {
		vcc.LogError(err, "failed to start the database")
		return err
	}

	vcc.PrintInfo("Successfully start the database %s", options.DBName)

	// for Eon database, update config file to fill nodes' subcluster information
	if options.IsEon {
		// write db info to vcluster config file
		err := writeConfig(vdb, vcc.GetLog())
		if err != nil {
			vcc.PrintWarning("fail to update config file, details: %s", err)
		}
	}

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdStartDB
func (c *CmdStartDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.startDBOptions.DatabaseOptions = *opt
}
