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

	Force               bool // Force cleanup to start the database
	AllowFallbackKeygen bool // Generate spread encryption key from Vertica. Use under support guidance only
	IgnoreClusterLease  bool // Ignore the cluster lease in communal storage
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
		`This command starts a database on a set of hosts.

Starts Vertica on each host and establishes cluster quorum. This command is 
similar to start_node, except start_db assumes that cluster quorum
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
  # Start a database partially with config file on a sandbox
  vcluster start_db --password testpassword \
    --config /home/dbadmin/vertica_cluster.yaml --sandbox "sand"
  # Start a database partially with config file on a sandbox
  vcluster start_db --password testpassword \
    --config /home/dbadmin/vertica_cluster.yaml --main-cluster-only
`,
		[]string{dbNameFlag, hostsFlag, communalStorageLocationFlag, ipv6Flag,
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
	// Update description of hosts flag locally for a detailed hint
	cmd.Flags().Lookup(hostsFlag).Usage = "Comma-separated list of hosts in database. This is used to start sandboxed hosts"

	cmd.Flags().StringVar(
		&c.startDBOptions.Sandbox,
		sandboxFlag,
		"",
		"Name of the sandbox to start",
	)
	cmd.Flags().BoolVar(
		&c.startDBOptions.MainCluster,
		"main-cluster-only",
		false,
		"Start the database on main cluster, but don't start any of the sandboxes",
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
	cmd.Flags().BoolVar(
		&c.startDBOptions.HostsInSandbox,
		"hosts-in-sandbox",
		false,
		"",
	)
	hideLocalFlags(cmd, []string{"unsafe", "force", "allow_fallback_keygen", "ignore_cluster_lease", "fast", "trim-hosts", "hosts-in-sandbox"})
}

func (c *CmdStartDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	c.ResetUserInputOptions(&c.startDBOptions.DatabaseOptions)
	return c.validateParse(logger)
}

func (c *CmdStartDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()", "command", startDBSubCmd)

	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.startDBOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

	err := c.ValidateParseBaseOptions(&c.startDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.setDBPassword(&c.startDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.setConfigParam(&c.startDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return nil
}
func filterInputHosts(options *vclusterops.VStartDatabaseOptions, dbConfig *DatabaseConfig) []string {
	filteredHosts := []string{}
	for _, n := range dbConfig.Nodes {
		// Collect sandbox hosts
		if options.Sandbox == n.Sandbox && n.Sandbox != util.MainClusterSandbox {
			filteredHosts = append(filteredHosts, n.Address)
		}
		// Collect main cluster hosts
		if options.MainCluster && n.Sandbox == util.MainClusterSandbox {
			filteredHosts = append(filteredHosts, n.Address)
		}
	}
	if len(options.Hosts) > 0 {
		return util.SliceCommon(filteredHosts, options.Hosts)
	}
	return filteredHosts
}
func (c *CmdStartDB) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.startDBOptions
	if options.Sandbox != "" && options.MainCluster {
		return fmt.Errorf("cannot use both --sandbox and --main-cluster-only options together ")
	}
	dbConfig, readConfigErr := readConfig()
	if readConfigErr == nil {
		if options.Sandbox != util.MainClusterSandbox || options.MainCluster {
			options.RawHosts = filterInputHosts(options, dbConfig)
		}
		options.FirstStartAfterRevive = dbConfig.FirstStartAfterRevive
	} else {
		vcc.DisplayWarning("fail to read config file", "error", readConfigErr)
		if options.MainCluster || options.Sandbox != util.MainClusterSandbox {
			return fmt.Errorf("cannot start the database partially without config file")
		}
	}

	vdb, err := vcc.VStartDatabase(options)
	if err != nil {
		vcc.LogError(err, "fail to start the database")
		return err
	}

	msg := fmt.Sprintf("Started database %s", options.DBName)
	if options.Sandbox != "" {
		sandboxMsg := fmt.Sprintf(" on sandbox %s", options.Sandbox)
		vcc.DisplayInfo(msg + sandboxMsg)
		return nil
	}
	if options.MainCluster {
		startMsg := " on the main cluster"
		vcc.DisplayInfo(msg + startMsg)
		return nil
	}
	vcc.DisplayInfo(msg)

	// for Eon database, update config file to fill nodes' subcluster information
	if readConfigErr == nil && options.IsEon {
		// write db info to vcluster config file
		vdb.FirstStartAfterRevive = false
		err = writeConfig(vdb, true /*forceOverwrite*/)
		if err != nil {
			vcc.DisplayWarning("fail to update config file, details: %s", err)
		}
	}

	// write config parameters to vcluster config param file
	err = c.writeConfigParam(options.ConfigurationParameters, true /*forceOverwrite*/)
	if err != nil {
		vcc.PrintWarning("fail to write config param file, details: %s", err)
	}

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdStartDB
func (c *CmdStartDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.startDBOptions.DatabaseOptions = *opt
}
