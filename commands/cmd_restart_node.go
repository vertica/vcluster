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

/* CmdRestartNodes
 *
 * Implements ClusterCommand interface
 */
type CmdRestartNodes struct {
	CmdBase
	restartNodesOptions *vclusterops.VStartNodesOptions

	// Comma-separated list of vnode=host
	vnodeListStr map[string]string
}

func makeCmdRestartNodes() *cobra.Command {
	// CmdRestartNodes
	newCmd := &CmdRestartNodes{}
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VStartNodesOptionsFactory()
	newCmd.restartNodesOptions = &opt

	cmd := OldMakeBasicCobraCmd(
		newCmd,
		restartNodeSubCmd,
		"Restart nodes in the database",
		`This subcommand starts individual nodes in a running cluster. This differs from
start_db, which starts Vertica after cluster quorum is lost.

You can pass --restart a comma-separated list of NODE_NAME=IP_TO_RESTART pairs to restart 
multiple nodes without a config file. If the IP_TO_RESTART value does not match the information 
stored in the catalog for NODE_NAME, Vertica updates the catalog with the IP_TO_RESTART value and 
restarts the node.

Examples:
  # Restart a single host in the database with a config file
  vcluster restart_node --db-name test_db --restart v_test_db_node0004=192.168.1.104
  --password "" --config $HOME/test_db/vertica_config.yaml

  # Restart a single host and change its IP address in the database with a config file
  vcluster restart_node --db-name test_db --restart v_test_db_node0004=192.168.1.105
  --password "" --config $HOME/test_db/vertica_config.yaml
 
  # Restart multiple hosts in the database with a config file
  vcluster restart_node --db-name test_db --restart v_test_db_node0003=192.168.1.103,
  v_test_db_node0004=192.168.1.104 --password "" --config $HOME/test_db/vertica_config.yaml		
`,
	)

	// common db flags
	newCmd.setCommonFlags(cmd, []string{dbNameFlag, hostsFlag, configFlag, passwordFlag})

	// local flags
	newCmd.setLocalFlags(cmd)

	// require nodes to restart
	markFlagsRequired(cmd, []string{"restart"})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdRestartNodes) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringToStringVar(
		&c.vnodeListStr,
		"restart",
		map[string]string{},
		"Comma-separated list of <node_name=re_ip_host> pairs part of the database nodes that need to be restarted",
	)
	cmd.Flags().IntVar(
		&c.restartNodesOptions.StatePollingTimeout,
		"timeout",
		util.DefaultTimeoutSeconds,
		"The timeout (in seconds) to wait for polling node state operation",
	)
}

func (c *CmdRestartNodes) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.OldResetUserInputOptions()

	return c.validateParse(logger)
}

func (c *CmdRestartNodes) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	err := c.restartNodesOptions.ParseNodesList(c.vnodeListStr)
	if err != nil {
		return err
	}

	err = c.getCertFilesFromCertPaths(&c.restartNodesOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.restartNodesOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.restartNodesOptions.DatabaseOptions)
}

func (c *CmdRestartNodes) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.restartNodesOptions

	// load vdb info from the YAML config file
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	// this is the instruction that will be used by both CLI and operator
	err = vcc.VStartNodes(options)
	if err != nil {
		return err
	}

	var hostToRestart []string
	for _, ip := range options.Nodes {
		hostToRestart = append(hostToRestart, ip)
	}
	vcc.PrintInfo("Successfully restart hosts %s of the database %s", hostToRestart, *options.DBName)

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdRestartNodes
func (c *CmdRestartNodes) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.restartNodesOptions.DatabaseOptions = *opt
}
