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
	"strings"

	"github.com/spf13/cobra"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdAddNode
 *
 * Implements ClusterCommand interface
 */
type CmdAddNode struct {
	addNodeOptions *vclusterops.VAddNodeOptions
	// Comma-separated list of hosts to add
	newHostListStr string
	// Comma-separated list of node names, which exist in the cluster
	nodeNameListStr string

	CmdBase
}

func makeCmdAddNode() *cobra.Command {
	// CmdAddNode
	newCmd := &CmdAddNode{}
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VAddNodeOptionsFactory()
	newCmd.addNodeOptions = &opt

	cmd := OldMakeBasicCobraCmd(
		newCmd,
		"db_add_node",
		"Add host(s) to an existing database",
		`This subcommand adds one or more hosts to an existing database.

You must provide the --add option one or more hosts to remove as a
comma-separated list. You cannot add hosts to a sandbox subcluster in an
Eon Mode database.

The --node-names option is utilized to address issues resulting from a failed
node addition attempt. It's crucial to include all expected nodes in the catalog
when using this option. This subcommand removes any surplus nodes from the
catalog, provided they are down, before commencing the node addition process.
Omitting the option will skip this node trimming process.

Examples:
  # Add a single host to the existing database with config file
  vcluster db_add_node --db-name test_db --add 10.20.30.43 --config \
  /opt/vertica/config/vertica_cluster.yaml

  # Add multiple hosts to the existing database with user input
  vcluster db_add_node --db-name test_db --add 10.20.30.43,10.20.30.44 \
  --data-path /data --hosts 10.20.30.40 --node-names \
  test_db_node0001,test_db_node0002
`,
	)

	// common db flags
	newCmd.setCommonFlags(cmd, []string{dbNameFlag, configFlag, hostsFlag, dataPathFlag, depotPathFlag,
		passwordFlag})

	// local flags
	newCmd.setLocalFlags(cmd)

	// require hosts to add
	markFlagsRequired(cmd, []string{"add"})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdAddNode) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.newHostListStr,
		"add",
		"",
		"Comma-separated list of host(s) to add to the database",
	)
	cmd.Flags().BoolVar(
		c.addNodeOptions.ForceRemoval,
		"force-removal",
		false,
		"Whether to force clean-up of existing directories before adding host(s)",
	)
	cmd.Flags().BoolVar(
		c.addNodeOptions.SkipRebalanceShards,
		"skip-rebalance-shards",
		false,
		util.GetEonFlagMsg("Skip the subcluster shards rebalancing"),
	)
	cmd.Flags().StringVar(
		c.addNodeOptions.SCName,
		"subcluster",
		"",
		util.GetEonFlagMsg("The Name of subcluster"+
			" to which the host(s) must be added. If empty default subcluster is considered"),
	)
	cmd.Flags().StringVar(
		c.addNodeOptions.DepotSize,
		"depot-size",
		"",
		util.GetEonFlagMsg("Size of depot"),
	)
	cmd.Flags().StringVar(
		&c.nodeNameListStr,
		"node-names",
		"",
		"Comma-separated list of node names that exist in the cluster",
	)
}

func (c *CmdAddNode) CommandType() string {
	return "db_add_node"
}

func (c *CmdAddNode) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.OldResetUserInputOptions()
	return c.validateParse(logger)
}

func (c *CmdAddNode) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	err := c.parseNewHostList()
	if err != nil {
		return err
	}

	err = c.parseNodeNameList()
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.addNodeOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.addNodeOptions.DatabaseOptions)
}

// ParseNewHostList converts the string list of hosts, to add, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
func (c *CmdAddNode) parseNewHostList() error {
	inputHostList, err := util.SplitHosts(c.newHostListStr)
	if err != nil {
		return fmt.Errorf("must specify at least one host to add: %w", err)
	}

	c.addNodeOptions.NewHosts = inputHostList
	return nil
}

func (c *CmdAddNode) parseNodeNameList() error {
	// if --node-names is set, there must be at least one node name
	if c.parser.Changed("node-names") {
		if c.nodeNameListStr == "" {
			return fmt.Errorf("when --node-names is specified, "+
				"must provide all existing node names in %q", *c.addNodeOptions.DBName)
		}

		c.addNodeOptions.ExpectedNodeNames = strings.Split(c.nodeNameListStr, ",")
	}

	return nil
}

func (c *CmdAddNode) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.addNodeOptions

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	vdb, addNodeError := vcc.VAddNode(options)
	if addNodeError != nil {
		return addNodeError
	}
	// write cluster information to the YAML config file
	err = vdb.WriteClusterConfig(options.ConfigPath, vcc.GetLog())
	if err != nil {
		vcc.PrintWarning("fail to write config file, details: %s", err)
	}
	vcc.PrintInfo("Added nodes %s to database %s", c.newHostListStr, *options.DBName)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdAddNode
func (c *CmdAddNode) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.addNodeOptions.DatabaseOptions = *opt
}
