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
	// Comma-separated list of node names, which exist in the cluster
	nodeNameListStr string

	CmdBase
}

func makeCmdAddNode() *cobra.Command {
	// CmdAddNode
	newCmd := &CmdAddNode{}
	opt := vclusterops.VAddNodeOptionsFactory()
	newCmd.addNodeOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		addNodeSubCmd,
		"Add host(s) to an existing database",
		`This command adds one or more hosts to an existing database.

You must provide the --new-hosts option followed by one or more hosts to add as
a comma-separated list.

You cannot add hosts to a sandbox subcluster in an Eon Mode database.

Use the --node-names option to address issues resulting from a failed node 
addition attempt. It's crucial to include all expected nodes in the catalog
when using this option. This command removes any surplus nodes from the
catalog, provided they are down, before commencing the node addition process.
Omitting the option will skip this node trimming process.

Examples:
  # Add a single host to the existing database with config file
  vcluster add_node --db-name test_db --new-hosts 10.20.30.43 \
    --config /opt/vertica/config/vertica_cluster.yaml

  # Add multiple hosts to the existing database with user input
  vcluster add_node --db-name test_db --new-hosts 10.20.30.43,10.20.30.44 \
    --data-path /data --hosts 10.20.30.40 \
    --node-names v_test_db_node0001,v_test_db_node0002
`,
		[]string{dbNameFlag, configFlag, hostsFlag, ipv6Flag, dataPathFlag, depotPathFlag,
			passwordFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// require hosts to add
	markFlagsRequired(cmd, addNodeFlag)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdAddNode) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVar(
		&c.addNodeOptions.NewHosts,
		addNodeFlag,
		[]string{},
		"Comma-separated list of host(s) to add to the database",
	)
	cmd.Flags().BoolVar(
		&c.addNodeOptions.ForceRemoval,
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
		&c.addNodeOptions.SCName,
		subclusterFlag,
		"",
		util.GetEonFlagMsg("The Name of subcluster"+
			" to which the host(s) must be added. If empty default subcluster is considered"),
	)
	cmd.Flags().StringVar(
		&c.addNodeOptions.DepotSize,
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
	cmd.Flags().IntVar(
		&c.addNodeOptions.TimeOut,
		"add-node-timeout",
		util.GetEnvInt("NODE_STATE_POLLING_TIMEOUT", util.DefaultTimeoutSeconds),
		"The timeout to wait for the nodes to add",
	)
}

func (c *CmdAddNode) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.ResetUserInputOptions(&c.addNodeOptions.DatabaseOptions)
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

	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.addNodeOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

	return c.setDBPassword(&c.addNodeOptions.DatabaseOptions)
}

// parseNewHostList trims and lowercases the hosts in --add
func (c *CmdAddNode) parseNewHostList() error {
	if len(c.addNodeOptions.NewHosts) > 0 {
		err := util.ParseHostList(&c.addNodeOptions.NewHosts)
		if err != nil {
			// the err from util.ParseHostList will be "must specify a host or host list"
			// we overwrite the error here to provide more details
			return fmt.Errorf("must specify at least one host to add")
		}
	}
	return nil
}

func (c *CmdAddNode) parseNodeNameList() error {
	// if --node-names is set, there must be at least one node name
	if c.parser.Changed("node-names") {
		if c.nodeNameListStr == "" {
			return fmt.Errorf("when --node-names is specified, "+
				"must provide all existing node names in %q", c.addNodeOptions.DBName)
		}

		c.addNodeOptions.ExpectedNodeNames = strings.Split(c.nodeNameListStr, ",")
	}

	return nil
}

func (c *CmdAddNode) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.addNodeOptions

	vdb, err := vcc.VAddNode(options)
	if err != nil {
		vcc.LogError(err, "fail to add node")
		return err
	}

	// write db info to vcluster config file
	err = writeConfig(&vdb, true /*forceOverwrite*/)
	if err != nil {
		vcc.DisplayWarning("fail to write config file, details: %s", err)
	}

	vcc.DisplayInfo("Successfully added nodes %v to database %s", c.addNodeOptions.NewHosts, options.DBName)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdAddNode
func (c *CmdAddNode) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.addNodeOptions.DatabaseOptions = *opt
}
