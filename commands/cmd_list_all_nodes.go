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
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdListAllNodes
 *
 * Implements ClusterCommand interface
 */
type CmdListAllNodes struct {
	fetchNodeStateOptions *vclusterops.VFetchNodeStateOptions

	CmdBase
}

func makeListAllNodes() *cobra.Command {
	newCmd := &CmdListAllNodes{}
	newCmd.ipv6 = new(bool)

	opt := vclusterops.VFetchNodeStateOptionsFactory()
	newCmd.fetchNodeStateOptions = &opt

	cmd := OldMakeBasicCobraCmd(
		newCmd,
		"list_allnodes",
		"List all nodes in the database",
		`This subcommand queries the status of the nodes in the consensus and prints
whether they are currently up or down.

The --host option allows you to specify the host or hosts that the program
should communicate with. The program will return the first response it
receives from any of the specified hosts.

The only requirement for each host is that it is running the spread daemon.

Examples:
	vcluster list_allnodes --password <password> --config <config_file>
`)

	// common db flags
	newCmd.setCommonFlags(cmd, []string{hostsFlag, configFlag, passwordFlag, outputFileFlag})

	return cmd
}

func (c *CmdListAllNodes) CommandType() string {
	return "list_allnodes"
}

func (c *CmdListAllNodes) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.OldResetUserInputOptions()

	return c.validateParse(logger)
}

func (c *CmdListAllNodes) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()", "command", c.CommandType())
	err := c.ValidateParseBaseOptions(&c.fetchNodeStateOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.fetchNodeStateOptions.DatabaseOptions)
}

func (c *CmdListAllNodes) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	nodeStates, err := vcc.VFetchNodeState(c.fetchNodeStateOptions)
	if err != nil {
		// if all nodes are down, the nodeStates list is not empty
		// for this case, we don't want to show errors but show DOWN for the nodes
		if len(nodeStates) == 0 {
			vcc.PrintError("fail to list all nodes: %s", err)
			return err
		}
	}

	bytes, err := json.MarshalIndent(nodeStates, "", "  ")
	if err != nil {
		return fmt.Errorf("fail to marshal the node state result, details %w", err)
	}

	c.writeCmdOutputToFile(globals.file, bytes, vcc.GetLog())
	vcc.LogInfo("Node states: ", "nodeStates", string(bytes))
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdListAllNodes
func (c *CmdListAllNodes) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.fetchNodeStateOptions.DatabaseOptions = *opt
}
