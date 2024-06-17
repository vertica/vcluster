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

	opt := vclusterops.VFetchNodeStateOptionsFactory()
	newCmd.fetchNodeStateOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		listAllNodesSubCmd,
		"List all nodes in the database",
		`This command queries the status of the nodes in the database and prints
whether they are up or down.

To provide its status, each host must run the spread daemon.

You must provide the --hosts option one or more hosts as a comma-separated
list. list_all_nodes returns the first response it receives from any host.

The --db-name and --catalog-path options are required only when vcluster cannot
obtain node information from a running database and the config file is not
provided.

Examples:
  # List the status of nodes with config file where password authentication is
  # used to access the database
  vcluster list_all_nodes --password testpassword \
    --config /opt/vertica/config/vertica_cluster.yaml
`,
		[]string{dbNameFlag, hostsFlag, passwordFlag, ipv6Flag, catalogPathFlag, configFlag, outputFileFlag},
	)

	return cmd
}

func (c *CmdListAllNodes) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	c.ResetUserInputOptions(&c.fetchNodeStateOptions.DatabaseOptions)

	// Set GetVersion to true so that the CLI can retrieve versions for down nodes
	// by invoking two additional operations: NMAHealth and NMA readCatalogEditor
	c.fetchNodeStateOptions.GetVersion = true
	return c.validateParse(logger)
}

func (c *CmdListAllNodes) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()", "command", listAllNodesSubCmd)
	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.fetchNodeStateOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

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
			vcc.LogError(err, "fail to list all nodes")
			return err
		}
	}

	bytes, err := c.marshalNoteStates(nodeStates)
	if err != nil {
		return err
	}

	c.writeCmdOutputToFile(globals.file, bytes, vcc.GetLog())
	vcc.LogInfo("Node states: ", "nodeStates", string(bytes))
	// if writing into stdout, add a new line
	// otherwise, the successful message may be wrapped into the same line of the node state output
	if c.output == "" {
		fmt.Println("")
	}
	vcc.DisplayInfo("Successfully listed all nodes")
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdListAllNodes
func (c *CmdListAllNodes) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.fetchNodeStateOptions.DatabaseOptions = *opt
}

func (c *CmdListAllNodes) marshalNoteStates(nodeStates []vclusterops.NodeInfo) (bytes []byte, err error) {
	var isEon bool
	if len(nodeStates) > 0 {
		// node in Eon database should not have an empty sc name
		if nodeStates[0].Subcluster != "" {
			isEon = true
		}
	}

	if isEon {
		bytes, err = json.MarshalIndent(nodeStates, "", "  ")
		if err != nil {
			return bytes, fmt.Errorf("fail to marshal the node state result, details %w", err)
		}
	} else {
		var nodeStatesEnterprise []vclusterops.NodeInfoEnterprise
		for _, n := range nodeStates {
			var nEnterprise vclusterops.NodeInfoEnterprise
			nEnterprise.Address = n.Address
			nEnterprise.Name = n.Name
			nEnterprise.State = n.State
			nEnterprise.CatalogPath = n.CatalogPath
			nEnterprise.Version = n.Version
			nodeStatesEnterprise = append(nodeStatesEnterprise, nEnterprise)
		}
		bytes, err = json.MarshalIndent(nodeStatesEnterprise, "", "  ")
		if err != nil {
			return bytes, fmt.Errorf("fail to marshal the node state result, details %w", err)
		}
	}

	return bytes, nil
}
