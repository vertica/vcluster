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
	"flag"
	"fmt"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
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

func makeListAllNodes() *CmdListAllNodes {
	newCmd := &CmdListAllNodes{}
	newCmd.oldParser = flag.NewFlagSet("list_allnodes", flag.ExitOnError)

	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", "Comma-separated list of hosts to participate in database")
	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, "List all nodes with IPv6 hosts")

	fetchNodeStateOpt := vclusterops.VFetchNodeStateOptionsFactory()
	fetchNodeStateOpt.Password = newCmd.oldParser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))

	newCmd.fetchNodeStateOptions = &fetchNodeStateOpt

	return newCmd
}

func (c *CmdListAllNodes) CommandType() string {
	return "list_allnodes"
}

func (c *CmdListAllNodes) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.oldParser, "password") {
		c.fetchNodeStateOptions.Password = nil
	}

	return c.validateParse(logger)
}

func (c *CmdListAllNodes) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	// parse raw host str input into a []string
	err := c.parseHostList(&c.fetchNodeStateOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	// parse Ipv6
	c.fetchNodeStateOptions.Ipv6.FromBoolPointer(c.CmdBase.ipv6)

	return nil
}

func (c *CmdListAllNodes) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdListAllNodes) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	nodeStates, err := vcc.VFetchNodeState(c.fetchNodeStateOptions)
	if err != nil {
		// if all nodes are down, the nodeStates list is not empty
		// for this case, we don't want to show errors but show DOWN for the nodes
		if len(nodeStates) == 0 {
			vcc.Log.PrintError("fail to list all nodes: %s", err)
			return err
		}
	}

	bytes, err := json.MarshalIndent(nodeStates, "", "  ")
	if err != nil {
		return fmt.Errorf("fail to marshal the node state result, details %w", err)
	}
	fmt.Printf("Node states:\n%s", string(bytes))

	return nil
}
