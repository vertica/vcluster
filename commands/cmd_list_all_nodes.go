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

func MakeListAllNodes() *CmdListAllNodes {
	newCmd := &CmdListAllNodes{}
	newCmd.parser = flag.NewFlagSet("list_allnodes", flag.ExitOnError)

	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated list of hosts to participate in database")
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "List all nodes with IPv6 hosts")

	fetchNodeStateOpt := vclusterops.VFetchNodeStateOptionsFactory()
	fetchNodeStateOpt.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))

	newCmd.fetchNodeStateOptions = &fetchNodeStateOpt

	return newCmd
}

func (c *CmdListAllNodes) CommandType() string {
	return "list_allnodes"
}

func (c *CmdListAllNodes) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.fetchNodeStateOptions.Password = nil
	}

	return c.validateParse()
}

func (c *CmdListAllNodes) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// parse raw host str input into a []string
	err := c.ParseHostList(&c.fetchNodeStateOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	// parse Ipv6
	c.fetchNodeStateOptions.Ipv6.FromBoolPointer(c.CmdBase.ipv6)

	return nil
}

func (c *CmdListAllNodes) Analyze() error {
	return nil
}

func (c *CmdListAllNodes) Run() error {
	vlog.LogInfoln("Called method Run()")

	vcc := vclusterops.VClusterCommands{}
	nodeStates, err := vcc.VFetchNodeState(c.fetchNodeStateOptions)
	if err != nil {
		// if all nodes are down, the nodeStates list is not empty
		// for this case, we don't want to show errors but show DOWN for the nodes
		if len(nodeStates) == 0 {
			vlog.LogPrintError("fail to list all nodes: %w", err)
			return err
		}
	}

	bytes, err := json.Marshal(nodeStates)
	if err != nil {
		return fmt.Errorf("fail to marshal the node state result, details %w", err)
	}
	vlog.LogPrintInfo("Node states: %s", string(bytes))
	return nil
}
