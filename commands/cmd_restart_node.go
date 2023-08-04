package commands

import (
	"flag"
	"fmt"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdRestartNode
 *
 * Implements ClusterCommand interface
 */
type CmdRestartNodes struct {
	CmdBase
	restartNodesOptions *vclusterops.VRestartNodesOptions

	// Comma-separated list of hosts to restart
	HostsToRestartStr *string
}

func MakeCmdRestartNodes() *CmdRestartNodes {
	// CmdStartNodes
	newCmd := &CmdRestartNodes{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("restart_node", flag.ExitOnError)
	restartNodesOptions := vclusterops.VRestartNodesOptionsFactory()

	// require flags
	restartNodesOptions.Name = newCmd.parser.String("db-name", "", "The name of the database to restart nodes")
	newCmd.HostsToRestartStr = newCmd.parser.String("restart", "", "Comma-separated list of hosts to restart")

	// optional flags
	restartNodesOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated hosts that participate in the database"+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "restart nodes with IPv6 hosts")

	restartNodesOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user input instead of reading the options from "+vclusterops.ConfigFileName))
	restartNodesOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	newCmd.restartNodesOptions = &restartNodesOptions
	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "restart_node")
	}
	return newCmd
}

func (c *CmdRestartNodes) CommandType() string {
	return "restart_node"
}

func (c *CmdRestartNodes) Parse(inputArgv []string) error {
	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	if !util.IsOptionSet(c.parser, "config-directory") {
		c.restartNodesOptions.ConfigDirectory = nil
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	if util.IsOptionSet(c.parser, "password") {
		c.restartNodesOptions.UsePassword = true
	}

	return c.validateParse()
}

func (c *CmdRestartNodes) validateParse() error {
	err := c.restartNodesOptions.ParseHostListToRestart(*c.HostsToRestartStr)
	if err != nil {
		return err
	}

	vlog.LogInfo("[%s] Called validateParse()", c.CommandType())
	return c.ValidateParseBaseOptions(&c.restartNodesOptions.DatabaseOptions)
}

func (c *CmdRestartNodes) Analyze() error {
	// Analyze() is needed to fulfill an interface
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdRestartNodes) Run() error {
	vlog.LogInfo("[%s] Called method Run()", c.CommandType())
	vcc := vclusterops.VClusterCommands{}
	err := vcc.VRestartNodes(c.restartNodesOptions)
	if err != nil {
		return err
	}

	vlog.LogPrintInfo("Successfully restart nodes %s of the database %s", *c.HostsToRestartStr, *c.restartNodesOptions.Name)

	return nil
}
