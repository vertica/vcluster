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
	"flag"
	"fmt"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdUnsandbox
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for Unsandboxing operation.
 * Prepares the inputs for the library.
 *
 */
type CmdUnsandboxSubcluster struct {
	CmdBase
	usOptions vclusterops.VUnsandboxOptions
}

func (c *CmdUnsandboxSubcluster) TypeName() string {
	return "CmdUnsandboxSubcluster"
}

func makeCmdUnsandboxSubcluster() *CmdUnsandboxSubcluster {
	newCmd := &CmdUnsandboxSubcluster{}
	newCmd.oldParser = flag.NewFlagSet("unsandbox", flag.ExitOnError)

	newCmd.usOptions = vclusterops.VUnsandboxOptionsFactory()

	// required flags
	newCmd.usOptions.DBName = newCmd.oldParser.String("db-name", "", "The name of the database to run unsandboxing. May be omitted on k8s.")
	newCmd.usOptions.SCName = newCmd.oldParser.String("subcluster", "", "The name of the subcluster to be unsandboxed")

	// optional flags
	newCmd.usOptions.Password = newCmd.oldParser.String("password", "",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))
	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", util.GetOptionalFlagMsg(
		"Comma-separated list of hosts to participate in database."+" Use it when you do not trust "+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, "start database with with IPv6 hosts")
	newCmd.usOptions.HonorUserInput = newCmd.oldParser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	newCmd.oldParser.StringVar(&newCmd.usOptions.ConfigPath, "config", "", util.GetOptionalFlagMsg("Path to the config file"))

	return newCmd
}

func (c *CmdUnsandboxSubcluster) CommandType() string {
	return "unsandbox_subcluster"
}

func (c *CmdUnsandboxSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}
	return c.parseInternal(logger)
}

// ParseInternal parses internal commands for unsandboxed subclusters.
func (c *CmdUnsandboxSubcluster) parseInternal(logger vlog.Printer) error {
	logger.Info("Called parseInternal()")
	if c.oldParser == nil {
		return fmt.Errorf("unexpected nil for CmdUnsandboxSubcluster.parser")
	}
	c.setUnsetOptions()
	return c.OldValidateParseBaseOptions(&c.usOptions.DatabaseOptions)
}

func (c *CmdUnsandboxSubcluster) setUnsetOptions() {
	if !util.IsOptionSet(c.oldParser, "password") {
		c.usOptions.Password = nil
	}

	if !util.IsOptionSet(c.oldParser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}
}

func (c *CmdUnsandboxSubcluster) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")

	return nil
}

func (c *CmdUnsandboxSubcluster) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.PrintInfo("Running unsandbox subcluster")
	vcc.Log.Info("Calling method Run() for command " + c.CommandType())

	options := c.usOptions
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config
	err = vcc.VUnsandbox(&options)
	vcc.Log.PrintInfo("Completed method Run() for command " + c.CommandType())
	return err
}
