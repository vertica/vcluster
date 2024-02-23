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

/* CmdSandbox
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for sandbox operation.
 * Prepares the inputs for the library.
 *
 */

type CmdSandboxSubcluster struct {
	CmdBase
	sbOptions vclusterops.VSandboxOptions
}

func (c *CmdSandboxSubcluster) TypeName() string {
	return "CmdSandboxSubcluster"
}

func makeCmdSandboxSubcluster() *CmdSandboxSubcluster {
	newCmd := &CmdSandboxSubcluster{}
	newCmd.oldParser = flag.NewFlagSet("sandbox_subcluster", flag.ExitOnError)
	newCmd.sbOptions = vclusterops.VSandboxOptionsFactory()

	// required flags
	newCmd.sbOptions.DBName = newCmd.oldParser.String("db-name", "", "The name of the database to run sandbox. May be omitted on k8s.")
	newCmd.sbOptions.SCName = newCmd.oldParser.String("subcluster", "", "The name of the subcluster to be sandboxed")
	newCmd.sbOptions.SandboxName = newCmd.oldParser.String("sandbox", "", "The name of the sandbox")

	// optional flags
	newCmd.sbOptions.Password = newCmd.oldParser.String("password", "",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))
	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", util.GetOptionalFlagMsg(
		"Comma-separated list of hosts to participate in database."+" Use it when you do not trust "+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, "start database with with IPv6 hosts")
	newCmd.sbOptions.HonorUserInput = newCmd.oldParser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	newCmd.oldParser.StringVar(&newCmd.sbOptions.ConfigPath, "config", "", util.GetOptionalFlagMsg("Path to the config file"))

	return newCmd
}

func (c *CmdSandboxSubcluster) CommandType() string {
	return "sandbox_subcluster"
}

func (c *CmdSandboxSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}
	return c.parseInternal(logger)
}

func (c *CmdSandboxSubcluster) parseInternal(logger vlog.Printer) error {
	logger.Info("Called parseInternal()")
	if c.oldParser == nil {
		return fmt.Errorf("unexpected nil for CmdSandboxSubcluster.parser")
	}
	if !util.IsOptionSet(c.oldParser, "password") {
		c.sbOptions.Password = nil
	}
	if !util.IsOptionSet(c.oldParser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.OldValidateParseBaseOptions(&c.sbOptions.DatabaseOptions)
}

func (c *CmdSandboxSubcluster) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdSandboxSubcluster) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.PrintInfo("Running sandbox subcluster")
	vcc.Log.Info("Calling method Run() for command " + c.CommandType())

	options := c.sbOptions
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config
	err = vcc.VSandbox(&options)
	vcc.Log.PrintInfo("Completed method Run() for command " + c.CommandType())
	return err
}
