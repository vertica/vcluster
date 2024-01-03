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
	newCmd.parser = flag.NewFlagSet("unsandbox", flag.ExitOnError)
	newCmd.usOptions = vclusterops.VUnsandboxOptionsFactory()

	// required flags
	newCmd.usOptions.DBName = newCmd.parser.String("db-name", "", "The name of the database to run unsandboxing. May be omitted on k8s.")
	newCmd.usOptions.SCName = newCmd.parser.String("subcluster", "", "The name of the subcluster to be unsandboxed")

	// optional flags
	newCmd.usOptions.Password = newCmd.parser.String("password", "",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts to participate in database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "start database with with IPv6 hosts")
	newCmd.usOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	newCmd.usOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	return newCmd
}

func (c *CmdUnsandboxSubcluster) CommandType() string {
	return "unsandbox_subcluster"
}

func (c *CmdUnsandboxSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	// from now on we use the internal copy of argv
	return c.parseInternal(logger)
}

func (c *CmdUnsandboxSubcluster) parseInternal(logger vlog.Printer) error {
	if c.parser == nil {
		return fmt.Errorf("unexpected nil for CmdUnsandboxSubcluster.parser")
	}
	logger.PrintInfo("Parsing Unsandboxing command input")
	parseError := c.ParseArgv()
	if parseError != nil {
		return parseError
	}
	return nil
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
