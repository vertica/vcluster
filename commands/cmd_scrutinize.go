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

/* CmdScrutinize
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for scrutinize operation.
 * Prepares the inputs for the library.
 *
 */
type CmdScrutinize struct {
	CmdBase
	sOptions vclusterops.VScrutinizeOptions
}

func makeCmdScrutinize() *CmdScrutinize {
	newCmd := &CmdScrutinize{}
	newCmd.parser = flag.NewFlagSet("scrutinize", flag.ExitOnError)

	// Parse one variable and store it in a arbitrary location
	newCmd.sOptions.Password = newCmd.parser.String("password",
		"",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))

	newCmd.sOptions.UserName = newCmd.parser.String("db-user",
		"",
		util.GetOptionalFlagMsg("Database username. Consider using single quotes to avoid shell substitution."))

	newCmd.sOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input",
		false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))

	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated host list"))

	return newCmd
}

func (c *CmdScrutinize) CommandType() string {
	return "scrutinize"
}

func (c *CmdScrutinize) Parse(inputArgv []string) error {
	c.argv = inputArgv
	// from now on we use the internal copy of argv
	return c.parseInternal()
}

func (c *CmdScrutinize) parseInternal() error {
	if c.parser == nil {
		return fmt.Errorf("unexpected nil for CmdScrutinize.parser")
	}
	vlog.LogPrintInfo("Parsing scrutinize command input")
	parseError := c.ParseArgv()
	if parseError != nil {
		return parseError
	}

	vlog.LogInfoln("Parsing host list")
	var hostParseError error
	c.sOptions.RawHosts, hostParseError = util.SplitHosts(*c.hostListStr)
	if hostParseError != nil {
		return hostParseError
	}
	vlog.LogInfo("Host list size %d values %s", len(c.sOptions.RawHosts), c.sOptions.RawHosts)
	return nil
}

func (c *CmdScrutinize) Analyze() error {
	vlog.LogInfoln("Called method Analyze()")

	var resolveError error
	c.sOptions.Hosts, resolveError = util.ResolveRawHostsToAddresses(c.sOptions.RawHosts, false /*ipv6?*/)
	if resolveError != nil {
		return resolveError
	}
	vlog.LogInfo("Resolved host list to IPs: %s", c.sOptions.Hosts)
	return nil
}

func (c *CmdScrutinize) Run(log vlog.Printer) error {
	vcc := vclusterops.VClusterCommands{
		Log: log.WithName(c.CommandType()),
	}
	vlog.LogPrintInfo("Running scrutinize")
	vcc.Log.V(0).Info("Calling method Run() for command " + c.CommandType())
	err := vcc.VScrutinize(&c.sOptions)
	vlog.LogPrintInfo("Completed method Run() for command " + c.CommandType())
	return err
}
