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

/* CmdAddSubcluster
 *
 * Parses arguments to addSubcluster and calls
 * the high-level function for addSubcluster.
 *
 * Implements ClusterCommand interface
 */

type CmdAddSubcluster struct {
	CmdBase
	addSubclusterOptions *vclusterops.VAddSubclusterOptions
	scHostListStr        *string
}

func MakeCmdAddSubcluster() CmdAddSubcluster {
	// CmdAddSubcluster
	newCmd := CmdAddSubcluster{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("db_add_subcluster", flag.ExitOnError)
	addSubclusterOptions := vclusterops.VAddSubclusterOptionsFactory()

	// required flags
	addSubclusterOptions.Name = newCmd.parser.String("name", "", "The name of the database to be modified")
	addSubclusterOptions.SCName = newCmd.parser.String("subcluster", "", "The name of the new subcluster")

	// optional flags
	addSubclusterOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts in database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	addSubclusterOptions.IsPrimary = newCmd.parser.Bool("is-primary", false,
		util.GetOptionalFlagMsg("The new subcluster will be a primary subcluster"))
	addSubclusterOptions.ControlSetSize = newCmd.parser.Int("control-set-size", vclusterops.ControlSetSizeDefaultValue,
		util.GetOptionalFlagMsg("The number of nodes that will run spread within the subcluster"))
	// new flags comparing to adminTools db_add_subcluster
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Add subcluster with IPv6 hosts"))
	addSubclusterOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	addSubclusterOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	// Eon flags
	// isEon is target for early checking before VClusterOpEngine runs since add-subcluster is only supported in eon mode
	newCmd.isEon = newCmd.parser.Bool("eon-mode", false, util.GetEonFlagMsg("indicate if the database is an Eon db."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))

	// hidden options
	// TODO implement these hidden options in db_add_subcluster, then move them to optional flags above
	newCmd.scHostListStr = newCmd.parser.String("sc-hosts", "", util.SuppressHelp)
	addSubclusterOptions.CloneSC = newCmd.parser.String("like", "", util.SuppressHelp)

	newCmd.addSubclusterOptions = &addSubclusterOptions

	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "db_add_subcluster")
	}

	return newCmd
}

func (c *CmdAddSubcluster) CommandType() string {
	return "db_add_subcluster"
}

func (c *CmdAddSubcluster) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the values of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.addSubclusterOptions.Password = nil
	}
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.CmdBase.isEon = nil
	}
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.addSubclusterOptions.ConfigDirectory = nil
	}

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdAddSubcluster) validateParse() error {
	vlog.LogInfoln("Called validateParse()")
	return c.ValidateParseBaseOptions(&c.addSubclusterOptions.DatabaseOptions)
}

func (c *CmdAddSubcluster) Analyze() error {
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdAddSubcluster) Run() error {
	vlog.LogInfoln("Called method Run()")
	vcc := vclusterops.VClusterCommands{}
	err := vcc.VAddSubcluster(c.addSubclusterOptions)
	if err != nil {
		return err
	}
	vlog.LogPrintInfo("Added subcluster %s to database %s", *c.addSubclusterOptions.SCName, *c.addSubclusterOptions.Name)
	return nil
}
