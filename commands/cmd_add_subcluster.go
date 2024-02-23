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

func makeCmdAddSubcluster() *CmdAddSubcluster {
	// CmdAddSubcluster
	newCmd := &CmdAddSubcluster{}

	// parser, used to parse command-line flags
	newCmd.oldParser = flag.NewFlagSet("db_add_subcluster", flag.ExitOnError)
	addSubclusterOptions := vclusterops.VAddSubclusterOptionsFactory()

	// required flags
	addSubclusterOptions.DBName = newCmd.oldParser.String("db-name", "", "The name of the database to be modified")
	addSubclusterOptions.SCName = newCmd.oldParser.String("subcluster", "", "The name of the new subcluster")

	// optional flags
	addSubclusterOptions.Password = newCmd.oldParser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts in database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	addSubclusterOptions.IsPrimary = newCmd.oldParser.Bool("is-primary", false,
		util.GetOptionalFlagMsg("The new subcluster will be a primary subcluster"))
	addSubclusterOptions.ControlSetSize = newCmd.oldParser.Int("control-set-size", vclusterops.ControlSetSizeDefaultValue,
		util.GetOptionalFlagMsg("The number of nodes that will run spread within the subcluster"))
	// new flags comparing to adminTools db_add_subcluster
	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, util.GetOptionalFlagMsg("Add subcluster with IPv6 hosts"))
	addSubclusterOptions.HonorUserInput = newCmd.oldParser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	newCmd.oldParser.StringVar(&addSubclusterOptions.ConfigPath, "config", "", util.GetOptionalFlagMsg("Path to the config file"))

	// Eon flags
	// isEon is target for early checking before VClusterOpEngine runs since add-subcluster is only supported in eon mode
	newCmd.isEon = newCmd.oldParser.Bool("eon-mode", false, util.GetEonFlagMsg("indicate if the database is an Eon db."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))

	// hidden options
	// TODO implement these hidden options in db_add_subcluster, then move them to optional flags above
	newCmd.scHostListStr = newCmd.oldParser.String("sc-hosts", "", util.SuppressHelp)
	addSubclusterOptions.CloneSC = newCmd.oldParser.String("like", "", util.SuppressHelp)

	newCmd.addSubclusterOptions = &addSubclusterOptions

	newCmd.oldParser.Usage = func() {
		util.SetParserUsage(newCmd.oldParser, "db_add_subcluster")
	}

	return newCmd
}

func (c *CmdAddSubcluster) CommandType() string {
	return "db_add_subcluster"
}

func (c *CmdAddSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the values of those options to nil
	if !util.IsOptionSet(c.oldParser, "password") {
		c.addSubclusterOptions.Password = nil
	}
	if !util.IsOptionSet(c.oldParser, "eon-mode") {
		c.CmdBase.isEon = nil
	}
	if !util.IsOptionSet(c.oldParser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdAddSubcluster) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	return c.OldValidateParseBaseOptions(&c.addSubclusterOptions.DatabaseOptions)
}

func (c *CmdAddSubcluster) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdAddSubcluster) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	options := c.addSubclusterOptions

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	err = vcc.VAddSubcluster(options)
	if err != nil {
		vcc.Log.Error(err, "failed to add subcluster")
		return err
	}

	vcc.Log.PrintInfo("Added subcluster %s to database %s", *options.SCName, *options.DBName)
	return nil
}
