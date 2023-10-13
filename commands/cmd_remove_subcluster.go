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

/* CmdRemoveSubcluster
 *
 * Implements ClusterCommand interface
 */
type CmdRemoveSubcluster struct {
	removeScOptions *vclusterops.VRemoveScOptions

	CmdBase
}

func makeCmdRemoveSubcluster() *CmdRemoveSubcluster {
	newCmd := &CmdRemoveSubcluster{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("db_remove_subcluster", flag.ExitOnError)
	removeScOptions := vclusterops.VRemoveScOptionsFactory()

	// required flags
	removeScOptions.DBName = newCmd.parser.String("db-name", "", "Name of the database to remove subcluster")
	removeScOptions.SubclusterToRemove = newCmd.parser.String("remove", "", "Name of subcluster to be removed")
	// VER-88096: get all nodes information from the database and remove this option
	removeScOptions.DepotPrefix = newCmd.parser.String("depot-path", "", util.GetEonFlagMsg("Path to depot directory"))

	// optional flags
	removeScOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	removeScOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated hosts that will initially be used"+
		" to get cluster info from the db. Use it when you do not trust "+vclusterops.ConfigFileName))
	removeScOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))
	removeScOptions.ForceDelete = newCmd.parser.Bool("force-delete", true, util.GetOptionalFlagMsg("Whether force delete directories"+
		" if they are not empty"))
	removeScOptions.DataPrefix = newCmd.parser.String("data-path", "", util.GetOptionalFlagMsg("Path of data directory"))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Whether the hosts use IPv6 addresses"))

	newCmd.removeScOptions = &removeScOptions
	return newCmd
}

func (c *CmdRemoveSubcluster) CommandType() string {
	return "db_remove_subcluster"
}

func (c *CmdRemoveSubcluster) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.removeScOptions.ConfigDirectory = nil
	}

	if !util.IsOptionSet(c.parser, "password") {
		c.removeScOptions.Password = nil
	}
	return c.validateParse()
}

func (c *CmdRemoveSubcluster) validateParse() error {
	vlog.LogInfo("[%s] Called validateParse()", c.CommandType())

	return c.ValidateParseBaseOptions(&c.removeScOptions.DatabaseOptions)
}

func (c *CmdRemoveSubcluster) Analyze() error {
	return nil
}

func (c *CmdRemoveSubcluster) Run(log vlog.Printer) error {
	vcc := vclusterops.VClusterCommands{
		Log: log.WithName(c.CommandType()),
	}

	vcc.Log.V(1).Info("Called method Run()")
	vdb, err := vcc.VRemoveSubcluster(c.removeScOptions)
	if err != nil {
		return err
	}
	vcc.Log.PrintInfo("Successfully removed subcluster %s from database %s",
		*c.removeScOptions.SubclusterToRemove, *c.removeScOptions.DBName)

	// write cluster information to the YAML config file.
	err = vdb.WriteClusterConfig(c.removeScOptions.ConfigDirectory)
	if err != nil {
		vcc.Log.PrintWarning("failed to write config file, details: %s", err)
	}
	vcc.Log.PrintInfo("Successfully updated config file")

	return nil
}
