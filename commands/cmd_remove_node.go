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

	"github.com/go-logr/logr"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdRemoveNode
 *
 * Implements ClusterCommand interface
 */
type CmdRemoveNode struct {
	removeNodeOptions *vclusterops.VRemoveNodeOptions
	// Comma-separated list of hosts to add
	hostToRemoveListStr *string

	CmdBase
}

func makeCmdRemoveNode() *CmdRemoveNode {
	// CmdRemoveNode
	newCmd := &CmdRemoveNode{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("db_remove_node", flag.ExitOnError)
	removeNodeOptions := vclusterops.VRemoveNodeOptionsFactory()

	// required flags
	removeNodeOptions.Name = newCmd.parser.String("db-name", "", "The name of the database to remove node(s) from")
	newCmd.hostToRemoveListStr = newCmd.parser.String("remove", "", "Comma-separated list of hosts to remove from the database")

	// optional flags
	removeNodeOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	removeNodeOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated hosts that will initially be used"+
		" to get cluster info from the db. Use it when you do not trust "+vclusterops.ConfigFileName))
	removeNodeOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))
	removeNodeOptions.ForceDelete = newCmd.parser.Bool("force-delete", true, util.GetOptionalFlagMsg("Whether force delete directories"+
		" if they are not empty"))
	removeNodeOptions.DataPrefix = newCmd.parser.String("data-path", "", util.GetOptionalFlagMsg("Path of data directory"))
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Create database with IPv6 hosts"))

	// Eon flags
	// VER-88096: get all nodes information from the database and remove this option
	removeNodeOptions.DepotPrefix = newCmd.parser.String("depot-path", "", util.GetEonFlagMsg("Path to depot directory"))

	newCmd.removeNodeOptions = &removeNodeOptions
	return newCmd
}

func (c *CmdRemoveNode) CommandType() string {
	return "db_remove_node"
}

func (c *CmdRemoveNode) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.removeNodeOptions.ConfigDirectory = nil
	}

	if !util.IsOptionSet(c.parser, "password") {
		c.removeNodeOptions.Password = nil
	}
	return c.validateParse()
}

func (c *CmdRemoveNode) validateParse() error {
	vlog.LogInfo("[%s] Called validateParse()", c.CommandType())

	err := c.removeNodeOptions.ParseHostToRemoveList(*c.hostToRemoveListStr)
	if err != nil {
		return err
	}
	return c.ValidateParseBaseOptions(&c.removeNodeOptions.DatabaseOptions)
}

func (c *CmdRemoveNode) Analyze() error {
	return nil
}

func (c *CmdRemoveNode) Run(log logr.Logger) error {
	vcc := vclusterops.VClusterCommands{
		Log: log.WithName(c.CommandType()),
	}
	vcc.Log.V(1).Info("Called method Run()")

	vdb, err := vcc.VRemoveNode(c.removeNodeOptions)
	if err != nil {
		return err
	}
	vlog.LogPrintInfo("Successfully removed nodes %s from database %s", *c.hostToRemoveListStr, *c.removeNodeOptions.Name)

	// write cluster information to the YAML config file.
	err = vclusterops.WriteClusterConfig(&vdb, c.removeNodeOptions.ConfigDirectory)
	if err != nil {
		vlog.LogPrintWarning("failed to write config file, details: %s", err)
	}
	vlog.LogPrintInfo("Successfully updated config file")
	return nil
}
