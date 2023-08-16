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

/* CmdAddNode
 *
 * Implements ClusterCommand interface
 */
type CmdAddNode struct {
	addNodeOptions *vclusterops.VAddNodeOptions
	// Comma-separated list of hosts to add
	newHostListStr *string
	// Comma-separated list of vnode=host
	vnodeListStr *string

	CmdBase
}

func makeCmdAddNode() *CmdAddNode {
	// CmdAddNode
	newCmd := &CmdAddNode{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("db_add_node", flag.ExitOnError)
	addNodeOptions := vclusterops.VAddNodeOptionsFactory()

	// required flags
	addNodeOptions.Name = newCmd.parser.String("name", "", "The name of the database to be modified")
	newCmd.newHostListStr = newCmd.parser.String("add", "", "Comma-separated list of hosts to add to the database")

	// optional flags
	addNodeOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	// VER-88096: get all nodes information from the database and remove this option
	newCmd.vnodeListStr = newCmd.parser.String("vnodes", "", util.GetOptionalFlagMsg(
		"Comma-separated list of VNODE=HOST pairs part of the database nodes."+
			" Use it when you do not trust "+vclusterops.ConfigFileName))
	addNodeOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	addNodeOptions.InputHost = *newCmd.parser.String("host", "", util.GetOptionalFlagMsg(
		"The name or ip address of an up node that will be used to execute operations"))
	addNodeOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))
	addNodeOptions.CatalogPrefix = newCmd.parser.String("catalog-path", "", util.GetOptionalFlagMsg("Path of catalog directory"))
	addNodeOptions.DataPrefix = newCmd.parser.String("data-path", "", util.GetOptionalFlagMsg("Path of data directory"))
	addNodeOptions.SkipRebalanceShards = newCmd.parser.Bool("skip-rebalance-shards", false,
		util.GetOptionalFlagMsg("Skip the subcluster shards rebalancing"))

	// Eon flags
	// VER-88096: get all nodes information from the database and remove this option
	newCmd.isEon = newCmd.parser.Bool("eon-mode", false, util.GetEonFlagMsg("indicate if the database is an Eon db."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	addNodeOptions.SCName = newCmd.parser.String("subcluster", "", util.GetEonFlagMsg("The Name of subcluster for the new node"))
	addNodeOptions.DepotPrefix = newCmd.parser.String("depot-path", "", util.GetEonFlagMsg("Path to depot directory"))
	addNodeOptions.DepotSize = newCmd.parser.String("depot-size", "", util.GetEonFlagMsg("Size of depot"))

	newCmd.addNodeOptions = &addNodeOptions
	return newCmd
}

func (c *CmdAddNode) CommandType() string {
	return "db_add_node"
}

func (c *CmdAddNode) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	if !util.IsOptionSet(c.parser, "config-directory") {
		c.addNodeOptions.ConfigDirectory = nil
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.addNodeOptions.Password = nil
	}
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.CmdBase.isEon = nil
	}
	return c.validateParse()
}

func (c *CmdAddNode) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	err := c.addNodeOptions.ParseNewHostList(*c.newHostListStr)
	if err != nil {
		return err
	}
	err = c.validateAddNodeParseBaseOptions(&c.addNodeOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	return err
}

func (c *CmdAddNode) validateAddNodeParseBaseOptions(opt *vclusterops.DatabaseOptions) error {
	if !*opt.HonorUserInput {
		return nil
	}
	// parse IsEon
	opt.IsEon.FromBoolPointer(c.isEon)
	// parse Ipv6
	opt.Ipv6.FromBoolPointer(c.ipv6)
	err := c.addNodeOptions.ParseNodeList(*c.vnodeListStr)
	return err
}

func (c *CmdAddNode) Analyze() error {
	return nil
}

func (c *CmdAddNode) Run(log logr.Logger) error {
	vcc := vclusterops.VClusterCommands{
		Log: log.WithName(c.CommandType()),
	}
	vcc.Log.V(1).Info("Called method Run()")
	vdb, addNodeError := vcc.VAddNode(c.addNodeOptions)
	if addNodeError != nil {
		return addNodeError
	}
	// write cluster information to the YAML config file
	err := vclusterops.WriteClusterConfig(&vdb, c.addNodeOptions.ConfigDirectory)
	if err != nil {
		vlog.LogPrintWarning("fail to write config file, details: %s", err)
	}
	vlog.LogPrintInfo("Added nodes %s to database %s", *c.newHostListStr, *c.addNodeOptions.Name)
	return nil
}
