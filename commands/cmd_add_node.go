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
	"strings"

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
	// Comma-separated list of node names, which exist in the cluster
	nodeNameListStr *string

	CmdBase
}

func makeCmdAddNode() *CmdAddNode {
	// CmdAddNode
	newCmd := &CmdAddNode{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("db_add_node", flag.ExitOnError)
	addNodeOptions := vclusterops.VAddNodeOptionsFactory()

	// required flags
	addNodeOptions.DBName = newCmd.parser.String("db-name", "", "The name of the database to be modified")
	newCmd.newHostListStr = newCmd.parser.String("add", "", "Comma-separated list of hosts to add to the database")

	// optional flags
	addNodeOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	addNodeOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated hosts that will initially be used"+
		" to get cluster info from the database. Use it when you do not trust "+vclusterops.ConfigFileName))
	addNodeOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))
	addNodeOptions.DataPrefix = newCmd.parser.String("data-path", "", util.GetOptionalFlagMsg("Path of data directory"))
	addNodeOptions.ForceRemoval = newCmd.parser.Bool("force-removal", false,
		util.GetOptionalFlagMsg("Force removal of existing directories before adding nodes"))
	addNodeOptions.SkipRebalanceShards = newCmd.parser.Bool("skip-rebalance-shards", false,
		util.GetOptionalFlagMsg("Skip the subcluster shards rebalancing"))

	// Eon flags
	// VER-88096: get all nodes information from the database and remove this option
	addNodeOptions.SCName = newCmd.parser.String("subcluster", "", util.GetEonFlagMsg("The Name of subcluster"+
		" to which the nodes must be added. If empty default subcluster is considered"))
	addNodeOptions.DepotPrefix = newCmd.parser.String("depot-path", "", util.GetEonFlagMsg("Path to depot directory"))
	addNodeOptions.DepotSize = newCmd.parser.String("depot-size", "", util.GetEonFlagMsg("Size of depot"))

	// Optional flags
	newCmd.nodeNameListStr = newCmd.parser.String("node-names", "",
		util.GetOptionalFlagMsg("Comma-separated list of node names that exist in the cluster. "+
			"Use with caution: not mentioned nodes will be trimmed from catalog."))

	newCmd.addNodeOptions = &addNodeOptions
	return newCmd
}

func (c *CmdAddNode) CommandType() string {
	return "db_add_node"
}

func (c *CmdAddNode) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
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
	return c.validateParse(logger)
}

func (c *CmdAddNode) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	err := c.parseNewHostList()
	if err != nil {
		return err
	}

	err = c.parseNodeNameList()
	if err != nil {
		return err
	}

	return c.ValidateParseBaseOptions(&c.addNodeOptions.DatabaseOptions)
}

// ParseNewHostList converts the string list of hosts, to add, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
func (c *CmdAddNode) parseNewHostList() error {
	inputHostList, err := util.SplitHosts(*c.newHostListStr)
	if err != nil {
		return fmt.Errorf("must specify at least one host to add: %w", err)
	}

	c.addNodeOptions.NewHosts = inputHostList
	return nil
}

func (c *CmdAddNode) parseNodeNameList() error {
	// if --node-names is set, there must be at least one node name
	if util.IsOptionSet(c.parser, "node-names") {
		if *c.nodeNameListStr == "" {
			return fmt.Errorf("when --node-names is specified, "+
				"must provide all existing node names in %s", *c.addNodeOptions.DBName)
		}

		c.addNodeOptions.ExpectedNodeNames = strings.Split(*c.nodeNameListStr, ",")
	}

	return nil
}

func (c *CmdAddNode) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdAddNode) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	options := c.addNodeOptions

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	vdb, addNodeError := vcc.VAddNode(options)
	if addNodeError != nil {
		return addNodeError
	}
	// write cluster information to the YAML config file
	err = vdb.WriteClusterConfig(options.ConfigDirectory, vcc.Log)
	if err != nil {
		vcc.Log.PrintWarning("fail to write config file, details: %s", err)
	}
	vcc.Log.PrintInfo("Added nodes %s to database %s", *c.newHostListStr, *options.DBName)
	return nil
}
