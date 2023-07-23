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
	"strconv"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStopDB
 *
 * Parses arguments to stopDB and calls
 * the high-level function for stopDB.
 *
 * Implements ClusterCommand interface
 */

type CmdStopDB struct {
	CmdBase
	stopDBOptions *vclusterops.VStopDatabaseOptions
}

func MakeCmdStopDB() *CmdStopDB {
	// CmdStopDB
	newCmd := &CmdStopDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("stop_db", flag.ExitOnError)
	stopDBOptions := vclusterops.VStopDatabaseOptionsFactory()

	// required flags
	stopDBOptions.Name = newCmd.parser.String("name", "", "The name of the database to be stopped")

	// optional flags
	stopDBOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.parser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts in database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	// new flags comparing to adminTools stop_db
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Stop database with IPv6 hosts"))
	stopDBOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	stopDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	// Eon flags
	newCmd.isEon = newCmd.parser.Bool("eon-mode", false, util.GetEonFlagMsg("indicate if the database is an Eon db."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	stopDBOptions.DrainSeconds = newCmd.parser.Int("drain-seconds", util.DefaultDrainSeconds,
		util.GetEonFlagMsg("seconds to wait for user connections to close."+
			" Default value is "+strconv.Itoa(util.DefaultDrainSeconds)+" seconds."+
			" When the time expires, connections will be forcibly closed and the db will shut down"))

	// hidden options
	// TODO use these hidden options in stop_db, CheckUserConn can be move to optional flags above when we support it
	stopDBOptions.CheckUserConn = newCmd.parser.Bool("if-no-users", false, util.SuppressHelp)
	stopDBOptions.ForceKill = newCmd.parser.Bool("force-kill", false, util.SuppressHelp)

	newCmd.stopDBOptions = &stopDBOptions

	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "stop_db")
	}

	return newCmd
}

func (c *CmdStopDB) CommandType() string {
	return "stop_db"
}

func (c *CmdStopDB) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.stopDBOptions.Password = nil
	}
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.CmdBase.isEon = nil
	}
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}
	if !util.IsOptionSet(c.parser, "drain-seconds") {
		c.stopDBOptions.DrainSeconds = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.stopDBOptions.ConfigDirectory = nil
	}

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdStopDB) validateParse() error {
	vlog.LogInfo("[%s] Called validateParse()", c.CommandType())
	return c.ValidateParseBaseOptions(&c.stopDBOptions.DatabaseOptions)
}

func (c *CmdStopDB) Analyze() error {
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdStopDB) Run() error {
	vlog.LogInfo("[%s] Called method Run()", c.CommandType())
	vcc := vclusterops.VClusterCommands{}
	stopError := vcc.VStopDatabase(c.stopDBOptions)
	if stopError != nil {
		return stopError
	}
	vlog.LogPrintInfo("Stopped a database with name %s", *c.stopDBOptions.Name)
	return nil
}
