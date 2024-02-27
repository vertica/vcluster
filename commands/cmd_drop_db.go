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
	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdDropDB
 *
 * Implements ClusterCommand interface
 */
type CmdDropDB struct {
	dropDBOptions *vclusterops.VDropDatabaseOptions

	CmdBase
}

func makeCmdDropDB() *cobra.Command {
	newCmd := &CmdDropDB{}
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VDropDatabaseOptionsFactory()
	newCmd.dropDBOptions = &opt
	newCmd.dropDBOptions.ForceDelete = new(bool)

	// VER-92345 update the long description about the hosts option
	cmd := makeBasicCobraCmd(
		newCmd,
		"drop_db",
		"Drop a database",
		`This command drops a database. Before running it, the database must be stopped. 
For an EON database, communal storage will not be deleted and the dropped database can be recovered 
by running revive_db.

The config file must exist for this command to work. The host information is exclusively obtained from it. 
When the command completes successfully, the config file is removed.

If you want to remove the local directories like catalog, depot, and data, you can use the --force-delete option. 
This option is not recoverable, so make sure you want this data removed before using it.
		
The database name must be provided.


Example:
  vcluster drop_db --db-name <db_name> --config <config_file>
`,
	)

	// common db flags
	setCommonFlags(cmd, []string{"db-name", "config", "honor-user-input"})

	// local flags
	newCmd.setLocalFlags(cmd)

	// require db-name, and one of (--honor-user-input, --config)
	markFlagsRequired(cmd, []string{"db-name"})
	requireHonorUserInputOrConfDir(cmd)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdDropDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		c.dropDBOptions.ForceDelete,
		"force-delete",
		false,
		util.GetOptionalFlagMsg("Delete local directories like catalog, depot, and data. "+
			"This option is not recoverable, so make sure you want this data removed before using it."),
	)
}

func (c *CmdDropDB) CommandType() string {
	return "drop_db"
}

func (c *CmdDropDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !c.parser.Changed("ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse(logger)
}

func (c *CmdDropDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	return c.ValidateParseBaseOptions(&c.dropDBOptions.DatabaseOptions)
}

func (c *CmdDropDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	err := vcc.VDropDatabase(c.dropDBOptions)
	if err != nil {
		vcc.Log.Error(err, "failed do drop the database")
		return err
	}

	vcc.Log.PrintInfo("Successfully dropped database %s", *c.dropDBOptions.DBName)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdDropDB
func (c *CmdDropDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.dropDBOptions.DatabaseOptions = *opt
}
