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
	"strconv"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdReviveDB
 *
 * Implements ClusterCommand interface
 */
type CmdReviveDB struct {
	CmdBase
	reviveDBOptions     *vclusterops.VReviveDatabaseOptions
	configurationParams *string // raw input from user, need further processing
}

func makeCmdReviveDB() *cobra.Command {
	// CmdReviveDB
	newCmd := &CmdReviveDB{}
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VReviveDBOptionsFactory()
	newCmd.reviveDBOptions = &opt
	newCmd.reviveDBOptions.CommunalStorageLocation = new(string)
	newCmd.configurationParams = new(string)

	cmd := makeBasicCobraCmd(
		newCmd,
		"revive_db",
		"Revive a database",
		`This revives an EON database to a given set of hosts.

The communal storage path must be provided and it cannot be empty. If access to communal storage 
requires access keys, these can be provided through the --config-param option.

You must also specify a set of hosts that matches the number of hosts when the database was running.

If --ignore-cluster-lease is set, revive_db will disable the check for the existence of other clusters 
running on the shared storage. Be cautious with this action, as it may lead to data corruption.

The name of the database must be provided.

Example:
  vcluster revive_db --db-name <db_name> --hosts <list_of_hosts>
    --communal-storage-location <communal_storage_location> --config <config_file>
`,
	)

	// common db flags
	setCommonFlags(cmd, []string{"db-name", "hosts", "communal-storage-location", "config"})

	// local flags
	newCmd.setLocalFlags(cmd)

	// require db-name
	markFlagsRequired(cmd, []string{"db-name", "communal-storage-location"})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdReviveDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().UintVar(
		&c.reviveDBOptions.LoadCatalogTimeout,
		"load-catalog-timeout",
		util.DefaultLoadCatalogTimeoutSeconds,
		util.GetOptionalFlagMsg("Set a timeout (in seconds) for loading remote catalog operation, default timeout is "+
			strconv.Itoa(util.DefaultLoadCatalogTimeoutSeconds)+"seconds"),
	)
	cmd.Flags().BoolVar(
		&c.reviveDBOptions.ForceRemoval,
		"force-removal",
		false,
		util.GetOptionalFlagMsg("Prior to reviving a database, ensure the deletion of pre-existing database directories "+
			"(excluding user storage directories)"),
	)
	cmd.Flags().BoolVar(
		&c.reviveDBOptions.DisplayOnly,
		"display-only",
		false,
		util.GetOptionalFlagMsg("Describe the database on communal storage, and exit"),
	)
	cmd.Flags().BoolVar(
		&c.reviveDBOptions.IgnoreClusterLease,
		"ignore-cluster-lease",
		false,
		util.GetOptionalFlagMsg("Disable the check for the existence of other clusters running on the shared storage, "+
			"but be cautious with this action, as it may lead to data corruption"),
	)
	cmd.Flags().StringVar(
		&c.reviveDBOptions.RestorePoint.Archive,
		"restore-point-archive",
		"",
		util.GetOptionalFlagMsg("Name of the restore archive to use for bootstrapping"),
	)
	cmd.Flags().IntVar(
		&c.reviveDBOptions.RestorePoint.Index,
		"restore-point-index",
		0,
		util.GetOptionalFlagMsg("The (1-based) index of the restore point in the restore archive to restore from"),
	)
	cmd.Flags().StringVar(
		&c.reviveDBOptions.RestorePoint.ID,
		"restore-point-id",
		"",
		util.GetOptionalFlagMsg("The identifier of the restore point in the restore archive to restore from"),
	)
	cmd.Flags().StringVar(
		c.configurationParams,
		"config-param",
		"",
		util.GetOptionalFlagMsg(
			"Comma-separated list of NAME=VALUE pairs for configuration parameters"),
	)
	// only one of restore-point-index or restore-point-id" will be required
	cmd.MarkFlagsMutuallyExclusive("restore-point-index", "restore-point-id")
}

func (c *CmdReviveDB) CommandType() string {
	return "revive_db"
}

func (c *CmdReviveDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !c.parser.Changed("ipv6") {
		c.ipv6 = nil
	}

	return c.validateParse(logger)
}

func (c *CmdReviveDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	// check the format of configuration params string, and parse it into configParams
	configurationParams, err := util.ParseConfigParams(*c.configurationParams)
	if err != nil {
		return err
	}
	if configurationParams != nil {
		c.reviveDBOptions.ConfigurationParameters = configurationParams
	}

	// when --display-only is provided, we do not need to parse some base options like hostListStr
	if c.reviveDBOptions.DisplayOnly {
		return nil
	}

	return c.ValidateParseBaseOptions(&c.reviveDBOptions.DatabaseOptions)
}

func (c *CmdReviveDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")
	dbInfo, vdb, err := vcc.VReviveDatabase(c.reviveDBOptions)
	if err != nil {
		vcc.Log.Error(err, "fail to revive database", "DBName", *c.reviveDBOptions.DBName)
		return err
	}

	if c.reviveDBOptions.DisplayOnly {
		vcc.Log.PrintInfo("database details:\n%s", dbInfo)
		return nil
	}

	err = vdb.WriteClusterConfig(c.reviveDBOptions.ConfigPath, vcc.Log)
	if err != nil {
		vcc.Log.PrintWarning("fail to write config file, details: %s", err)
	}

	vcc.Log.PrintInfo("Successfully revived database %s", *c.reviveDBOptions.DBName)

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdReviveDB
func (c *CmdReviveDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.reviveDBOptions.DatabaseOptions = *opt
}
