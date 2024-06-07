/*
 (c) Copyright [2023-2024] Open Text.
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
	reviveDBOptions *vclusterops.VReviveDatabaseOptions
}

func makeCmdReviveDB() *cobra.Command {
	// CmdReviveDB
	newCmd := &CmdReviveDB{}
	opt := vclusterops.VReviveDBOptionsFactory()
	newCmd.reviveDBOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		reviveDBSubCmd,
		"Revive a database",
		`This command revives an Eon Mode database on the specified hosts or restores
an Eon Mode database to the specified restore point.

The --communal-storage-location option is required. If access to communal
storage requires access keys, provide the keys with the --config-param option.

The number of hosts that you provide to the --hosts option must match the
number of hosts in the existing database. You can omit the hosts only if
--display-only is specified.

The name of the database must be provided.

To restore a database to a restore point, you must provide the
--restore-point-archive option, and specify the restore point with either the
--restore-point-index or --restore-point-id option.

Examples:
  # Revive a database with user input and save the generated config file
  # under the given directory
  vcluster revive_db --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --communal-storage-location /communal \
    --config /opt/vertica/config/vertica_cluster.yaml

  # Describe the database only when reviving the database
  vcluster revive_db --db-name test_db --communal-storage-location /communal \
    --display-only

  # Revive a database with user input by restoring to a given restore point
  vcluster revive_db --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
    --communal-storage-location /communal \
    --config /opt/vertica/config/vertica_cluster.yaml --force-removal \
    --ignore-cluster-lease --restore-point-archive db --restore-point-index 1

`,
		[]string{dbNameFlag, hostsFlag, ipv6Flag, communalStorageLocationFlag, configFlag, outputFileFlag, configParamFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// require db-name and communal-storage-location
	markFlagsRequired(cmd, dbNameFlag, communalStorageLocationFlag)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdReviveDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().UintVar(
		&c.reviveDBOptions.LoadCatalogTimeout,
		"load-catalog-timeout",
		util.DefaultLoadCatalogTimeoutSeconds,
		"Set a timeout (in seconds) for loading remote catalog operation, default timeout is "+
			strconv.Itoa(util.DefaultLoadCatalogTimeoutSeconds)+"seconds",
	)
	cmd.Flags().BoolVar(
		&c.reviveDBOptions.ForceRemoval,
		"force-removal",
		false,
		"Prior to reviving a database, ensure the deletion of pre-existing database directories "+
			"(excluding user storage directories)",
	)
	cmd.Flags().BoolVar(
		&c.reviveDBOptions.DisplayOnly,
		"display-only",
		false,
		"Describe the database on communal storage, and exit",
	)
	cmd.Flags().BoolVar(
		&c.reviveDBOptions.IgnoreClusterLease,
		"ignore-cluster-lease",
		false,
		"Disable the check for the existence of other clusters running on the shared storage, "+
			"but be cautious with this action, as it may lead to data corruption",
	)
	cmd.Flags().StringVar(
		&c.reviveDBOptions.RestorePoint.Archive,
		"restore-point-archive",
		"",
		"Name of the restore archive to use for bootstrapping",
	)
	cmd.Flags().IntVar(
		&c.reviveDBOptions.RestorePoint.Index,
		"restore-point-index",
		0,
		"The (1-based) index of the restore point in the restore archive to restore from",
	)
	cmd.Flags().StringVar(
		&c.reviveDBOptions.RestorePoint.ID,
		"restore-point-id",
		"",
		"The identifier of the restore point in the restore archive to restore from",
	)
	// only one of restore-point-index or restore-point-id" will be required
	cmd.MarkFlagsMutuallyExclusive("restore-point-index", "restore-point-id")
}

func (c *CmdReviveDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	return c.validateParse(logger)
}

func (c *CmdReviveDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.reviveDBOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

	// when --display-only is provided, we do not need to parse some base options like hostListStr
	if c.reviveDBOptions.DisplayOnly {
		return nil
	}

	err := c.ValidateParseBaseOptions(&c.reviveDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.setConfigParam(&c.reviveDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return nil
}

func (c *CmdReviveDB) Run(vcc vclusterops.ClusterCommands) error {
	vcc.LogInfo("Called method Run()")
	dbInfo, vdb, err := vcc.VReviveDatabase(c.reviveDBOptions)
	if err != nil {
		vcc.LogError(err, "fail to revive database", "DBName", c.reviveDBOptions.DBName)
		return err
	}

	if c.reviveDBOptions.DisplayOnly {
		c.writeCmdOutputToFile(globals.file, []byte(dbInfo), vcc.GetLog())
		vcc.LogInfo("database details: ", "db-info", dbInfo)
		return nil
	}

	vcc.DisplayInfo("Successfully revived database %s", c.reviveDBOptions.DBName)

	// write db info to vcluster config file
	vdb.FirstStartAfterRevive = true
	err = writeConfig(vdb, true /*forceOverwrite*/)
	if err != nil {
		vcc.DisplayWarning("fail to write config file, details: %s", err)
	}

	// write config parameters to vcluster config param file
	err = c.writeConfigParam(c.reviveDBOptions.ConfigurationParameters, true /*forceOverwrite*/)
	if err != nil {
		vcc.DisplayWarning("fail to write config param file, details: %s", err)
	}
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdReviveDB
func (c *CmdReviveDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.reviveDBOptions.DatabaseOptions = *opt
}
