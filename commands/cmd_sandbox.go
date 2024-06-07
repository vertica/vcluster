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
	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdSandbox
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for sandbox operation.
 * Prepares the inputs for the library.
 *
 */

type CmdSandboxSubcluster struct {
	CmdBase
	sbOptions vclusterops.VSandboxOptions
}

func (c *CmdSandboxSubcluster) TypeName() string {
	return "CmdSandboxSubcluster"
}

func makeCmdSandboxSubcluster() *cobra.Command {
	// CmdSandboxSubcluster
	newCmd := &CmdSandboxSubcluster{}
	opt := vclusterops.VSandboxOptionsFactory()
	newCmd.sbOptions = opt

	cmd := makeBasicCobraCmd(
		newCmd,
		sandboxSubCmd,
		"Sandbox a subcluster",
		`This command sandboxes a subcluster in an existing Eon Mode database.

Only secondary subclusters can be sandboxed. All hosts in the subcluster that
you want to sandbox must be up.

When you sandbox a subcluster, its hosts shut down and restart as part of the
sandbox. A sandbox can contain multiple subclusters.

You must provide the subcluster name with the --subcluster option and the
sandbox name with the --sandbox option.
		
Examples:
  # Sandbox a subcluster with config file
  vcluster sandbox_subcluster --subcluster sc1 --sandbox sand \
    --config /opt/vertica/config/vertica_cluster.yaml

  # Sandbox a subcluster with user input
  vcluster sandbox_subcluster --subcluster sc1 --sandbox sand \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 --db-name test_db
`,
		[]string{dbNameFlag, configFlag, hostsFlag, ipv6Flag, passwordFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// require name of subcluster to sandbox as well as the sandbox name
	markFlagsRequired(cmd, subclusterFlag, sandboxFlag)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdSandboxSubcluster) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.sbOptions.SCName,
		subclusterFlag,
		"",
		"The name of the subcluster to be sandboxed",
	)
	cmd.Flags().StringVar(
		&c.sbOptions.SandboxName,
		sandboxFlag,
		"",
		"The name of the sandbox",
	)
	cmd.Flags().BoolVar(
		&c.sbOptions.SaveRp,
		saveRpFlag,
		false,
		"A restore point is saved when creating the sandbox",
	)
	cmd.Flags().BoolVar(
		&c.sbOptions.Imeta,
		isolateMetadataFlag,
		false,
		"The metadata of sandboxed subcluster is isolated",
	)
	cmd.Flags().BoolVar(
		&c.sbOptions.Sls,
		createStorageLocationsFlag,
		false,
		"The sandbox create its own storage locations",
	)
}

func (c *CmdSandboxSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	return c.parseInternal(logger)
}

func (c *CmdSandboxSubcluster) parseInternal(logger vlog.Printer) error {
	logger.Info("Called parseInternal()")

	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.sbOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

	err := c.ValidateParseBaseOptions(&c.sbOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.sbOptions.DatabaseOptions)
}

func (c *CmdSandboxSubcluster) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdSandboxSubcluster) Run(vcc vclusterops.ClusterCommands) error {
	vcc.LogInfo("Calling method Run() for command " + sandboxSubCmd)

	options := c.sbOptions

	err := vcc.VSandbox(&options)
	if err != nil {
		vcc.LogError(err, "fail to sandbox subcluster")
		return err
	}

	defer vcc.DisplayInfo("Successfully sandboxed subcluster " + c.sbOptions.SCName + " as " + c.sbOptions.SandboxName)
	// Read and then update the sandbox information on config file
	dbConfig, configErr := readConfig()
	if configErr != nil {
		vcc.DisplayWarning("fail to read config file, skipping config file update", "error", configErr)
		return nil
	}
	// Update config
	updatedConfig := c.updateSandboxInfo(dbConfig)
	if !updatedConfig {
		vcc.DisplayWarning("did not update node info for sandboxed sc " + c.sbOptions.SCName +
			", info about the subcluster nodes are missing in config file, skipping config update")
		return nil
	}

	writeErr := dbConfig.write(options.ConfigPath, true /*forceOverwrite*/)
	if writeErr != nil {
		vcc.DisplayWarning("fail to write the config file, details: " + writeErr.Error())
		return nil
	}
	return nil
}

// updateSandboxInfo will update sandbox info for the sandboxed subcluster in the config object
// returns true if the info are updated, returns false if no info is updated
func (c *CmdSandboxSubcluster) updateSandboxInfo(dbConfig *DatabaseConfig) bool {
	needToUpdate := false
	for _, n := range dbConfig.Nodes {
		if c.sbOptions.SCName == n.Subcluster {
			n.Sandbox = c.sbOptions.SandboxName
			needToUpdate = true
		}
	}
	return needToUpdate
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdSandboxSubcluster
func (c *CmdSandboxSubcluster) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.sbOptions.DatabaseOptions = *opt
}
