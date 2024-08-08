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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdCheckVClusterServerPid
 *
 * Implements ClusterCommand interface
 */
type CmdCheckVClusterServerPid struct {
	checkPidOptions *vclusterops.VCheckVClusterServerPidOptions

	CmdBase
}

func makeCmdCheckVClusterServerPid() *cobra.Command {
	newCmd := &CmdCheckVClusterServerPid{}
	opt := vclusterops.VCheckVClusterServerPidOptionsFactory()
	newCmd.checkPidOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		"check_pid",
		"Check VCluster server PID files",
		`Check VCluster server PID files in nodes to make sure that 
only one host is running the VCluster server`,
		[]string{hostsFlag},
	)

	return cmd
}

func (c *CmdCheckVClusterServerPid) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	return c.validateParse(logger)
}

func (c *CmdCheckVClusterServerPid) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.checkPidOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}
	return c.ValidateParseBaseOptions(&c.checkPidOptions.DatabaseOptions)
}

func (c *CmdCheckVClusterServerPid) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	hostsWithVclusterServerPid, err := vcc.VCheckVClusterServerPid(c.checkPidOptions)
	if err != nil {
		vcc.LogError(err, "failed to drop the database")
		return err
	}
	fmt.Printf("Hosts with VCluster server PID files: %+v\n", hostsWithVclusterServerPid)

	return nil
}

func (c *CmdCheckVClusterServerPid) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.checkPidOptions.DatabaseOptions = *opt
}
