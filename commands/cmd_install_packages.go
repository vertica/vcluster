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

/* CmdInstallPackages
 *
 * Parses arguments for VInstallPackagesOptions to pass down to
 * VInstallPackages.
 *
 * Implements ClusterCommand interface
 */

type CmdInstallPackages struct {
	CmdBase
	installPkgOpts *vclusterops.VInstallPackagesOptions
}

func makeCmdInstallPackages() *CmdInstallPackages {
	newCmd := &CmdInstallPackages{}

	// parser, used to parse command-line flags
	newCmd.oldParser = flag.NewFlagSet("install_packages", flag.ExitOnError)
	installPkgOpts := vclusterops.VInstallPackagesOptionsFactory()

	// required flags
	installPkgOpts.DBName = newCmd.oldParser.String("db-name", "", "The name of the database to install packages in")

	// optional flags
	installPkgOpts.Password = newCmd.oldParser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", util.GetOptionalFlagMsg("Comma-separated list of hosts in database."))
	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, util.GetOptionalFlagMsg("Used to specify the hosts are IPv6 hosts"))
	installPkgOpts.HonorUserInput = newCmd.oldParser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	newCmd.oldParser.StringVar(&installPkgOpts.ConfigPath, "config", "", util.GetOptionalFlagMsg("Path to the config file"))
	installPkgOpts.ForceReinstall = newCmd.oldParser.Bool("force-reinstall", false,
		util.GetOptionalFlagMsg("Install the packages, even if they are already installed."))

	newCmd.installPkgOpts = &installPkgOpts

	newCmd.oldParser.Usage = func() {
		util.SetParserUsage(newCmd.oldParser, "install_packages")
	}

	return newCmd
}

func (c *CmdInstallPackages) CommandType() string {
	return "install_packages"
}

func (c *CmdInstallPackages) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.oldParser, "password") {
		c.installPkgOpts.Password = nil
	}
	if !util.IsOptionSet(c.oldParser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdInstallPackages) validateParse() error {
	return c.OldValidateParseBaseOptions(&c.installPkgOpts.DatabaseOptions)
}

func (c *CmdInstallPackages) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdInstallPackages) Run(vcc vclusterops.VClusterCommands) error {
	options := c.installPkgOpts

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	var status *vclusterops.InstallPackageStatus
	status, err = vcc.VInstallPackages(options)
	if err != nil {
		vcc.Log.Error(err, "failed to install the packages")
		return err
	}

	vcc.Log.PrintInfo("Installed the packages:\n%v", status.Packages)
	return nil
}
