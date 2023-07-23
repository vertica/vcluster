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
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdInit
 *
 * A command creating the YAML config file "vertica_cluster.yaml"
 * under the current or a specified directory.
 *
 * Implements ClusterCommand interface
 */
type CmdInit struct {
	Hosts *string
	ConfigHandler
}

func MakeCmdInit() *CmdInit {
	newCmd := &CmdInit{}
	newCmd.parser = flag.NewFlagSet("init", flag.ExitOnError)
	newCmd.directory = newCmd.parser.String(
		"directory",
		"",
		"The directory under which the config file will be created. "+
			"By default the current directory will be used.",
	)
	newCmd.Hosts = newCmd.parser.String("hosts", "", "Comma-separated list of hosts to participate in database")
	return newCmd
}

func (c *CmdInit) CommandType() string {
	return "init"
}

func (c *CmdInit) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ParseArgv()
	if err != nil {
		return err
	}

	return c.validateParse()
}

func (c *CmdInit) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// if directory is not provided, then use the current directory
	err := c.validateDirectory()
	if err != nil {
		return err
	}

	// the host list must be provided
	if *c.Hosts == "" {
		return fmt.Errorf("must provide the host list with --hosts")
	}

	return nil
}

func (c *CmdInit) Analyze() error {
	return nil
}

func (c *CmdInit) Run() error {
	configFilePath := filepath.Join(*c.directory, vclusterops.ConfigFileName)

	// check config file existence
	_, e := os.Stat(configFilePath)
	if e == nil {
		errMsg := fmt.Sprintf("The config file %s already exists", configFilePath)
		vlog.LogPrintErrorln(errMsg)
		return errors.New(errMsg)
	}

	// TODO: this will be improved later with more cluster info
	// build cluster config information
	clusterConfig := vclusterops.MakeClusterConfig()
	hosts, err := util.SplitHosts(*c.Hosts)
	if err != nil {
		return err
	}
	clusterConfig.Hosts = hosts

	// write information to the YAML file
	err = clusterConfig.WriteConfig(configFilePath)
	if err != nil {
		return err
	}

	vlog.LogPrintInfo("Created config file at %s\n", configFilePath)

	return nil
}
