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
	"os"
	"path/filepath"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdConfig
 *
 * A command managing the YAML config file "vertica_cluster.yaml"
 * under the current or a specified directory.
 *
 * Implements ClusterCommand interface
 */
type CmdConfig struct {
	show *bool
	ConfigHandler
}

func MakeCmdConfig() *CmdConfig {
	newCmd := &CmdConfig{}
	newCmd.parser = flag.NewFlagSet("config", flag.ExitOnError)
	newCmd.show = newCmd.parser.Bool("show", false, "show the content of the config file")
	newCmd.directory = newCmd.parser.String(
		"directory",
		"",
		"The directory under which the config file was created. "+
			"By default the current directory will be used.",
	)

	return newCmd
}

func (c *CmdConfig) CommandType() string {
	return "config"
}

func (c *CmdConfig) Parse(inputArgv []string) error {
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

func (c *CmdConfig) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// if directory is not provided, then use the current directory
	return c.validateDirectory()
}

func (c *CmdConfig) Analyze() error {
	return nil
}

func (c *CmdConfig) Run() error {
	if *c.show {
		configFilePath := filepath.Join(*c.directory, vclusterops.ConfigFileName)
		fileBytes, err := os.ReadFile(configFilePath)
		if err != nil {
			return fmt.Errorf("fail to read config file, details: %w", err)
		}
		vlog.LogPrintInfo("Content of the config file:\n%s", string(fileBytes))
	}

	return nil
}
