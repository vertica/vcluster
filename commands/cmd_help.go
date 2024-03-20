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

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdHelp
 *
 * A command providing top-level help on
 * various topics. PrintUsage() will print
 * the requested help.
 *
 * Implements ClusterCommand interface
 */
type CmdHelp struct {
	topic *string

	CmdBase
}

func makeCmdHelp() *CmdHelp {
	newCmd := &CmdHelp{}
	newCmd.oldParser = flag.NewFlagSet("help", flag.ExitOnError)
	newCmd.topic = newCmd.oldParser.String("topic", "", "The topic for more help")
	return newCmd
}

func (c *CmdHelp) CommandType() string {
	return "help"
}

func (c *CmdHelp) Parse(inputArgv []string, logger vlog.Printer) error {
	logger.LogArgParse(&inputArgv)

	if c.oldParser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ParseArgv()
	if err != nil {
		return err
	}

	return c.validateParse(logger)
}

func (c *CmdHelp) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	return nil
}

func (c *CmdHelp) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdHelp) Run(_ vclusterops.ClusterCommands) error {
	return nil
}
