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
	"fmt"
	"os"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* ClusterCommandLauncher
 * A class that encapsulates the data and steps
 * of a CLI program for run cluster operations. Similar
 * to commandLineCtrl.py from Admintools.
 *
 * Main programs instaniate ClusterCommandLauncher with
 * makeClusterCommandLauncher. The main program calls the Run() method
 * with command line arguments. Run() returns errors.
 *
 */
type ClusterCommandLauncher struct {
	argv     []string
	commands map[string]ClusterCommand
}

/* minArgs - the minimum number of arguments
 * that the program expects.
 *
 * The command lines that we expect to parse
 * look like:
 * vcluster <subcommand> [options] [args]
 *
 * Examples:
 * vcluster help
 * vcluster create_db --name db1
 *
 *
 */
const minArgs = 2
const helpString = "help"

/* ClusterCommandLauncherFactory()
 * Returns a new instance of a ClusterCommandLauncher
 * with some reasonable defaults.
 */
func MakeClusterCommandLauncher() ClusterCommandLauncher {
	// setup logs for command launcher initialization
	logPath := vlog.ParseLogPathArg(os.Args, vlog.DefaultLogPath)
	vlog.SetupOrDie(logPath)
	vlog.LogInfoln("New vcluster command initialization")
	newLauncher := ClusterCommandLauncher{}

	allCommands := constructCmds()

	newLauncher.commands = map[string]ClusterCommand{}
	for _, c := range allCommands {
		_, existsInMap := newLauncher.commands[c.CommandType()]
		if existsInMap {
			// shout loud if there's a programmer error
			vlog.LogPrintError("Programmer Error: tried add command %s to the commands index twice. Check cluster_command_launcher.go",
				c.CommandType())
			os.Exit(1)
		}
		newLauncher.commands[c.CommandType()] = c
	}

	return newLauncher
}

// constructCmds returns a list of commands that will be executed
// by the cluster command launcher.
func constructCmds() []ClusterCommand {
	return []ClusterCommand{
		// db-scope cmds
		MakeCmdCreateDB(),
		MakeCmdStartDB(),
		MakeCmdStopDB(),
		MakeCmdDropDB(),
		MakeListAllNodes(),
		MakeCmdReIP(),
		// sc-scope cmds
		MakeCmdAddSubcluster(),
		// node-scope cmds
		MakeCmdAddNode(),
		// others
		MakeCmdHelp(),
		MakeCmdInit(),
		MakeCmdConfig(),
	}
}

/* Run is expected be called by a CLI program
 * Run executes the following algorithm:
 *     + Identifies the appropriate sub-command
 *     + Asks the sub-command to parse the command line
 *     + Loads the configuration file using the vconfig library. (not implemented)
 *     + Asks the sub-command to analyze the command line given the
 *       context from the config file.
 *     + Calls Run() for the sub-command
 *     + Returns any errors to the caller
 */
func (c ClusterCommandLauncher) Run(inputArgv []string) error {
	c.argv = inputArgv
	minArgsError := checkMinimumInput(c.argv)

	if minArgsError != nil {
		return minArgsError
	}

	subCommand, idError := identifySubcommand(c.argv, c.commands)
	if idError != nil {
		return idError
	}

	parseError := subCommand.Parse(inputArgv[2:])
	if parseError != nil {
		return parseError
	}

	/* Special case: help
	 * If the user asked for help, handle that now
	 * and then exit. Avoid doing anything else that
	 * might fail and prevent access to help.
	 */
	if subCommand.CommandType() == helpString {
		subCommand.PrintUsage(subCommand.CommandType())
		return nil
	}

	/* TODO: this is where we would read a
	 * configuration file. Not currently implemented.
	 */
	analyzeError := subCommand.Analyze()

	if analyzeError != nil {
		return analyzeError
	}
	return subCommand.Run()
}

func identifySubcommand(inputArgv []string, commands map[string]ClusterCommand) (ClusterCommand, error) {
	userCommandString := os.Args[1]
	command, ok := commands[userCommandString]

	if !ok {
		return nil, fmt.Errorf("unrecognized command '%s'",
			userCommandString)
	}

	vlog.LogInfo("Recognized command: %s\n", userCommandString)
	return command, nil
}

func checkMinimumInput(inputArgv []string) error {
	if len(inputArgv) >= minArgs {
		return nil
	}
	return fmt.Errorf("expected at least %d arguments but found only %d",
		minArgs,
		len(inputArgv))
}
