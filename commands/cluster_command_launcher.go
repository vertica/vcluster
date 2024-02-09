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

	"github.com/vertica/vcluster/vclusterops"
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
 * vcluster create_db --db-name db1
 *
 *
 */
const minArgs = 2
const helpString = "help"
const defaultLogPath = "/opt/vertica/log/vcluster.log"

/* ClusterCommandLauncherFactory()
 * Returns a new instance of a ClusterCommandLauncher
 * with some reasonable defaults.
 */
func MakeClusterCommandLauncher() (ClusterCommandLauncher, vclusterops.VClusterCommands) {
	// setup logs for command launcher initialization
	userCommandString := os.Args[1]
	logger := vlog.Printer{ForCli: true}
	logPath := parseLogPathArg(os.Args, defaultLogPath)
	logger.SetupOrDie(logPath)
	vcc := vclusterops.VClusterCommands{
		Log: logger.WithName(userCommandString),
	}
	vcc.Log.Info("New vcluster command initialization")
	newLauncher := ClusterCommandLauncher{}
	allCommands := constructCmds(vcc.Log)

	newLauncher.commands = map[string]ClusterCommand{}
	for _, c := range allCommands {
		_, existsInMap := newLauncher.commands[c.CommandType()]
		if existsInMap {
			// shout loud if there's a programmer error
			vcc.Log.PrintError("Programmer Error: tried to add command %s to the commands index twice. Check cluster_command_launcher.go",
				c.CommandType())
			os.Exit(1)
		}
		newLauncher.commands[c.CommandType()] = c
	}

	return newLauncher, vcc
}

// constructCmds returns a list of commands that will be executed
// by the cluster command launcher.
func constructCmds(_ vlog.Printer) []ClusterCommand {
	return []ClusterCommand{
		// db-scope cmds
		makeCmdCreateDB(),
		makeCmdStartDB(),
		makeCmdStopDB(),
		makeCmdDropDB(),
		makeListAllNodes(),
		makeCmdReIP(),
		makeCmdReviveDB(),
		makeCmdShowRestorePoints(),
		makeCmdInstallPackages(),
		// sc-scope cmds
		makeCmdAddSubcluster(),
		makeCmdRemoveSubcluster(),
		makeCmdSandboxSubcluster(),
		makeCmdUnsandboxSubcluster(),
		// node-scope cmds
		makeCmdAddNode(),
		makeCmdRemoveNode(),
		makeCmdRestartNodes(),
		// others
		makeCmdScrutinize(),
		makeCmdHelp(),
		makeCmdInit(),
		makeCmdConfig(),
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
 *     + Returns any errors to the caller after writing the error to the log
 */
func (c ClusterCommandLauncher) Run(inputArgv []string, vcc vclusterops.VClusterCommands) error {
	userCommandString := os.Args[1]
	c.argv = inputArgv
	minArgsError := checkMinimumInput(c.argv)

	if minArgsError != nil {
		vcc.Log.Error(minArgsError, "fail to check minimum argument")
		return minArgsError
	}

	subCommand, idError := identifySubcommand(c.commands, userCommandString, vcc.Log)

	if idError != nil {
		vcc.Log.Error(idError, "fail to recognize command")
		return idError
	}

	parseError := subCommand.Parse(inputArgv[2:], vcc.Log)
	if parseError != nil {
		vcc.Log.Error(parseError, "fail to parse command")
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
	analyzeError := subCommand.Analyze(vcc.Log)

	if analyzeError != nil {
		vcc.Log.Error(analyzeError, "fail to analyze command")
		return analyzeError
	}

	runError := subCommand.Run(vcc)
	if runError != nil {
		vcc.Log.Error(runError, "fail to run command")
	}
	return runError
}

func identifySubcommand(commands map[string]ClusterCommand, userCommandString string,
	logger vlog.Printer) (ClusterCommand, error) {
	command, ok := commands[userCommandString]
	if !ok {
		return nil, fmt.Errorf("unrecognized command '%s'",
			userCommandString)
	}

	logger.Log.Info("Recognized command", "cmd", userCommandString)
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

func parseLogPathArg(argInput []string, defaultPath string) (logPath string) {
	for idx, arg := range argInput {
		if arg == "--log-path" {
			return argInput[idx+1]
		}
	}
	return defaultPath
}
