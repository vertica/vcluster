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

func init() {
	// move below memory allocation code to setDefaultValues() in
	// vcluster_database_options.go in VER-92369
	// allocate memory to Password and LogPath for flags parse
	dbOptions.Password = new(string)
	dbOptions.LogPath = new(string)

	// set the log path depending on executable path
	setLogPath()

	allCommands := constructCmds()
	for _, c := range allCommands {
		rootCmd.AddCommand(c)
	}
}
