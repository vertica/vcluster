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

package main

import (
	"fmt"
	"os"

	"github.com/vertica/vcluster/commands"
	"github.com/vertica/vcluster/vclusterops/util"
)

func main() {
	// only keep commands.Execute() in main() in VER-92222
	var cobraSupportedCmdAndFlags = []string{"-h", "--help", "completion", "stop_db", "create_db"}
	if len(os.Args) == 1 || util.StringInArray(os.Args[1], cobraSupportedCmdAndFlags) {
		commands.Execute()
	} else {
		launcher, vcc := commands.MakeClusterCommandLauncher()
		runError := launcher.Run(os.Args, vcc)
		if runError != nil {
			fmt.Printf("Error during execution: %s\n", runError)
			os.Exit(1)
		}
	}
}
