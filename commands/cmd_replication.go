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
)

func makeCmdReplication() *cobra.Command {
	cmd := makeSimpleCobraCmd(
		replicationSubCmd,
		"Manages database replication.",
		`Starts database replication or displays the status of an
on-going replication operation.`)

	cmd.AddCommand(makeCmdStartReplication())
	cmd.AddCommand(makeCmdGetReplicationStatus())
	return cmd
}
