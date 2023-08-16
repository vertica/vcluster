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

import "github.com/go-logr/logr"

type ClusterCommand interface {
	CommandType() string
	Parse(argv []string) error

	/* TODO: Analyze information about the state of
	 * the cluster. The information could be
	 * cached in a config file or constructed through
	 * cluster discovery.
	 */
	Analyze() error
	Run(log logr.Logger) error
	PrintUsage(string)
}
