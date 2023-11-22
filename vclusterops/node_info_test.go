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

package vclusterops

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindHosts(t *testing.T) {
	var nodesInfo NodesInfo

	for i := 1; i <= 3; i++ {
		var n NodeInfo
		n.Address = fmt.Sprintf("vnode%d", i)
		nodesInfo.NodeList = append(nodesInfo.NodeList, n)
	}

	// positive case: single input
	found := nodesInfo.findHosts([]string{"vnode3"})
	assert.True(t, found)

	// positive case: input multiple hosts
	found = nodesInfo.findHosts([]string{"vnode3", "vnode4"})
	assert.True(t, found)

	// negative case
	found = nodesInfo.findHosts([]string{"vnode4"})
	assert.False(t, found)

	// negative case: input multiple hosts
	found = nodesInfo.findHosts([]string{"vnode4", "vnode5"})
	assert.False(t, found)

	// negative case: empty input
	found = nodesInfo.findHosts([]string{})
	assert.False(t, found)
}
