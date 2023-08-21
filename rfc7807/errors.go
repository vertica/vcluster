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

package rfc7807

// List of all known RFC 7807 problems that vcluster may see. All are exported
// from the package so they can be used by the NMA, vcluster, etc.
//
// Treat each problem's type (URL reference) as immutable. They should never be
// changed across server releases. And they should never be reused as it is used to
// uniquely identify the problem that is hit.
//
// In general, the title should be constant too. The only time we may want to
// relax that is if they are changed for localization purposes.
var (
	GenericBootstrapCatalogFailure = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/internal-bootstrap-catalog-failure",
		"Internal error while bootstraping the catalog",
	)
	CommunalStorageNotEmpty = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/communal-storage-not-empty",
		"Communal storage is not empty",
	)
	CommunalStoragePathInvalid = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/communal-storage-path-invalid",
		"Communal storage is not a valid path for the file system",
	)
	CommunalRWAccessError = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/communal-read-write-access-error",
		"Failed while testing read/write access to the communal storage",
	)
	CommunalAccessError = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/communal-access-error",
		"Error accessing communal storage",
	)
	GenericLicenseCheckFailure = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/internal-license-check-failure",
		"Internal error while checking license file",
	)
	WrongRequestMethod = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/wrong-request-method",
		"Wrong request method used",
	)
	BadRequest = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/bad-request",
		"Bad request sent",
	)
	GenericGetNodeInfoFailure = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/internal-get-node-info-failure",
		"Internal error while getting node information",
	)
	GenericLoadRemoteCatalogFailure = newProblemID(
		"https://integrators.vertica.com/vcluster/errors/internal-load-remote-catalog-failure",
		"Internal error while loading remote catalog",
	)
)
