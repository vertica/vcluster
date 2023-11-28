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
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	// Environment variable names storing name of k8s secret that has NMA cert
	secretNameSpaceEnvVar = "NMA_SECRET_NAMESPACE"
	secretNameEnvVar      = "NMA_SECRET_NAME"

	// Environment variable names for locating the NMA certs located in the file system
	nmaRootCAPathEnvVar = "NMA_ROOTCA_PATH"
	nmaCertPathEnvVar   = "NMA_CERT_PATH"
	nmaKeyPathEnvVar    = "NMA_KEY_PATH"
)

const (
	kubernetesPort  = "KUBERNETES_PORT"
	databaseName    = "DATABASE_NAME"
	catalogPathPref = "CATALOG_PATH"
)

// secretRetriever is an interface for retrieving secrets.
type secretRetriever interface {
	RetrieveSecret(namespace, secretName string) ([]byte, []byte, []byte, error)
}

// k8sSecretRetrieverStruct is an implementation of secretRetriever.
type k8sSecretRetrieverStruct struct {
}

/* CmdScrutinize
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for scrutinize operation.
 * Prepares the inputs for the library.
 *
 */
type CmdScrutinize struct {
	CmdBase
	k8secretRetreiver secretRetriever
	sOptions          vclusterops.VScrutinizeOptions
}

func makeCmdScrutinize() *CmdScrutinize {
	newCmd := &CmdScrutinize{}
	newCmd.parser = flag.NewFlagSet("scrutinize", flag.ExitOnError)
	newCmd.sOptions = vclusterops.VScrutinizOptionsFactory()
	newCmd.k8secretRetreiver = k8sSecretRetrieverStruct{}
	// required flags
	newCmd.sOptions.DBName = newCmd.parser.String("db-name", "", "The name of the database to run scrutinize.  May be omitted on k8s.")

	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated host list")

	// optional flags
	newCmd.sOptions.Password = newCmd.parser.String("password", "",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))

	newCmd.sOptions.UserName = newCmd.parser.String("db-user", "",
		util.GetOptionalFlagMsg("Database username. Consider using single quotes to avoid shell substitution."))

	newCmd.sOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))

	newCmd.sOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Scrutinize database with IPv6 hosts"))

	return newCmd
}

func (c *CmdScrutinize) CommandType() string {
	return vclusterops.VScrutinizeTypeName
}

func (c *CmdScrutinize) Parse(inputArgv []string, logger vlog.Printer) error {
	logger.PrintInfo("Parsing scrutinize command input")
	c.argv = inputArgv
	err := c.ValidateParseMaskedArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.sOptions.Password = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.sOptions.ConfigDirectory = nil
	}
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}
	// just so generic parsing works - not relevant for functionality
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.CmdBase.isEon = nil
	}

	// parses host list and ipv6 - eon is irrelevant but handled
	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdScrutinize) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	return c.ValidateParseBaseOptions(&c.sOptions.DatabaseOptions)
}

func (c *CmdScrutinize) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")

	// Read the NMA certs into the options struct
	err := c.readNMACerts(logger)
	if err != nil {
		return err
	}

	var allErrs error
	port, found := os.LookupEnv(kubernetesPort)
	if found && port != "" && *c.sOptions.HonorUserInput {
		logger.Info(kubernetesPort, " is set, k8s environment detected", found)
		dbName, found := os.LookupEnv(databaseName)
		if !found || dbName == "" {
			allErrs = errors.Join(allErrs, fmt.Errorf("unable to get database name from environment variable. "))
		} else {
			c.sOptions.DBName = &dbName
			logger.Info("Setting database name from env as", "DBName", *c.sOptions.DBName)
		}

		catPrefix, found := os.LookupEnv(catalogPathPref)
		if !found || catPrefix == "" {
			allErrs = errors.Join(allErrs, fmt.Errorf("unable to get catalog path from environment variable. "))
		} else {
			c.sOptions.CatalogPrefix = &catPrefix
			logger.Info("Setting catalog path from env as", "CatalogPrefix", *c.sOptions.CatalogPrefix)
		}
		if allErrs != nil {
			return allErrs
		}
	}
	return nil
}

func (c *CmdScrutinize) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.PrintInfo("Running scrutinize") // TODO remove when no longer needed for tests
	vcc.Log.Info("Calling method Run()")

	// get config from vertica_cluster.yaml (if exists)
	config, err := c.sOptions.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	c.sOptions.Config = config

	err = vcc.VScrutinize(&c.sOptions)
	if err != nil {
		vcc.Log.Error(err, "scrutinize run failed")
		return err
	}
	vcc.Log.PrintInfo("Successfully completed scrutinize run for the database %s", *c.sOptions.DBName)
	return err
}

// RetrieveSecret retrieves a secret from Kubernetes and returns its data.
func (k8sSecretRetrieverStruct) RetrieveSecret(namespace, secretName string) (ca, cert, key []byte, err error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, nil, err
	}
	secretsClient := clientset.CoreV1().Secrets(namespace)
	certData, err := secretsClient.Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, err
	}
	const (
		CACertName  = "ca.crt"
		TLSCertName = "tls.crt"
		TLSKeyName  = "tls.key"
	)
	caCertVal, exists := certData.Data[CACertName]
	if !exists {
		return nil, nil, nil, fmt.Errorf("missing key %s in secret", CACertName)
	}
	tlsCertVal, exists := certData.Data[TLSCertName]
	if !exists {
		return nil, nil, nil, fmt.Errorf("missing key %s in secret", TLSCertName)
	}
	tlsKeyVal, exists := certData.Data[TLSKeyName]
	if !exists {
		return nil, nil, nil, fmt.Errorf("missing key %s in secret", TLSKeyName)
	}
	return caCertVal, tlsCertVal, tlsKeyVal, nil
}

func (c *CmdScrutinize) readNMACerts(logger vlog.Printer) error {
	loaderFuncs := []func(vlog.Printer) (bool, error){
		c.nmaCertLookupFromK8sSecret,
		c.nmaCertLookupFromEnv,
	}
	for _, fnc := range loaderFuncs {
		certsLoaded, err := fnc(logger)
		if err != nil || certsLoaded {
			return err
		}
	}
	logger.Info("failed to retrieve the NMA certs from any source")
	return nil
}

// nmaCertLookupFromK8sSecret retrieves PEM-encoded text of CA certs, the server cert, and
// the server key directly from kubernetes secrets.
func (c *CmdScrutinize) nmaCertLookupFromK8sSecret(logger vlog.Printer) (bool, error) {
	_, portSet := os.LookupEnv(kubernetesPort)
	if !portSet {
		return false, nil
	}
	logger.Info("K8s environment")
	secretNameSpace, nameSpaceSet := os.LookupEnv(secretNameSpaceEnvVar)
	secretName, nameSet := os.LookupEnv(secretNameEnvVar)

	// either secret namespace/name must be set, or none at all
	if !((nameSpaceSet && nameSet) || (!nameSpaceSet && !nameSet)) {
		missingParamError := constructMissingParamsMsg([]bool{nameSpaceSet, nameSet},
			[]string{secretNameSpaceEnvVar, secretNameEnvVar})
		return false, fmt.Errorf("all or none of the environment variables %s and %s must be set. %s",
			secretNameSpaceEnvVar, secretNameEnvVar, missingParamError)
	}

	if !nameSpaceSet {
		logger.Info("Secret name not set in env. Failback to other cert retieval methods.")
		return false, nil
	}

	caCert, cert, key, err := c.k8secretRetreiver.RetrieveSecret(secretNameSpace, secretName)
	if err != nil {
		return false, fmt.Errorf("failed to read certs from k8s secret %s in namespace %s: %w", secretName, secretNameSpace, err)
	}
	if len(caCert) != 0 && len(cert) != 0 && len(key) != 0 {
		logger.Info("Successfully read cert from k8s secret ", "secretName", secretName, "secretNameSpace", secretNameSpace)
	} else {
		return false, fmt.Errorf("failed to read CA, cert or key (sizes = %d/%d/%d)",
			len(caCert), len(cert), len(key))
	}
	c.sOptions.CaCert = string(caCert)
	c.sOptions.Cert = string(cert)
	c.sOptions.Key = string(key)

	return true, nil
}

// nmaCertLookupFromEnv retrieves the NMA certs from plaintext file identified
// by an environment variable.
func (c *CmdScrutinize) nmaCertLookupFromEnv(logger vlog.Printer) (bool, error) {
	rootCAPath, rootCAPathSet := os.LookupEnv(nmaRootCAPathEnvVar)
	certPath, certPathSet := os.LookupEnv(nmaCertPathEnvVar)
	keyPath, keyPathSet := os.LookupEnv(nmaKeyPathEnvVar)

	// either all env vars are set or none at all
	if !((rootCAPathSet && certPathSet && keyPathSet) || (!rootCAPathSet && !certPathSet && !keyPathSet)) {
		missingParamError := constructMissingParamsMsg([]bool{rootCAPathSet, certPathSet, keyPathSet},
			[]string{nmaRootCAPathEnvVar, nmaCertPathEnvVar, nmaKeyPathEnvVar})
		return false, fmt.Errorf("all or none of the environment variables %s, %s and %s must be set. %s",
			nmaRootCAPathEnvVar, nmaCertPathEnvVar, nmaKeyPathEnvVar, missingParamError)
	}

	if !rootCAPathSet {
		logger.Info("NMA cert location paths not set in env")
		return false, nil
	}

	var err error

	c.sOptions.CaCert, err = readNonEmptyFile(rootCAPath)
	if err != nil {
		return false, fmt.Errorf("failed to read root CA from %s: %w", rootCAPath, err)
	}

	c.sOptions.Cert, err = readNonEmptyFile(certPath)
	if err != nil {
		return false, fmt.Errorf("failed to read cert from %s: %w", certPath, err)
	}

	c.sOptions.Key, err = readNonEmptyFile(keyPath)
	if err != nil {
		return false, fmt.Errorf("failed to read key from %s: %w", keyPath, err)
	}

	logger.Info("Successfully read certs from file", "rootCAPath", rootCAPath, "certPath", certPath, "keyPath", keyPath)
	return true, nil
}

// readNonEmptyFile is a helper that reads the contents of a file into a string.
// It returns an error if the file is empty.
func readNonEmptyFile(filename string) (string, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("failed to read from %s: %w", filename, err)
	}
	if len(contents) == 0 {
		return "", fmt.Errorf("%s is empty", filename)
	}
	return string(contents), nil
}

// constructMissingParamsMsg builds a warning string listing each
// param in params flagged as not existing in exists.  The input
// slice order is assumed to match.
func constructMissingParamsMsg(exists []bool, params []string) string {
	needComma := false
	var warningBuilder strings.Builder
	warningBuilder.WriteString("Missing parameters: ")
	for paramIndex, paramExists := range exists {
		if !paramExists {
			if needComma {
				warningBuilder.WriteString(", ")
			}
			warningBuilder.WriteString(params[paramIndex])
			needComma = true
		}
	}
	return warningBuilder.String()
}
