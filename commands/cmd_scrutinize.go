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

	"k8s.io/apimachinery/pkg/types"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"github.com/vertica/vertica-kubernetes/pkg/secrets"
)

const (
	// Environment variable names storing name of k8s secret that has NMA cert
	secretNameSpaceEnvVar = "NMA_SECRET_NAMESPACE"
	secretNameEnvVar      = "NMA_SECRET_NAME"

	// Environment variable names for locating the NMA certs located in the file system
	nmaRootCAPathEnvVar = "NMA_ROOTCA_PATH"
	nmaCertPathEnvVar   = "NMA_CERT_PATH"
	nmaKeyPathEnvVar    = "NMA_KEY_PATH"

	// Environment variable names storing name of secret that has the db password
	passwordSecretNamespaceEnvVar = "PASSWORD_SECRET_NAMESPACE"
	passwordSecretNameEnvVar      = "PASSWORD_SECRET_NAME"
)

const (
	kubernetesPort  = "KUBERNETES_PORT"
	databaseName    = "DATABASE_NAME"
	catalogPathPref = "CATALOG_PATH"
)

// secretRetriever is an interface for retrieving secrets.
type secretRetriever interface {
	RetrieveSecret(logger vlog.Printer, namespace, secretName string) (map[string][]byte, error)
}

// secretStoreRetrieverStruct is an implementation of secretRetriever. It
// handles reading secrets from k8s and external sources like GSM, AWS, etc.
type secretStoreRetrieverStruct struct {
	Log vlog.Printer
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
	secretStoreRetriever secretRetriever
	sOptions             vclusterops.VScrutinizeOptions
}

func makeCmdScrutinize() *CmdScrutinize {
	newCmd := &CmdScrutinize{}
	newCmd.oldParser = flag.NewFlagSet("scrutinize", flag.ExitOnError)
	newCmd.sOptions = vclusterops.VScrutinizOptionsFactory()
	newCmd.secretStoreRetriever = secretStoreRetrieverStruct{}
	// required flags
	newCmd.sOptions.DBName = newCmd.oldParser.String("db-name", "", "The name of the database to run scrutinize.  May be omitted on k8s.")

	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", "Comma-separated host list")

	// optional flags
	newCmd.sOptions.Password = newCmd.oldParser.String("password", "",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))

	newCmd.sOptions.UserName = newCmd.oldParser.String("db-user", "",
		util.GetOptionalFlagMsg("Database username. Consider using single quotes to avoid shell substitution."))

	newCmd.sOptions.HonorUserInput = newCmd.oldParser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))

	newCmd.oldParser.StringVar(&newCmd.sOptions.ConfigPath, "config", "", util.GetOptionalFlagMsg("Path to the config file"))

	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, util.GetOptionalFlagMsg("Scrutinize database with IPv6 hosts"))

	// this argument is parsed separately by the cluster command launcher to initialize the logger
	newCmd.sOptions.LogPath = newCmd.oldParser.String("log-path", defaultLogPath,
		util.GetOptionalFlagMsg("File path of the vcluster scrutinize log"))

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
	if !util.IsOptionSet(c.oldParser, "password") {
		c.sOptions.Password = nil
	}
	if !util.IsOptionSet(c.oldParser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}
	// just so generic parsing works - not relevant for functionality
	if !util.IsOptionSet(c.oldParser, "eon-mode") {
		c.CmdBase.isEon = nil
	}

	// parses host list and ipv6 - eon is irrelevant but handled
	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdScrutinize) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	return c.OldValidateParseBaseOptions(&c.sOptions.DatabaseOptions)
}

func (c *CmdScrutinize) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")

	// Read the password from a secret
	err := c.dbPassswdLookupFromSecretStore(logger)
	if err != nil {
		return err
	}

	// Read the NMA certs into the options struct
	err = c.readNMACerts(logger)
	if err != nil {
		return err
	}

	// get extra options direct from env variables
	err = c.readOptionsFromK8sEnv(logger)
	if err != nil {
		return err
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

// RetrieveSecret retrieves a secret from a secret store, such as Kubernetes or
// GSM, and returns its data.
func (k secretStoreRetrieverStruct) RetrieveSecret(logger vlog.Printer, namespace, secretName string) (map[string][]byte, error) {
	// We use MultiSourceSecretFetcher since it will use the correct client
	// depending on the secret path reference of the secret name. This can
	// handle reading clients from the k8s-apiserver using a k8s client, or from
	// external sources such as Google Secret Manager (GSM).
	fetcher := secrets.MultiSourceSecretFetcher{
		Log: &logger,
	}
	ctx := context.Background()
	fetchName := types.NamespacedName{
		Namespace: namespace,
		Name:      secretName,
	}
	return fetcher.Fetch(ctx, fetchName)
}

// extractNMACerts extracts the ca, cert and key from a secret data and
// set the options struct
func (c *CmdScrutinize) extractNMACerts(certData map[string][]byte) (err error) {
	const (
		CACertName  = "ca.crt"
		TLSCertName = "tls.crt"
		TLSKeyName  = "tls.key"
	)
	caCertVal, exists := certData[CACertName]
	if !exists {
		return fmt.Errorf("missing key %s in secret", CACertName)
	}
	tlsCertVal, exists := certData[TLSCertName]
	if !exists {
		return fmt.Errorf("missing key %s in secret", TLSCertName)
	}
	tlsKeyVal, exists := certData[TLSKeyName]
	if !exists {
		return fmt.Errorf("missing key %s in secret", TLSKeyName)
	}

	if len(caCertVal) == 0 || len(tlsCertVal) == 0 || len(tlsKeyVal) == 0 {
		return fmt.Errorf("failed to read CA, cert or key (sizes = %d/%d/%d)",
			len(caCertVal), len(tlsCertVal), len(tlsKeyVal))
	}

	c.sOptions.CaCert = string(caCertVal)
	c.sOptions.Cert = string(tlsCertVal)
	c.sOptions.Key = string(tlsKeyVal)
	return nil
}

// extractDBPassword extracts the password from a secret data and
// set the options struct
func (c *CmdScrutinize) extractDBPassword(pwdData map[string][]byte) (err error) {
	const passwordKey = "password"
	pwd, ok := pwdData[passwordKey]
	if !ok {
		return fmt.Errorf("password not found, secret must have a key with name %q", passwordKey)
	}
	if c.sOptions.Password == nil {
		c.sOptions.Password = new(string)
	}
	*c.sOptions.Password = string(pwd)
	return nil
}

func (c *CmdScrutinize) readNMACerts(logger vlog.Printer) error {
	loaderFuncs := []func(vlog.Printer) (bool, error){
		c.nmaCertLookupFromSecretStore,
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

// dbPassswdLookupFromSecretStore retrieves the db password directly from a secret store.
func (c *CmdScrutinize) dbPassswdLookupFromSecretStore(logger vlog.Printer) error {
	// no-op if we are not on k8s or the password has already
	// been set through another method
	if !isK8sEnvironment() || c.sOptions.Password != nil {
		return nil
	}

	secret, err := lookupAndCheckSecretEnvVars(passwordSecretNameEnvVar, passwordSecretNamespaceEnvVar)
	if secret == nil || err != nil {
		if err == nil {
			logger.Info("Password secret environment variables are not set. Password read will rely on the user input.")
		}
		return err
	}

	pwdData, err := c.secretStoreRetriever.RetrieveSecret(logger, secret.Namespace, secret.Name)
	if err != nil {
		return err
	}
	err = c.extractDBPassword(pwdData)
	if err != nil {
		return err
	}
	logger.Info("Successfully read db password from secret store", "secretName", secret.Name)
	return nil
}

// nmaCertLookupFromSecretStore retrieves PEM-encoded text of CA certs, the server cert, and
// the server key directly from a secret store.
func (c *CmdScrutinize) nmaCertLookupFromSecretStore(logger vlog.Printer) (bool, error) {
	if !isK8sEnvironment() {
		return false, nil
	}

	logger.Info("K8s environment")
	secret, err := lookupAndCheckSecretEnvVars(secretNameEnvVar, secretNameSpaceEnvVar)
	if secret == nil || err != nil {
		if err == nil {
			logger.Info("Secret name not set in env. Failback to other cert retieval methods.")
		}
		return false, err
	}

	certData, err := c.secretStoreRetriever.RetrieveSecret(logger, secret.Namespace, secret.Name)
	if err != nil {
		return false, err
	}
	err = c.extractNMACerts(certData)
	if err != nil {
		return false, err
	}
	logger.Info("Successfully read cert from secret store", "secretName", secret.Name, "secretNameSpace", secret.Namespace)
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

// readOptionsFromK8sEnv picks up the catalog path and dbname from the environment when on k8s
// which otherwise would need to be set at the command line or read from a config file.
func (c *CmdScrutinize) readOptionsFromK8sEnv(logger vlog.Printer) (allErrs error) {
	if !isK8sEnvironment() || !*c.sOptions.HonorUserInput {
		return
	}

	logger.Info("k8s environment detected")
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
	return
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

func isK8sEnvironment() bool {
	port, portSet := os.LookupEnv(kubernetesPort)
	return portSet && port != ""
}

// lookupAndCheckSecretEnvVars retrieves the values of the secret environment variables
// and checks if they are valid
func lookupAndCheckSecretEnvVars(nameEnv, namespaceEnv string) (*types.NamespacedName, error) {
	secretNameSpace, nameSpaceSet := os.LookupEnv(namespaceEnv)
	secretName, nameSet := os.LookupEnv(nameEnv)

	// either secret namespace/name must be set, or none at all
	if !((nameSpaceSet && nameSet) || (!nameSpaceSet && !nameSet)) {
		missingParamError := constructMissingParamsMsg([]bool{nameSpaceSet, nameSet},
			[]string{secretNameSpaceEnvVar, secretNameEnvVar})
		return nil, fmt.Errorf("all or none of the environment variables %s and %s must be set. %s",
			secretNameSpaceEnvVar, secretNameEnvVar, missingParamError)
	}
	if !nameSpaceSet {
		return nil, nil
	}
	secret := &types.NamespacedName{
		Name:      secretName,
		Namespace: secretNameSpace,
	}
	return secret, nil
}
