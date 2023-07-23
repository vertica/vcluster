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

package util

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"

	"golang.org/x/exp/slices"
	"golang.org/x/sys/unix"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	keyValueArrayLen = 2
	ipv4Str          = "IPv4"
	ipv6Str          = "IPv6"
)

func GetJSONLogErrors(responseContent string, responseObj any, opName string) error {
	err := json.Unmarshal([]byte(responseContent), responseObj)
	if err != nil {
		opTag := ""
		if opName != "" {
			opTag = fmt.Sprintf("[%s] ", opName)
		}

		vlog.LogError(opTag+"fail to unmarshal the response content, details: %v\n", err)
		return err
	}

	return nil
}

// calculate array diff: m-n
func SliceDiff[K comparable](m, n []K) []K {
	nSet := make(map[K]struct{}, len(n))
	for _, x := range n {
		nSet[x] = struct{}{}
	}

	var diff []K
	for _, x := range m {
		if _, found := nSet[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

// calculate diff of map keys: m-n
func MapKeyDiff[M ~map[K]V, K comparable, V any](m, n M) []K {
	var diff []K

	for k := range m {
		if _, found := n[k]; !found {
			diff = append(diff, k)
		}
	}

	return diff
}

func CheckPathExist(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func StringInArray(str string, list []string) bool {
	return slices.Contains(list, str)
}

// convert an array to a string by joining the elements in the array
// using the given delimiter
func ArrayToString(arr []string, delimiter string) string {
	return strings.Join(arr, delimiter)
}

func GetCurrentUsername() (string, error) {
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	return currentUser.Username, nil
}

// avoid import strings in every operation
func TrimSpace(str string) string {
	return strings.TrimSpace(str)
}

// have this util function so no need to import file/path
// for every command that needs check file path
func IsAbsPath(path string) bool {
	return filepath.IsAbs(path)
}

func ResolveToAbsPath(path string) (string, error) {
	if !strings.Contains(path, "~") {
		return filepath.Abs(path)
	}
	// needed for resolving '~' in relative paths
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	homeDir := usr.HomeDir

	if path == "~" {
		return homeDir, nil
	} else if strings.HasPrefix(path, "~/") {
		return filepath.Join(homeDir, path[2:]), nil
	} else {
		return "", fmt.Errorf("invalid path")
	}
}

// IP util functions
func IsIPv4(ip string) bool {
	return net.ParseIP(ip).To4() != nil
}

func IsIPv6(ip string) bool {
	return net.ParseIP(ip).To16() != nil
}

func AddressCheck(address string, ipv6 bool) error {
	checkPassed := false
	if ipv6 {
		checkPassed = IsIPv6(address)
	} else {
		checkPassed = IsIPv4(address)
	}

	if !checkPassed {
		ipVersion := ipv4Str
		if ipv6 {
			ipVersion = ipv6Str
		}
		return fmt.Errorf("%s in the re-ip file is not a valid %s address", address, ipVersion)
	}

	return nil
}

func ResolveToIPAddrs(hostname string, ipv6 bool) ([]string, error) {
	// resolve hostname using local resolver
	hostIPs, err := net.LookupHost(hostname)
	if err != nil {
		return nil, err
	}
	if len(hostIPs) < 1 {
		return nil, fmt.Errorf("cannot resolve %s to a valid IP address", hostname)
	}
	var v4Addrs []string
	var v6Addrs []string
	for _, addr := range hostIPs {
		if IsIPv4(addr) {
			v4Addrs = append(v4Addrs, addr)
		} else if IsIPv6(addr) {
			v6Addrs = append(v6Addrs, addr)
		} else {
			return nil, fmt.Errorf("%s is resolved to invalid address %s", hostname, addr)
		}
	}
	if ipv6 {
		return v6Addrs, nil
	}
	return v4Addrs, nil
}

func ResolveToOneIP(hostname string, ipv6 bool) (string, error) {
	// already an IPv4 or IPv6 address
	if !ipv6 && IsIPv4(hostname) {
		return hostname, nil
	}
	// IPv6
	if ipv6 && IsIPv6(hostname) {
		return hostname, nil
	}
	addrs, err := ResolveToIPAddrs(hostname, ipv6)
	// contains the case where the hostname cannot be resolved to be IP
	if err != nil {
		return "", err
	}
	if len(addrs) > 1 {
		return "", fmt.Errorf("%s is resolved to more than one IP addresss: %v", hostname, addrs)
	}
	return addrs[0], nil
}

// resolve RawHosts to be IP addresses
func ResolveRawHostsToAddresses(rawHosts []string, ipv6 bool) ([]string, error) {
	var hostAddresses []string

	for _, host := range rawHosts {
		if host == "" {
			return hostAddresses, fmt.Errorf("invalid empty host found in the provided host list")
		}
		addr, err := ResolveToOneIP(host, ipv6)
		if err != nil {
			return hostAddresses, err
		}
		// use a list to respect user input order
		hostAddresses = append(hostAddresses, addr)
	}

	return hostAddresses, nil
}

// replace all '//' to be '/', trim the path string
func GetCleanPath(path string) string {
	if path == "" {
		return path
	}
	cleanPath := strings.TrimSpace(path)
	cleanPath = strings.ReplaceAll(cleanPath, "//", "/")
	return cleanPath
}

func AbsPathCheck(dirPath string) error {
	if !filepath.IsAbs(dirPath) {
		return fmt.Errorf("'%s' is not an absolute path", dirPath)
	}
	return nil
}

func SplitHosts(hosts string) ([]string, error) {
	if strings.TrimSpace(hosts) == "" {
		return []string{}, fmt.Errorf("must specify a host or host list")
	}
	splitRes := strings.Split(strings.ToLower(strings.TrimSpace(hosts)), ",")
	for i, host := range splitRes {
		splitRes[i] = strings.TrimSpace(host)
	}
	return splitRes, nil
}

// get env var with a fallback value
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func CheckMissingFields(object any) error {
	var missingFields []string
	v := reflect.ValueOf(object)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).IsZero() {
			missingFields = append(missingFields, v.Type().Field(i).Name)
		}
	}
	if len(missingFields) > 0 {
		return fmt.Errorf("unexpected or missing fields in response object: %v", missingFields)
	}
	return nil
}

// when password is given, the user name cannot be empty
func ValidateUsernameAndPassword(useHTTPPassword bool, userName string) {
	if useHTTPPassword && userName == "" {
		panic("[Programmer error] should provide a username for using basic authentication for HTTPS requests")
	}
}

const (
	FileExist    = 0
	FileNotExist = 1
	NoWritePerm  = 2
	// this can be extended
	// if we want to check other permissions
)

// Check whether the directory is read accessible
// by trying to open the file
func CanReadAccessDir(dirPath string) error {
	if _, err := os.Open(dirPath); err != nil {
		return fmt.Errorf("read access denied to path [%s]", dirPath)
	}
	return nil
}

// Check whether the directory is read accessible
func CanWriteAccessDir(dirPath string) int {
	// check whether the path exists
	_, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return FileNotExist
		}
	}

	// check whether the path has write access
	if err := unix.Access(dirPath, unix.W_OK); err != nil {
		log.Printf("Path '%s' is not writable.\n", dirPath)
		return NoWritePerm
	}

	return FileExist
}

// copy file from a source path to a destination path
func CopyFile(sourcePath, destinationPath string, perm fs.FileMode) error {
	fileBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	err = os.WriteFile(destinationPath, fileBytes, perm)
	if err != nil {
		return fmt.Errorf("fail to create file at %s", destinationPath)
	}

	return nil
}

// check if an option is passed in
func IsOptionSet(f *flag.FlagSet, optionName string) bool {
	found := false
	f.Visit(func(f *flag.Flag) {
		if f.Name == optionName {
			found = true
		}
	})
	return found
}

// ValidateName will validate the name of an obj, the obj can be database, subcluster, etc.
// when a name is provided, make sure no special chars are in it
func ValidateName(name, obj string) error {
	escapeChars := `=<>'^\".@*?#&/-:;{}()[] \~!%+|,` + "`$"
	for _, c := range name {
		if strings.Contains(escapeChars, string(c)) {
			return fmt.Errorf("invalid character in %s name: %c", obj, c)
		}
	}
	return nil
}

func ValidateDBName(dbName string) error {
	return ValidateName(dbName, "database")
}

// suppress help message for hidden options
func SetParserUsage(parser *flag.FlagSet, op string) {
	fmt.Printf("Usage of %s:\n", op)
	fmt.Println("Options:")
	parser.VisitAll(func(f *flag.Flag) {
		if f.Usage != SuppressHelp {
			fmt.Printf("  -%s\n\t%s\n", f.Name, f.Usage)
		}
	})
}

func GetOptionalFlagMsg(message string) string {
	return message + " [Optional]"
}

func GetEonFlagMsg(message string) string {
	return "[Eon only] " + message
}

func ValidateAbsPath(path *string, pathName string) error {
	if path != nil {
		err := AbsPathCheck(*path)
		if err != nil {
			return fmt.Errorf("must specify an absolute %s", pathName)
		}
	}
	return nil
}

// ValidateRequiredAbsPath check whether a required path is set
// then validate it
func ValidateRequiredAbsPath(path *string, pathName string) error {
	pathNotSpecifiedMsg := fmt.Sprintf("must specify an absolute %s", pathName)

	if path != nil {
		if *path == "" {
			return errors.New(pathNotSpecifiedMsg)
		}
		return ValidateAbsPath(path, pathName)
	}

	return errors.New(pathNotSpecifiedMsg)
}

func ParamNotSetErrorMsg(param string) error {
	return fmt.Errorf("%s is pointed to nil", param)
}

// only works for the happy path and is temporary
// will be remove after VER-88084 is completed
func GetHostCatalogPath(hosts []string, dbName, catalogPrefix string) map[string]string {
	dbNameLowerCase := strings.ToLower(dbName)
	hostCatalogPath := make(map[string]string)
	for i, h := range hosts {
		nodeNameSuffix := i + 1
		hostCatalogPath[h] = fmt.Sprintf("%s/%s/v_%s_node%04d_catalog",
			catalogPrefix, dbName, dbNameLowerCase, nodeNameSuffix)
	}
	return hostCatalogPath
}

// ParseConfigParams builds and returns a map from a comma-separated list of params.
func ParseConfigParams(configParamListStr string) (map[string]string, error) {
	return ParseKeyValueListStr(configParamListStr, "config-param")
}

// ParseKeyValueListStr converts a comma-separated list of key-value pairs into a map.
// Ex: key1=val1,key2=val2 ---> map[string]string{key1: val1, key2: val2}
func ParseKeyValueListStr(listStr, opt string) (map[string]string, error) {
	if listStr == "" {
		return nil, nil
	}
	list := strings.Split(strings.TrimSpace(listStr), ",")
	// passed an empty string to the given flag
	if len(list) == 0 {
		return nil, nil
	}

	listMap := make(map[string]string)
	for _, param := range list {
		// expected to see key value pairs of the format key=value
		keyValue := strings.Split(param, "=")
		if len(keyValue) != keyValueArrayLen {
			return nil, fmt.Errorf("--%s option must take NAME=VALUE as argument: %s is invalid", opt, param)
		} else if len(keyValue) > 0 && strings.TrimSpace(keyValue[0]) == "" {
			return nil, fmt.Errorf("--%s option must take NAME=VALUE as argument with NAME being non-empty: %s is invalid", opt, param)
		}
		key := strings.TrimSpace(keyValue[0])
		// we allow empty string value
		value := strings.TrimSpace(keyValue[1])
		listMap[key] = value
	}
	return listMap, nil
}

// GenVNodeName generates a vnode and returns it after checking it is not already
// taken by an existing node.
func GenVNodeName(vnodes map[string]string, dbName string, hostCount int) (string, bool) {
	dbNameInNode := strings.ToLower(dbName)
	for i := 0; i < hostCount; i++ {
		nodeNameSuffix := i + 1
		vname := fmt.Sprintf("v_%s_node%04d", dbNameInNode, nodeNameSuffix)
		if _, ok := vnodes[vname]; !ok {
			// we have found an available vnode name
			return vname, true
		}
	}
	return "", false
}
