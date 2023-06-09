# Quick start to add a third party library

1. Update imports to include a third party library. To add logrus
```
import (
   "os"
   // Recall:
   //   1. Standard library imports must come before third-party imports
   //   2. A blank line is semantically significant between
   // standard library imports and third-party imports.

   // ... etc ...
   // Here's the new third party
   "github.com/sirupsen/logrus"
)
```
2. Use `go get` to get the module. For the logrus module
   `[node_management_agent]$ go get github.com/sirupsen/logrus`
   Troubleshooting tips:
   i. [platform]$ make echo_go_build_prefix to check Golang env vars,
      Sometimes go commands without the environment settings try to 
      update files in ~/.cache. They might even try to take locks, 
      which will hang because we have NFS file system in our home dirs, 
      and that doesn't support locks.  
   ii. `go get` should take similar time as `git clone`,
      if you see `go get` hangs forever, try `git clone <repo>` to a temp
      dir to see how long it's expected to take. If `git clone` runs fast,
      then you may want to consider doing this `go get` in another or 
      a newer devjail.

3. Use `go mod vendor` to download the module into the local cache
   This can replace the whole vendor directory. You should treat
   the vendor directory as managed by go tools.
   `[node_management_agent]$ go mod vendor`

4. Run the build `cd platform; make go_all`

5. Run integration tests locally to confirm things work as expected
   `cd platform; ./tests/integration_tests.sh -s -k nma_`


6. Don't forget to Check in the new directories under vendor:

git add vendor/github.com

# The vendor directory has third-party go packages

Most third party packages are in server/third-party. go's tooling makes
it difficult to keep the packages outside of the "module" tree. The tooling
strongly encourages the convention of placing all third-party packages in
the vendor directory. The existence of the `vendor` directory triggers different
behavior in the go tools. Specifically, the build will only search for third-party
packages in that directory.


