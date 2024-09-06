package license

import (
	_ "embed"
	"os"
)

//go:embed LICENSE
var licenseData []byte

func DoLicense() {
	os.Stdout.Write(licenseData)
}
