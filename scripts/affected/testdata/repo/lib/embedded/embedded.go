package embedded

import _ "embed"

// Page is embedded from a subdirectory without Go files.
//
//go:embed assets/page.txt
var Page string
