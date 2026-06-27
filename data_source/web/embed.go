// Package web embeds the control dashboard's static assets so the whole app
// ships as a single self-contained binary.
package web

import (
	"embed"
	"io/fs"
)

//go:embed index.html app.js styles.css
var assets embed.FS

// FS is the embedded asset filesystem, served at the root path.
func FS() fs.FS { return assets }
