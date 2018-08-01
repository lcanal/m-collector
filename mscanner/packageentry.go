package mscanner

//PackageEntry main structure of node_modules package
type PackageEntry struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Description string `json:"description"`
	Homepage    string `json:"homepage"`
}
