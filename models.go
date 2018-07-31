package main

//NewAppEntry is the main container of the entry
type NewAppEntry struct {
	AppName     string        `json:"appname"`
	ModulesUsed []NodeLibrary `json:"node_modules"`
}

//NodeLibrary is the main model for node_module entries
type NodeLibrary struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	Vulnerable bool   `json:"is_vulnerable"`
}
