package kvimd

// Metadata is a datastructure given information about the initial creation of the DB
// It is a (json) exportable data structure that's written to metadata
type Metadata struct {
	workers  int `json:"workers"`
	hashSize int `json:"hash_size"` // The size of each hashmap file
}
