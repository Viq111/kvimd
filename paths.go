package kvimd

import (
	"io/ioutil"
	"regexp"
	"strconv"

	"github.com/pkg/errors"
)

var (
	errUnknownPattern = errors.New("unknow file pattern")
	hashDiskPattern   = regexp.MustCompile(`^db([0-9]+)\.hashdisk$`)
	valuesDiskPattern = regexp.MustCompile(`^db([0-9]+)\.valuesdisk$`)
)

// listFiles returns all the files that are present in root with the given pattern
// * represents any number
func listFiles(root string, pattern *regexp.Regexp) ([]string, error) {
	content, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0, len(content))
	for _, c := range content {
		if c.IsDir() {
			continue // Skip directories
		}
		n := []byte(c.Name())
		if pattern.Match(n) {
			ret = append(ret, string(n))
		}
	}
	return ret, nil
}

func getDBNumber(path string) (int, error) {
	if hashDiskPattern.MatchString(path) {
		m := hashDiskPattern.FindStringSubmatch(path)
		if len(m) != 2 {
			return 0, errors.Errorf("failed to get database index from file=%s (file doesn't match pattern)", path)
		}
		index, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, errors.Wrap(err, "failed to extract database index")
		}
		return index, nil
	} else if valuesDiskPattern.MatchString(path) {
		m := valuesDiskPattern.FindStringSubmatch(path)
		if len(m) != 2 {
			return 0, errors.Errorf("failed to get database index from file=%s (file doesn't match pattern)", path)
		}
		index, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, errors.Wrap(err, "failed to extract database index")
		}
		return index, nil
	}
	return 0, errUnknownPattern
}

func createHashDiskPath(index uint32) string {
	return "db" + strconv.Itoa(int(index)) + ".hashdisk"
}

func createValuesDiskPath(index uint32) string {
	return "db" + strconv.Itoa(int(index)) + ".valuesdisk"
}
