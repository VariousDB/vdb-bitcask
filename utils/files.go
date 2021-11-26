package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// GetDataFiles 获取指定目录下所有的数据文件
func GetDataFiles(path string) ([]string, error) {
	fns, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, err
	}
	sort.Strings(fns)
	return fns, nil
}

// GetDataFileIDs 获取所有文件的id列表
func GetDataFileIDs(files []string) ([]int, error) {
	var fid []int
	for _, fn := range files {
		var ext string
		fn = filepath.Base(fn)
		if ext = filepath.Ext(fn); ext != ".data" {
			continue
		}
		id, err := strconv.Atoi(strings.TrimSuffix(fn, ext))
		if err != nil {
			return nil, err
		}
		fid = append(fid, id)
	}
	return fid, nil
}

func Exist(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
