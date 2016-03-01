package mapreduce

// Inverted-index takes a set of files and transforms them into a list of words with each word indexing the files that
// contain it. E.g., if file1 contains "The Dog" and file2 contains "The Cat", then the output will be along the lines
// of "The -> {file1, file2}" and "Dog -> {file1}" and "Cat -> {file2}".

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

// Map transforms a file of words into a word with its containing file. I.e., if file name is thought of as the original
// index to a word, this function reverses this so that the word is now the index to the file(s) containing it.
func Map(input MRInput, resultChl chan MRInput, doneChl chan struct{}) {
	fileName := input.Values[0]
	words, err := GetWords(fileName)
	if err != nil {
		panic(err)
	}

	for _, word := range words {
		resultChl <- MRInput{Key: word, Values: []string{fileName}}
	}
	doneChl <- struct{}{}
}

// RemoveDups removes any duplicates from MRInput.Values. Specifically, if a given word appears more than once in a
// file, then that file's name will appear multiple times in MRInput.Values after processing by the Map() function.
func RemoveDups(input MRInput, resultChl chan MRInput, doneChl chan struct{}) {
	set := make(map[string]struct{})
	for _, fileName := range input.Values {
		set[fileName] = struct{}{}
	}

	var uniqueFiles []string
	for k := range set {
		uniqueFiles = append(uniqueFiles, k)
	}

	resultChl <- MRInput{Key: input.Key, Values: uniqueFiles}
	doneChl <- struct{}{}
}

// GetKVFiles returns all the files in the provided directory as a slice of MRInputs. The output provides the
// original input to the Map/Reduce process.
func GetKVFiles(dirName string) ([]MRInput, error) {
	files, err := GetFiles(dirName)
	if err != nil {
		return nil, fmt.Errorf("Unexpected error returned: <%v>", err)
	}

	var kvFiles []MRInput
	for i, file := range files {
		kvFiles = append(kvFiles, MRInput{Key: strconv.Itoa(i), Values: []string{file}})
	}

	return kvFiles, nil
}

// GetFiles returns an array of files for a given dir
func GetFiles(dirName string) (files []string, err error) {
	fileInfos, err := ioutil.ReadDir(dirName)
	if err != nil {
		fmt.Println("Error reading directory:", dirName, ":", err)
		return nil, err
	}

	for _, fileInfo := range fileInfos {
		file := fileInfo.Name()
		files = append(files, filepath.Join(dirName, file))
	}

	return files, err
}

// GetWords returns a set of words in a given file
func GetWords(fileName string) ([]string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Couldn't open file:", fileName, "Error:", err)
		return nil, fmt.Errorf("Couldn't open file %s. Error %v", fileName, err)
	}

	scanner := bufio.NewScanner(f)
	var words []string
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		words = append(words, scanner.Text())
	}
	if scanner.Err() != nil {
		return nil, fmt.Errorf("Scanning error: %v", scanner.Err())
	}

	return words, nil
}
