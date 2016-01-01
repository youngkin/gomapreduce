package main

import (
	"fmt"

	"github.com/youngkin/gomapreduce/mapreduce"
)

func main() {
	files, err := mapreduce.GetKVFiles("./testdata")
	if err != nil {
		panic(fmt.Sprintf("Error GetKVFiles()", err))
	}

	result := mapreduce.MapReduce(files, mapreduce.Map, mapreduce.RemoveDups)
	fmt.Println("Results:")
	for word, files := range result {
		fmt.Println("\t", word, "\t:", files)
	}
}
