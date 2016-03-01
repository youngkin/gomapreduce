package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/youngkin/gomapreduce/mapreduce"
)

func main() {
	files, err := mapreduce.GetKVFiles("./testdata")
	if err != nil {
		panic(fmt.Sprintf("Error GetKVFiles() [%v]", err))
	}

	result := mapreduce.MapReduce(files, mapreduce.Map, mapreduce.RemoveDups)

	fmt.Println("Results:")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	for word, files := range result {
		fmt.Fprintf(tw, fmt.Sprintf("\t%s\t%s\n", word, files))
	}
	tw.Flush()
}
