package mapreduce

import (
	"fmt"
	"strings"
	"testing"
)

func TestAll(t *testing.T) {
	files, err := GetKVFiles("../testdata")
	if err != nil {
		panic(fmt.Sprintf("Error GetKVFiles() [%v]", err))
	}

	MapReduce(files, Map, RemoveDups)
}

func TestGetFiles(t *testing.T) {
	files, err := GetFiles("../testdata")
	if err != nil {
		t.Errorf("Error <%v> getting files from <%s>.", err, "../testdata")
	}
	for _, fileName := range files {
		if !strings.Contains(fileName, "cat") &&
			!strings.Contains(fileName, "dog") &&
			!strings.Contains(fileName, "car") &&
			!strings.Contains(fileName, "nowisthetime") &&
			!strings.Contains(fileName, "butterfly") &&
			!strings.Contains(fileName, "thequickbrownfox") {
			t.Errorf("Unexpected file named <%s>", fileName)
		}
	}
}

func TestGetKVFiles(t *testing.T) {
	files, err := GetKVFiles("../testdata")
	if err != nil {
		t.Errorf("Error <%v> getting files from <%s>.", err, "../testdata")
	}
	for _, kvFile := range files {
		if !strings.Contains(kvFile.Values[0], "cat") &&
			!strings.Contains(kvFile.Values[0], "dog") &&
			!strings.Contains(kvFile.Values[0], "car") &&
			!strings.Contains(kvFile.Values[0], "nowisthetime") &&
			!strings.Contains(kvFile.Values[0], "butterfly") &&
			!strings.Contains(kvFile.Values[0], "thequickbrownfox") {
			t.Errorf("Unexpected file named <%s>", kvFile.Values[0])
		}
	}
}

func TestGetWords(t *testing.T) {
	words, err := GetWords("../testdata/cats.txt")
	if err != nil {
		t.Errorf("Error <%v> reading file <%s>.", err, "../testdata/cats.txt")
	}
	for _, word := range words {
		if !strings.EqualFold(word, "Jaguar") &&
			!strings.EqualFold(word, "Kitty") &&
			!strings.EqualFold(word, "Cheetah") &&
			!strings.EqualFold(word, "Lion") {
			t.Errorf("Unexpected word  <%s>", word)
		}
	}
}

func TestMap(t *testing.T) {
	resultChl := make(chan MRInput)
	doneChl := make(chan struct{}, 1)
	input := MRInput{Key: "doesn't matter,unused", Values: []string{"../testdata/cats.txt"}}
	go Map(input, resultChl, doneChl)
	results := make(map[string][]string)

GETRESULTS:
	for {
		select {
		case result := <-resultChl:
			values := results[result.Key]
			values = append(values, result.Values...)
			results[result.Key] = values
		case <-doneChl:
			break GETRESULTS
		}
	}
	actual := fmt.Sprint(results)

	t.Log(actual)
	//expected := "map[Jaguar:[./testdata/cats.txt ./testdata/cats.txt] Kitty:[./testdata/cats.txt] Lion:[./testdata/cats.txt] Cheetah:[./testdata/cats.txt]]"
	//if !strings.EqualFold(expected, actual) {
	//t.Errorf("Expected <%s>;\n Got <%s>", expected, actual)
	//}
}

func TestReduce(t *testing.T) {
	resultChl := make(chan MRInput)
	doneChl := make(chan struct{}, 1)
	input := MRInput{Key: "doesn't matter,unused", Values: []string{"../testdata/cats.txt"}}
	go Map(input, resultChl, doneChl)
	results := make(map[string][]string)

GETRESULTS:
	for {
		select {
		case result := <-resultChl:
			values := results[result.Key]
			values = append(values, result.Values...)
			results[result.Key] = values
		case <-doneChl:
			break GETRESULTS
		}
	}

	reduceInput := mapToKVSlice(results)
	go RemoveDups(reduceInput[0], resultChl, doneChl)

	reduceResults := make(map[string][]string)
GETREDUCERESULTS:
	for {
		select {
		case result := <-resultChl:
			values := reduceResults[result.Key]
			values = append(values, result.Values...)
			reduceResults[result.Key] = values
		case <-doneChl:
			break GETREDUCERESULTS
		}
	}
	actual := fmt.Sprint(reduceResults)
	t.Log(actual)
}

func BenchmarkReduce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resultChl := make(chan MRInput)
		doneChl := make(chan struct{}, 1)
		input := MRInput{Key: "doesn't matter,unused", Values: []string{"../testdata/cats.txt"}}
		go Map(input, resultChl, doneChl)
		results := make(map[string][]string)

	GETRESULTS:
		for {
			select {
			case result := <-resultChl:
				values := results[result.Key]
				values = append(values, result.Values...)
				results[result.Key] = values
			case <-doneChl:
				break GETRESULTS
			}
		}

		reduceInput := mapToKVSlice(results)
		go RemoveDups(reduceInput[0], resultChl, doneChl)

		reduceResults := make(map[string][]string)
	GETREDUCERESULTS:
		for {
			select {
			case result := <-resultChl:
				values := reduceResults[result.Key]
				values = append(values, result.Values...)
				reduceResults[result.Key] = values
			case <-doneChl:
				break GETREDUCERESULTS
			}
		}
	}
}
