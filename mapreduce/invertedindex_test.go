package mapreduce

import (
	"fmt"
	"strings"
	"testing"
)

func TestGetFiles(t *testing.T) {
	files, err := GetFiles("../testdata")
	if err != nil {
		t.Errorf("Error <%v> getting files from <%s>.", err, "../testdata")
	}
	for _, fileName := range files {
		if !strings.Contains(fileName, "cat") &&
		!strings.Contains(fileName, "dog") &&
		!strings.Contains(fileName, "car") {
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
		!strings.Contains(kvFile.Values[0], "car") {
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
	input := MRInput{Key: "doesn't matter,unused", Values: []string{"../testdata/cats.txt"}}
	go Map(input, resultChl)
	results := make(map[string][]string)
	for {
		result := <-resultChl
		if result.Key == "" {
			break
		}
		values := results[result.Key]
		values = append(values, result.Values...)
		results[result.Key] = values
	}
	actual := fmt.Sprint(results)

	fmt.Println(actual)
	//	expected := "map[Jaguar:[./testdata/cats.txt ./testdata/cats.txt] Kitty:[./testdata/cats.txt] Lion:[./testdata/cats.txt] Cheetah:[./testdata/cats.txt]]"
	//	if !strings.EqualFold(expected,	actual) {
	//		t.Errorf("Expected <%s>;\n Got <%s>", expected, actual)
	//	}
}

func TestReduce(t *testing.T) {
	resultChl := make(chan MRInput)
	input := MRInput{Key: "doesn't matter,unused", Values: []string{"../testdata/cats.txt"}}
	go Map(input, resultChl)
	results := make(map[string][]string)
	for {
		result := <-resultChl
		if result.Key == "" {
			break
		}
		values := results[result.Key]
		values = append(values, result.Values...)
		results[result.Key] = values
	}

	reduceInput := mapToKVSlice(results)
	go RemoveDups(reduceInput[0], resultChl)

	reduceResults := make(map[string][]string)
	for {
		result := <-resultChl
		if result.Key == "" {
			break
		}
		values := reduceResults[result.Key]
		values = append(values, result.Values...)
		reduceResults[result.Key] = values
	}
	actual := fmt.Sprint(reduceResults)
	fmt.Println(actual)
}
