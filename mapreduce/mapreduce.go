/*
gomapreduce implements a simple map-reduce alogorithm where the map and reduce operations can be specified
by the map-reduce client.
*/

package mapreduce

// MRInput defines the structure for inputs to the map and reduce functions.
type MRInput struct {
	Key    string
	Values []string
}

// MapReduce is the entry point to the map-reduce process. It takes an input to the map-reduce process,
// the mapping function, and the reduce function. "input" contains key/value pairs that represent the
// input to the map reduce process.
//
// mapFunc and ReduceFunc are expected to return their results on their respective collectChan
// channel parameters. They are also expected to return a final MRInput instance initialized with
// its "zero" values to signal they're done (e.g., MRInput{}).
//
// MapReduce is a simple function that runs in the same goroutine as the caller. The rest of the map-reduce
// process runs in separate goroutines.
//
func MapReduce(input []MRInput, mapFunc func(input MRInput, collectChan chan MRInput),
	reduceFunc func(input MRInput, collectChan chan MRInput)) (result map[string][]string) {
	resultChl := make(chan map[string][]string, 1)

	// Kick off map/reduce process
	go master(resultChl, mapFunc, reduceFunc, input)

	// Wait for result
	result = <-resultChl
	return result
}

// master implements the high level map-reduce algorithm. This mainly consists of (1) starting a goroutine for each
// of the entries in the inputs parameter to do the mapping; (2) Collecting the results of the mapping process from
// each of the mapper goroutines; (3) starting a goroutine for each of the entries in mapping results to perform
// the reduce operation; (4) collecting the final results and sending them over the resultChl.
func master(resultChl chan map[string][]string, mapFunc func(input MRInput, resChan chan MRInput),
	reduceFunc func(input MRInput, resChan chan MRInput), inputs []MRInput) {

	// Used to collect the results from the mapping and reduce operations.
	collectChl := make(chan MRInput)

	// Spawn a mapper goroutine for each input, with a mapping function and a
	// channel to collect the intermediate results.
	for _, input := range inputs {
		startWorkers(mapFunc, input, collectChl)
	}

	// Gather all the mapping results
	numResults := len(inputs)
	intermediateResultMap := collectResults(collectChl, numResults)

	// Spawn a reduce goroutine for each mapping result, with a reduce function and a
	// channel to collect the results. First though, convert the intermediate results into
	// a slice of MRInputs suitable for input for the reduce function.
	intermediateResults := mapToKVSlice(intermediateResultMap)
	for _, intermediateResult := range intermediateResults {
		startWorkers(reduceFunc, intermediateResult, collectChl)
	}

	// Collect the results from the reduce operation
	numResults = len(intermediateResults)
	finalResults := collectResults(collectChl, numResults)

	resultChl <- finalResults

}

func startWorkers(workerFun func(input MRInput, retChan chan MRInput), input MRInput, collectChl chan MRInput) {
	go workerFun(input, collectChl)
}

func collectResults(collectChl chan MRInput, numProcs int) map[string][]string {
	results := make(map[string][]string)

	// Each map/reduce process will provide a zero-value initialized MRInput in the final result just prior to exiting.
	// This function reduces numProcs by 1 for zero-value MRInput result until numProcs is 0. I.e., it runs until all
	// mappers/reducers have exited.

	for i := 0; numProcs > 0; i++ {
		result := <-collectChl
		if result.Key == "" {
			numProcs--
			continue
		}
		values := results[result.Key]
		values = append(values, result.Values...)
		results[result.Key] = values
	}
	return results
}

//mapToKVSlice transforms a map[string][]string to a slice of MRInputs.
func mapToKVSlice(kvMap map[string][]string) []MRInput {
	kvs := make([]MRInput, 0)
	for key, value := range kvMap {
		kv := MRInput{key, value}
		kvs = append(kvs, kv)
	}
	return kvs
}
