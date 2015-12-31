# gomapreduce
Simple map-reduce implementation in Go.

An example map/reduce application is included (inverted index). It maps each word in a file to the file that contains it, combines all the results, removes duplicate file entries, and returns a list of files associated with each word.

For example, if `file1` contains _"The dog"_ and `file2` contains _"The Cat"_, then the result will be:  
* `The -> {file1, file2}` 
* `Dog -> {file1}`  
* `Cat -> {file2}`  

This project is essentiallly a Go port of Tom Van Cutsem's [erlang-mapreduce project](https://github.com/tvcutsem/erlang-mapreduce).
