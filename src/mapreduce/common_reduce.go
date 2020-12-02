package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"sort"
)

type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	m := make(map[string][]string)

	for mi := 0; mi < nMap; mi++ {
		inFile := reduceName(jobName, mi, reduceTask)
		data, err := ioutil.ReadFile(inFile)
		if err != nil {
			log.Fatal(err)
		}

		var kvps []KeyValue
		err = json.Unmarshal(data, &kvps)
		if err != nil {
			log.Fatal(err)
		}

		for _, kvp := range kvps {
			m[kvp.Key] = append(m[kvp.Key], kvp.Value)
		}
	}

	var arrReduced []KeyValue
	for key, values := range m {
		reduced := reduceF(key, values)
		arrReduced = append(arrReduced, KeyValue{key, reduced})
	}

	sort.Sort(byKey(arrReduced))

	file, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	enc := json.NewEncoder(file)
	for _, kvp := range arrReduced {
		err := enc.Encode(kvp)
		if err != nil {
			log.Fatal(err)
		}
	}
}
