package mapreduce

import (
	"os"
	"encoding/json"
	"fmt"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

    outf , err := os.OpenFile(outFile, os.O_CREATE|os.O_RDWR, 0777)
    if err != nil {
    	panic("can not open or create outfile")
	}
	defer outf.Close()
	var kvSlice []KeyValue
	kvmap := make(map[string][]string)
	fdList := make([]*os.File, nMap)
	//fileToDelete := make([]string, nMap)
    for i:=0 ; i < nMap; i++  {
		filename := reduceName(jobName, i, reduceTaskNumber)
		fd , err := os.Open(filename)
		if err != nil {
			fmt.Printf("open imtermediate file err : map task number : %d, filename : %s, err: %v ", i, filename, err)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				return
			}else {
				panic("unknown error when open intermediate file")
			}
		}
		fdList = append(fdList,fd)
		//fileToDelete[i] = filename
		dec := json.NewDecoder(fd)
		for dec.More() {
			var kv KeyValue
			dec.Decode(&kv)
			kvSlice = append(kvSlice, kv)
		}
	}

	for _, eachKv := range kvSlice {
		_, ok := kvmap[eachKv.Key]
		if ok {
			kvmap[eachKv.Key] = append(kvmap[eachKv.Key], eachKv.Value)
		}else {
			kvmap[eachKv.Key] = []string{eachKv.Value}
		}

	}
	//b := new(bytes.Buffer)
	enc := json.NewEncoder(outf)
	for key, valueSlice := range kvmap{
		enc.Encode(KeyValue{key, reduceF(key , valueSlice)})
	}
	//ioutil.WriteFile(outFile, b.Bytes(), 0755)
	for _ , eachfd := range fdList {
		eachfd.Close()
	}
	//for _,eachFile := range fileToDelete {
	//	if eachFile == ""{
	//		continue
	//	}
	//	err := os.Remove(eachFile)
	//	if err != nil {
	//		panic("can not remove intermediate file" + eachFile)
	//	}
	//}
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
}
