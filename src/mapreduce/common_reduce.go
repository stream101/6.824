package mapreduce
import (
    "os"
    "encoding/json"
    "log"
    "sort"
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
	//
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

    out, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0644)
    fmt.Printf("reduce output %v\n", outFile)
    defer out.Close()
    enc := json.NewEncoder(out)
    if (err != nil) {
        log.Fatal(err)
    }

    //parse map intermediate files one by one
    //save the values of the same key
    m := make(map[string][]string)
    for i:=0; i<nMap; i++ {
        inFile := reduceName(jobName, i, reduceTaskNumber)
        //fmt.Printf("reduce input %v\n", inFile)
        in, err := os.OpenFile(inFile, os.O_RDONLY, 0644)
        if (err != nil) {
            log.Fatal(err)
        }
        dec := json.NewDecoder(in)
        for dec.More() {
            var kv KeyValue
            err := dec.Decode(&kv)
            if err != nil {
                log.Fatal(err)
            }
            //fmt.Println("key %v value %v", kv.Key, kv.Value)
            m[kv.Key] = append(m[kv.Key], kv.Value)
        }

        in.Close()
    }
    //sort key
    keys := make([]string, 0, len(m))
    for k := range m{
        keys = append(keys, k)
    }
    sort.Strings(keys)

    for _, key := range keys {
        res := reduceF(key, m[key])
        enc.Encode(KeyValue{key, res})
    }
}
