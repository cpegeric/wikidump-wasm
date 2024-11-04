package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	extism "github.com/extism/go-sdk"
)

type IndexLocation struct {
	Offset int `json:"offset"`
	Size   int `json:"size"`
}

func wikidumps() {
	/*
		manifest := extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmUrl{
					Url: "https://github.com/extism/plugins/releases/latest/download/count_vowels.wasm",
				},
			},
		}
	*/
	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmFile{
				Path: "wikidump.wasm",
			},
		},
	}

	/*
		wasmbytes, err := ioutil.ReadFile("println.wasm")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		manifest := extism.Manifest{
			Wasm: []extism.Wasm{
				extism.WasmData{
					Data: wasmbytes,
				},
			},
		}
	*/

	ctx := context.Background()
	//config := extism.PluginConfig{}
	config := extism.PluginConfig{
		EnableWasi: true,
	}
	plugin, err := extism.NewPlugin(ctx, manifest, config, []extism.HostFunction{})
	if err != nil {
		fmt.Printf("Failed to initialize plugin: %v\n", err)
		os.Exit(1)
	}

	indexf := "/Users/eric/github/eric_test/wiki/enwiki-latest-pages-articles-multistream-index1.txt-p1p41242.bz2"
	data, err := os.ReadFile(indexf)
	if err != nil {
		fmt.Println(err)
	}

	exit, out, err := plugin.Call("get_index", data)
	if err != nil {
		fmt.Printf("plugin call %v\n", err)
		os.Exit(int(exit))
	}

	var a []IndexLocation
	err = json.Unmarshal(out, &a)
	if err != nil {
		fmt.Printf("JSON error %v", err)
		os.Exit(3)
	}

	alen := len(a) - 1
	fmt.Printf("Last offset = %d, size = %d\n", a[alen].Offset, a[alen].Size)

	stream := "/Users/eric/github/eric_test/wiki/enwiki-latest-pages-articles-multistream1.xml-p1p41242.bz2"

	fp, err := os.Open(stream)
	if err != nil {
		fmt.Println(err)
		os.Exit(6)
	}
	defer fp.Close()

	filebytes := make([]byte, a[0].Size)
	nread, err := fp.ReadAt(filebytes, int64(a[0].Offset))
	if err != nil || nread != a[0].Size {
		fmt.Println("bz2 read failed")
		os.Exit(5)
	}

	exit, out, err = plugin.Call("get_pages", filebytes)
	if err != nil {
		fmt.Printf("plugin call %v\n", err)
		os.Exit(int(exit))
	}

	fmt.Println(string(out))

}

func main() {
	wikidumps()
}
