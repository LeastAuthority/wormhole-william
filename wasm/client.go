// +build js,wasm

package wasm

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"syscall/js"
	"unsafe"

	"github.com/psanford/wormhole-william/wormhole"
)

type ClientMap = map[uintptr]*wormhole.Client

// TODO: automate use of `-ld -X` with env vars
const DEFAULT_APP_ID = "myFileTransfer"
const DEFAULT_RENDEZVOUS_URL = "http://localhost:4000/v1"
const DEFAULT_TRANSIT_RELAY_ADDRESS = "ws://localhost:4002"
const DEFAULT_PASSPHRASE_COMPONENT_LENGTH = 2

var (
	ErrClientNotFound = fmt.Errorf("%s", "wormhole client not found")

	clientMap ClientMap
)

func init() {
	clientMap = make(ClientMap)
}

func NewClient(this js.Value, args []js.Value) interface{} {
	var (
		config js.Value
		object = js.Global().Get("Object")
	)
	if len(args) > 0 && args[0].InstanceOf(object) {
		config = args[0]
	} else {
		config = object.New()
	}

	// read from config
	appID := config.Get("appID")
	rendezvousURL := config.Get("rendezvousURL")
	transitRelayAddress := config.Get("transitRelayAddress")
	passPhraseComponentLength := config.Get("passPhraseComponentLength")

	//overwrite config with defaults where falsy
	//TODO: use constants for property names?
	if !appID.Truthy() {
		config.Set("appID", DEFAULT_APP_ID)
	}
	if !rendezvousURL.Truthy() {
		config.Set("rendezvousURL", DEFAULT_RENDEZVOUS_URL)
	}
	if !transitRelayAddress.Truthy() {
		config.Set("transitRelayAddress", DEFAULT_TRANSIT_RELAY_ADDRESS)
	}
	if !passPhraseComponentLength.Truthy() {
		config.Set("passPhraseComponentLength", DEFAULT_PASSPHRASE_COMPONENT_LENGTH)
	}

	// read config with defaults merged
	appID = config.Get("appID")
	rendezvousURL = config.Get("rendezvousURL")
	transitRelayAddress = config.Get("transitRelayAddress")
	passPhraseComponentLength = config.Get("passPhraseComponentLength")

	client := &wormhole.Client{
		AppID:                     appID.String(),
		RendezvousURL:             rendezvousURL.String(),
		TransitRelayURL:           transitRelayAddress.String(),
		PassPhraseComponentLength: passPhraseComponentLength.Int(),
	}
	clientPtr := uintptr(unsafe.Pointer(client))
	clientMap[clientPtr] = client

	return clientPtr
}

func Client_SendText(_ js.Value, args []js.Value) interface{} {
	ctx := context.Background()

	return NewPromise(func(resolve ResolveFn, reject RejectFn) {
		if len(args) != 2 {
			reject(fmt.Errorf("invalid number of arguments: %d. expected: %d", len(args), 2))
			return
		}

		clientPtr := uintptr(args[0].Int())
		msg := args[1].String()
		err, client := getClient(clientPtr)
		if err != nil {
			reject(err)
			return
		}

		code, _, err := client.SendText(ctx, msg)
		if err != nil {
			reject(err)
			return
		}
		resolve(code)
	})
}

func Client_SendFile(_ js.Value, args []js.Value) interface{} {
	ctx := context.Background()

	return NewPromise(func(resolve ResolveFn, reject RejectFn) {
		if len(args) != 3 {
			reject(fmt.Errorf("invalid number of arguments: %d. expected: %d", len(args), 3))
			return
		}

		clientPtr := uintptr(args[0].Int())
		fileName := args[1].String()

		uint8Array := args[2]
		size := uint8Array.Get("byteLength").Int()
		fileData := make([]byte, size)
		js.CopyBytesToGo(fileData, uint8Array)
		fileReader := bytes.NewReader(fileData)

		err, client := getClient(clientPtr)
		if err != nil {
			reject(err)
			return
		}

			code, _, err := client.SendFile(ctx, fileName, fileReader)
			if err != nil {
				reject(err)
				return
			}
			resolve(code)
	})
}

func Client_RecvText(_ js.Value, args []js.Value) interface{} {
	ctx := context.Background()

	return NewPromise(func(resolve ResolveFn, reject RejectFn) {
		if len(args) != 2 {
			reject(fmt.Errorf("invalid number of arguments: %d. expected: %d", len(args), 2))
			return
		}

		clientPtr := uintptr(args[0].Int())
		code := args[1].String()
		err, client := getClient(clientPtr)
		if err != nil {
			reject(err)
			return
		}

		msg, err := client.Receive(ctx, code)
		if err != nil {
			reject(err)
			return
		}

		msgBytes, err := ioutil.ReadAll(msg)
		if err != nil {
			reject(err)
			return
		}
		resolve(string(msgBytes))
	})
}

func Client_RecvFile(_ js.Value, args []js.Value) interface{} {
	ctx := context.Background()

	return NewPromise(func(resolve ResolveFn, reject RejectFn) {
		if len(args) != 2 {
			reject(fmt.Errorf("invalid number of arguments: %d. expected: %d", len(args), 2))
			return
		}

		clientPtr := uintptr(args[0].Int())
		code := args[1].String()
		err, client := getClient(clientPtr)
		if err != nil {
			reject(err)
			return
		}

		msg, err := client.Receive(ctx, code)
		if err != nil {
			reject(err)
			return
		}

		msgBytes, err := ioutil.ReadAll(msg)
		if err != nil {
			reject(err)
			return
		}

			// TODO: something better!
			jsData := js.Global().Get("Uint8Array").New(len(msgBytes))
			js.CopyBytesToJS(jsData, msgBytes)
			resolve(jsData)
	})
}

func Client_free(_ js.Value, args []js.Value) interface{} {
	if len(args) != 1 {
		return fmt.Errorf("invalid number of arguments: %d. expected: %d", len(args), 2)
	}

	clientPtr := uintptr(args[0].Int())
	delete(clientMap, clientPtr)
	return nil
}

func getClient(clientPtr uintptr) (error, *wormhole.Client) {
	client, ok := clientMap[clientPtr]
	if !ok {
		fmt.Println("clientMap entry missing")
		return ErrClientNotFound, nil
	}

	return nil, client
}

// pass a javascript defined function and call it from Go-Land
// How do we capture in types, the fact that callbackFn has two arguments?
func withProgress(_this js.Value, callbackFn js.Value) interface{} {
	if callbackFn.Type() != js.TypeFunction {
		fmt.Println("expected a function to be passed\n")
	}

	// returns an interface type that should be cast to SendOption
	// so that setOption() can be called on that returned value
	// with the rest of the options passed into it as a parameter.

	// some hints here:
	// https://stackoverflow.com/questions/62821877/how-to-call-an-external-js-function-from-wasm
	// we will have to use callbackFn.Invoke() to call it, I
	// think? So, we should probably pass a wrapper to
	// WithProgress?

	f := func(sentBytes js.Value, totalBytes js.Value) interface{} {
		return callbackFn.Invoke(sentBytes, totalBytes)
	}

	return wormhole.WithProgress(f)
}
