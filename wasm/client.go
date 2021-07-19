// +build js,wasm

package wasm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"syscall/js"
	"unsafe"

	"github.com/psanford/wormhole-william/wormhole"
)

type ClientMap = map[uintptr]*wormhole.Client

// TODO: automate use of `-ld -X` with env vars
const DEFAULT_APP_ID = "myFileTransfer"
const DEFAULT_RENDEZVOUS_URL = "ws://localhost:4000/v1"
const DEFAULT_TRANSIT_RELAY_URL = "ws://localhost:4002"
const DEFAULT_PASSPHRASE_COMPONENT_LENGTH = 2

var (
	ErrClientNotFound = fmt.Errorf("%s", "wormhole client not found")

	clientMap ClientMap
)

func init() {
	clientMap = make(ClientMap)
}

func NewClient(_ js.Value, args []js.Value) interface{} {
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
	transitRelayURL := config.Get("transitRelayURL")
	passPhraseComponentLength := config.Get("passPhraseComponentLength")

	//overwrite config with defaults where falsy
	//TODO: use constants for property names?
	if !appID.Truthy() {
		config.Set("appID", DEFAULT_APP_ID)
	}
	if !rendezvousURL.Truthy() {
		config.Set("rendezvousURL", DEFAULT_RENDEZVOUS_URL)
	}
	if !transitRelayURL.Truthy() {
		config.Set("transitRelayURL", DEFAULT_TRANSIT_RELAY_URL)
	}
	if !passPhraseComponentLength.Truthy() {
		config.Set("passPhraseComponentLength", DEFAULT_PASSPHRASE_COMPONENT_LENGTH)
	}

	// read config with defaults merged
	// TODO: need this?
	appID = config.Get("appID")
	rendezvousURL = config.Get("rendezvousURL")
	transitRelayURL = config.Get("transitRelayURL")
	passPhraseComponentLength = config.Get("passPhraseComponentLength")

	client := &wormhole.Client{
		AppID:                     appID.String(),
		RendezvousURL:             rendezvousURL.String(),
		TransitRelayURL:           transitRelayURL.String(),
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
type FileWrapper struct {
	file js.Value
	Size int64
	index int64
}

func NewFileWrapper(file js.Value) *FileWrapper {
	size := file.Get("size").Int()
	return &FileWrapper{file: file, Size: int64(size), index: 0}
}

var uint8Array = js.Global().Get("Uint8Array")

func (fileWrapper *FileWrapper) Read(p []byte) (n int, err error) {
	if fileWrapper.index >= fileWrapper.Size {
		return 0, io.EOF
	}

	// use Blob.slice(start, end) to read a part of the file.
	start := fileWrapper.index
	end := start + int64(len(p))

	var (
		bCh   = make(chan struct{}, 1)
		errCh = make(chan error, 1)
	)

	fileSlice := fileWrapper.file.Call("slice", start, end)
	arrayPromise := fileSlice.Call("arrayBuffer")

	success := js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		// uint8array from ArrayBuffer
		arrayBuf := args[0]
		uint8Buf := uint8Array.New(arrayBuf)

		n = js.CopyBytesToGo(p, uint8Buf)
		bCh <- struct{}{}
		return nil
	})
	defer success.Release()

	failure := js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		errCh <- errors.New(args[0].Get("message").String())
		return nil
	})
	defer failure.Release()

	arrayPromise.Call("then", success, failure)

	select {
	case <-bCh:
		// do nothing
	case err := <-errCh:
		return 0, err
	}

	fileWrapper.index += int64(n)

	return len(p), nil
}

func (fileWrapper *FileWrapper) Seek(offset int64, whence int) (int64, error) {
	var abs int64

	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = fileWrapper.index + offset
	case io.SeekEnd:
		abs = fileWrapper.Size + offset
	default:
		return 0, errors.New("Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("Seek: negative position")
	}

	fileWrapper.index = abs
	return abs, nil
}

func Client_SendFile(_ js.Value, args []js.Value) interface{} {
	ctx, cancel := context.WithCancel(context.Background())

	return NewPromise(func(resolve ResolveFn, reject RejectFn) {
		if len(args) != 3 && len(args) != 4 {
			reject(fmt.Errorf("invalid number of arguments: %d. expected: %s", len(args), "3 or 4"))
			return
		}

		clientPtr := uintptr(args[0].Int())
		fileName := args[1].String()

		fileJSVal := args[2]

		fileWrapper := NewFileWrapper(fileJSVal)

		err, client := getClient(clientPtr)
		if err != nil {
			reject(err)
			return
		}

		var opts []wormhole.TransferOption
		if len(args) == 4 {
			opts = collectTransferOptions(args[3])
		}

		code, resultChan, err := client.SendFile(ctx, fileName, fileWrapper, opts...)
		if err != nil {
			reject(err)
			return
		}

		returnObj := js.Global().Get("Object").New()
		returnObj.Set("code", code)
		returnObj.Set("cancel", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
			cancel()
			return nil
		}))
		returnObj.Set("done", NewPromise(
			func(resolve ResolveFn, reject RejectFn) {
				select {
				case result := <-resultChan:
					switch {
					case result.Error != nil:
						reject(result.Error)
					case result.OK == true:
						resolve(nil)
					default:
						reject(errors.New("unknown send result"))
					}
				case <-ctx.Done():
					if err := ctx.Err(); err == nil {
						resolve(nil)
					} else {
						reject(err)
					}
				}
			}),
		)
		resolve(returnObj)
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
	ctx, cancel := context.WithCancel(context.Background())

	return NewPromise(func(resolve ResolveFn, reject RejectFn) {
		// TODO: improve
		go func() {
			<-ctx.Done()
			if err := ctx.Err(); err != nil {
				reject(err)
			}
		}()

		if len(args) != 2 && len(args) != 3 {
			reject(fmt.Errorf("invalid number of arguments: %d. expected: %d or %d", len(args), 2, 3))
			return
		}

		clientPtr := uintptr(args[0].Int())
		code := args[1].String()
		err, client := getClient(clientPtr)
		if err != nil {
			reject(err)
			return
		}

		var opts []wormhole.TransferOption
		if len(args) == 3 {
			opts = collectTransferOptions(args[2])
		}

		msg, err := client.Receive(ctx, code, opts...)
		if err != nil {
			reject(err)
			return
		}

		readerObj := NewFileStreamReader(ctx, msg)
		readerObj.Set("cancel", js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
			cancel()
			return nil
		}))
		resolve(readerObj)
	})
}

func NewFileStreamReader(ctx context.Context, msg *wormhole.IncomingMessage) js.Value {
	// TODO: parameterize
	bufSize := 1 << 14 // 1024 * 4 // 4KiB

	total := 0
	readFunc := func(_ js.Value, args []js.Value) interface{} {
		buf := make([]byte, bufSize)
		return NewPromise(func(resolve ResolveFn, reject RejectFn) {
			// TODO: improve
			go func() {
				<-ctx.Done()
				if err := ctx.Err(); err != nil {
					reject(err)
				}
			}()

			if len(args) != 1 {
				reject(fmt.Errorf("invalid number of arguments: %d. expected: %d", len(args), 1))
			}

			jsBuf := args[0]
			_resolve := func(n int, done bool) {
				js.CopyBytesToJS(jsBuf, buf[:n])
				resolve(js.Global().Get("Array").New(n, done))
			}
			n, err := msg.Read(buf)
			total += n
			fmt.Printf("received total of %f Mega bytes from the sending side\n", float32(total)/(1024*1024.0))
			if err != nil {
				reject(err)
				return
			}
			if msg.ReadDone() {
				_resolve(n, true)
				return
			}
			_resolve(n, false)
		})
	}
	readerObj := js.Global().Get("Object").New()
	readerObj.Set("bufferSizeBytes", bufSize)
	readerObj.Set("read", js.FuncOf(readFunc))
	readerObj.Set("name", msg.Name)
	readerObj.Set("size", msg.UncompressedBytes64)
	return readerObj
}

func Client_free(_ js.Value, args []js.Value) interface{} {
	if len(args) != 1 {
		return fmt.Errorf("invalid number of arguments: %d. expected: %d", len(args), 2)
	}

	clientPtr := uintptr(args[0].Int())
	delete(clientMap, clientPtr)
	return js.Undefined()
}

func getClient(clientPtr uintptr) (error, *wormhole.Client) {
	client, ok := clientMap[clientPtr]
	if !ok {
		fmt.Println("clientMap entry missing")
		return ErrClientNotFound, nil
	}

	return nil, client
}

func collectTransferOptions(jsOpts js.Value) []wormhole.TransferOption {
	var opts []wormhole.TransferOption
	if !jsOpts.IsUndefined() {
		progressFunc := jsOpts.Get("progressFunc")
		if !progressFunc.IsUndefined() {
			progressOpt := withProgress(progressFunc)
			opts = append(opts, progressOpt)
		}

		offerCondition := jsOpts.Get("offerCondition")
		if !offerCondition.IsUndefined() {
			conditionalOfferOpt := withConditionalOffer(offerCondition)
			opts = append(opts, conditionalOfferOpt)
		}

		jsCode := jsOpts.Get("code")
		if !jsCode.IsUndefined() {
			codeOpt := withCode(jsOpts)
			opts = append(opts, codeOpt)
		}
	}
	return opts
}

func withProgress(progressFn js.Value) wormhole.TransferOption {
	return wormhole.WithProgress(func(sentBytes, totalBytes int64) {
		progressFn.Invoke(sentBytes, totalBytes)
	})
}

func withCode(code js.Value) wormhole.TransferOption {
	return wormhole.WithCode(code.String())
}

func withConditionalOffer(offerCondition js.Value) wormhole.TransferOption {
	return wormhole.WithConditionalOfferOption(func(offer wormhole.OfferMsg, acceptTransfer func() error, rejectTransfer func() error) {
		jsAccept := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
			return NewPromise(func(resolve ResolveFn, reject RejectFn) {
				if err := acceptTransfer(); err != nil {
					reject(err)
					return
				}
				resolve(nil)
				return
			})
		})

		jsReject := js.FuncOf(func(_ js.Value, args []js.Value) interface{} {
			return NewPromise(func(resolve ResolveFn, reject RejectFn) {
				if err := rejectTransfer(); err != nil {
					reject(err)
					return
				}
				resolve(nil)
				return
			})
		})

		if !offerCondition.IsUndefined() {
			offerObj := js.Global().Get("Object").New()
			if offer.File != nil {
				offerObj.Set("name", offer.File.FileName)
				offerObj.Set("size", offer.File.FileSize)
				offerObj.Set("accept", jsAccept)
				offerObj.Set("reject", jsReject)
				offerCondition.Invoke(offerObj)
			}
		}
	})
}
