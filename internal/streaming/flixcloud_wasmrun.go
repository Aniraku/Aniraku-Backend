package streaming

import (
	"context"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// FlixCloud ships every embed page with a tiny randomized WASM module
// (w_payload, ~350 bytes, base64 in the SSR data island). Two of its three
// exports matter:
//
//	_s(k)   — stores a 32-bit seed integer in a global
//	_r(p1,p2,p3,out,len) — mixes three 32-byte key fragments byte-wise into
//	                        out (the per-page transform order and constants
//	                        are randomized, so the module is executed rather
//	                        than reimplemented)
//	_c()    — XORs two 32-byte regions of the module's own data section and
//	                        returns the pointer: the static playlist-obfuscation
//	                        key (base64+XOR wrapper around every m3u8 body)
//
// The mix output seeds the PBKDF2 chain that decrypts the master playlist
// URL; _c's output de-obfuscates the playlists themselves. wazero (pure Go,
// no cgo) runs the module so any future structural randomization keeps
// working without reverse-engineering each variant.

var flixcloudRuntime wazero.Runtime

func flixcloudRT() (wazero.Runtime, error) {
	if flixcloudRuntime != nil {
		return flixcloudRuntime, nil
	}
	rt := wazero.NewRuntime(context.Background())
	flixcloudRuntime = rt
	return rt, nil
}

// flixcloudWASMOffsets mirror the offsets the player bundle writes into the
// module's linear memory before calling _r: fragments at 1000+0/32/64 and
// the mix output at 1000+96. The module never reads below 1000 itself (its
// data section sits at 0 and at 2000), so the layout is fixed by the caller
// side and safe to hardcode.
const (
	flixcloudFrag1Ptr = 1000
	flixcloudFrag2Ptr = 1032
	flixcloudFrag3Ptr = 1064
	flixcloudOutPtr   = 1096
	flixcloudFragLen  = 32
)

// flixcloudInstantiate compiles and starts one fresh instance. Freshness
// matters: _s mutates a global, and the browser player re-instantiates per
// decrypt, so state never leaks between resolutions.
func flixcloudInstantiate(payload []byte) (api.Module, error) {
	rt, err := flixcloudRT()
	if err != nil {
		return nil, err
	}
	return rt.Instantiate(context.Background(), payload)
}

// flixcloudMix runs the module's _s/_r pair over three key fragments and
// returns the 32-byte mixed key. Fragments are zero-padded to the 32-byte
// regions exactly like the browser copy does (the JS writes each fragment
// into zero-initialized linear memory before _r reads full 32-byte windows).
func flixcloudMix(mod api.Module, frag1, frag2, frag3 []byte, k uint32) ([]byte, error) {
	mem := mod.ExportedMemory("memory")
	if mem == nil {
		return nil, errors.New("flixcloud wasm: memory export missing")
	}
	if mem.Size() == 0 {
		if _, ok := mem.Grow(1); !ok {
			return nil, errors.New("flixcloud wasm: memory grow failed")
		}
	}
	_s := mod.ExportedFunction("_s")
	_r := mod.ExportedFunction("_r")
	if _s == nil || _r == nil {
		return nil, errors.New("flixcloud wasm: _s/_r exports missing")
	}
	pad := func(b []byte) []byte {
		out := make([]byte, flixcloudFragLen)
		copy(out, b)
		return out
	}
	if !mem.Write(flixcloudFrag1Ptr, pad(frag1)) {
		return nil, errors.New("flixcloud wasm: frag1 write failed")
	}
	if !mem.Write(flixcloudFrag2Ptr, pad(frag2)) {
		return nil, errors.New("flixcloud wasm: frag2 write failed")
	}
	if !mem.Write(flixcloudFrag3Ptr, pad(frag3)) {
		return nil, errors.New("flixcloud wasm: frag3 write failed")
	}
	_s.Call(context.Background(), uint64(k))
	if _, err := _r.Call(context.Background(),
		flixcloudFrag1Ptr, flixcloudFrag2Ptr, flixcloudFrag3Ptr,
		flixcloudOutPtr, flixcloudFragLen); err != nil {
		return nil, fmt.Errorf("flixcloud wasm: _r: %w", err)
	}
	out, ok := mem.Read(flixcloudOutPtr, flixcloudFragLen)
	if !ok {
		return nil, errors.New("flixcloud wasm: output read failed")
	}
	res := make([]byte, flixcloudFragLen)
	copy(res, out)
	return res, nil
}

// flixcloudPlaylistKey runs the module's _c export (data-section XOR) and
// returns the 32-byte playlist obfuscation key. The key is baked into the
// module's data section, so any payload works — it does not depend on the
// per-page seed or fragments.
func flixcloudPlaylistKey(mod api.Module) ([]byte, error) {
	mem := mod.ExportedMemory("memory")
	if mem == nil {
		return nil, errors.New("flixcloud wasm: memory export missing")
	}
	if mem.Size() == 0 {
		if _, ok := mem.Grow(1); !ok {
			return nil, errors.New("flixcloud wasm: memory grow failed")
		}
	}
	_c := mod.ExportedFunction("_c")
	if _c == nil {
		return nil, errors.New("flixcloud wasm: _c export missing")
	}
	ptr, err := _c.Call(context.Background())
	if err != nil {
		return nil, fmt.Errorf("flixcloud wasm: _c: %w", err)
	}
	out, ok := mem.Read(uint32(ptr[0]), flixcloudFragLen)
	if !ok {
		return nil, errors.New("flixcloud wasm: key read failed")
	}
	res := make([]byte, flixcloudFragLen)
	copy(res, out)
	return res, nil
}
