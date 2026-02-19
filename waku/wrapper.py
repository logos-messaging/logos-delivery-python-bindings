from cffi import FFI
from pathlib import Path
import json


config = {
    "relay": True,
    "discv5Discovery": True,
    "peerExchange": True,
    "clusterId": 3,
    "shard": 0,
    "rlnRelay": False
}
config_json1 = json.dumps(config)
ffi = FFI()

_repo_root = Path(__file__).resolve().parents[1]
lib = ffi.dlopen(str(_repo_root / "lib" / "liblogosdelivery.so"))

ffi.cdef("""
    typedef void (*FFICallBack)(int callerRet, const char *msg, size_t len, void *userData);

    void *logosdelivery_create_node(
        const char *configJson,
        FFICallBack callback,
        void *userData
    );
""")

def process_callback(ret, char_p, length, callback):
    byte_string = ffi.buffer(char_p, length)[:] if char_p != ffi.NULL and length else b""
    callback(ret, byte_string)

CallbackType = ffi.callback("void(int, const char*, size_t, void*)")

def logosdelivery_create_node(config_json, callback):
    def cb(ret, char_p, length, userData):
        process_callback(ret, char_p, length, callback)

    return lib.logosdelivery_create_node(
        config_json.encode("utf-8"),
        CallbackType(cb),
        ffi.cast("void*", 0),
    )

if __name__ == "__main__":
    def cb(ret, msg):
        print("ret:", ret)
        print("msg:", msg)

    ctx = logosdelivery_create_node(config_json1, cb)
    print("ctx:", ctx)