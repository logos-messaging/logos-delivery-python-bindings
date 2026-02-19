import json
from cffi import FFI
from pathlib import Path

ffi = FFI()

ffi.cdef("""
typedef void (*FFICallBack)(int callerRet, const char *msg, size_t len, void *userData);

void *logosdelivery_create_node(
    const char *configJson,
    FFICallBack callback,
    void *userData
);

int logosdelivery_start_node(
    void *ctx,
    FFICallBack callback,
    void *userData
);
""")

_repo_root = Path(__file__).resolve().parents[1]
lib = ffi.dlopen(str(_repo_root / "lib" / "liblogosdelivery.so"))

CallbackType = ffi.callback("void(int, const char*, size_t, void*)")

class NodeHandle:
    def __init__(self, ctx, cb_handle):
        self.ctx = ctx
        self._cb_handle = cb_handle  # keep callback alive

def logosdelivery_create_node(config: dict, py_callback):
    config_json = json.dumps(config, separators=(",", ":"), ensure_ascii=False)
    cnfig_bytes = config_json.encode("utf-8")

    def c_cb(ret, char_p, length, userData):
        if char_p != ffi.NULL and length :
            msg = ffi.buffer(char_p, length)[:]
        else :
            msg = b""
        py_callback(ret, msg)

    cb_handle = CallbackType(c_cb)
    ctx = lib.logosdelivery_create_node(
        cnfig_bytes,
        cb_handle,
        ffi.NULL,
    )
    return NodeHandle(ctx, cb_handle)

if __name__ == "__main__":
    config = {
        "logLevel": "DEBUG",
        "mode": "Core",
        "protocolsConfig": {
            "entryNodes": [
                "/dns4/node-01.do-ams3.misc.logos-chat.status.im/tcp/30303/p2p/16Uiu2HAkxoqUTud5LUPQBRmkeL2xP4iKx2kaABYXomQRgmLUgf78"
            ],
            "clusterId": 3,
            "autoShardingConfig": {"numShardsInCluster": 8},
        },
        "networkingConfig": {
            "listenIpv4": "0.0.0.0",
            "p2pTcpPort": 60000,
            "discv5UdpPort": 9000,
        },
    }

    def cb(ret, msg):
        print("ret:", ret)
        print("msg:", msg)

    h = logosdelivery_create_node(config, cb)
    print("ctx:", h.ctx)
