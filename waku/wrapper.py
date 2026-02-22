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

int logosdelivery_stop_node(
    void *ctx,
    FFICallBack callback,
    void *userData
);

void logosdelivery_set_event_callback(
    void *ctx,
    FFICallBack callback,
    void *userData
);

int logosdelivery_destroy(
    void *ctx,
    FFICallBack callback,
    void *userData
);

int logosdelivery_subscribe(
    void *ctx,
    FFICallBack callback,
    void *userData,
    const char *contentTopic
);

int logosdelivery_unsubscribe(
    void *ctx,
    FFICallBack callback,
    void *userData,
    const char *contentTopic
);
""")

_repo_root = Path(__file__).resolve().parents[1]
lib = ffi.dlopen(str(_repo_root / "lib" / "liblogosdelivery.so"))

CallbackType = ffi.callback("void(int, const char*, size_t, void*)")


class NodeWrapper:
    def __init__(self, ctx, config_buffer):
        self.ctx = ctx
        self._config_buffer = config_buffer
        self._event_cb_handler = None

    @staticmethod
    def _make_cb(py_callback):
        def c_cb(ret, char_p, length, userData):
            msg = ffi.buffer(char_p, length)[:]
            py_callback(ret, msg)

        return CallbackType(c_cb)

    @classmethod
    def create_node(cls, config: dict, py_callback):
        config_json = json.dumps(config, separators=(",", ":"), ensure_ascii=False)
        config_buffer = ffi.new("char[]", config_json.encode("utf-8"))

        cb = cls._make_cb(py_callback)

        ctx = lib.logosdelivery_create_node(
            config_buffer,
            cb,
            ffi.NULL,
        )

        return cls(ctx, config_buffer)

    def start_node(self, py_callback):
        cb = self._make_cb(py_callback)

        ret = lib.logosdelivery_start_node(
            self.ctx,
            cb,
            ffi.NULL,
        )

        return int(ret)

    @classmethod
    def create_and_start(cls, config: dict, create_cb, start_cb):
        node = cls.create_node(config, create_cb)
        rc = node.start_node(start_cb)
        return node, rc

    def stop_node(self, py_callback):
        cb = self._make_cb(py_callback)

        ret = lib.logosdelivery_stop_node(
            self.ctx,
            cb,
            ffi.NULL,
        )

        return int(ret)

        self._event_cb_handler = cb

    def destroy(self, py_callback):
        cb = self._make_cb(py_callback)

        ret = lib.logosdelivery_destroy(
            self.ctx,
            cb,
            ffi.NULL,
        )

        return int(ret)

    def stop_and_destroy(self, callback):
        stop_rc = self.stop_node(callback)
        if stop_rc != 0:
            raise RuntimeError(f"Stop failed (ret={stop_rc})")

        destroy_rc = self.destroy(callback)
        if destroy_rc != 0:
            raise RuntimeError(f"Destroy failed (ret={destroy_rc})")

        return 0
    
    def subscribe_content_topic(self, content_topic: str, py_callback):
        cb = self._make_cb(py_callback)

        ret = lib.logosdelivery_subscribe(
            self.ctx,
            cb,
            ffi.NULL,
            content_topic.encode("utf-8"),
        )

        return int(ret)

    def unsubscribe_content_topic(self, content_topic: str, py_callback):
        cb = self._make_cb(py_callback)

        ret = lib.logosdelivery_unsubscribe(
            self.ctx,
            cb,
            ffi.NULL,
            content_topic.encode("utf-8"),
        )

        return int(ret)


if __name__ == "__main__":
    config = {
        "logLevel": "DEBUG",
        "mode": "Core",
        "protocolsConfig": {
            "entryNodes": [
                "/dns4/node-01.do-ams3.misc.logos-chat.status.im/tcp/30303/p2p/16Uiu2HAkxoqUTud5LUPQBRmkeL2xP4iKx2kaABYXomQRgmLUgf78"
            ],
            "clusterId": 42,
            "autoShardingConfig": {"numShardsInCluster": 8},
        },
        "networkingConfig": {
            "listenIpv4": "0.0.0.0",
            "p2pTcpPort": 60000,
            "discv5UdpPort": 9000,
        },
    }

    def cb(ret, msg):
        print("ret:", ret, "msg:", msg)

    node = NodeWrapper.create_node(config, cb)
    rc = node.start_node(cb)
    print("start rc:", rc)

    topic = "/myapp/1/chat/proto"
    rc = node.subscribe(topic, cb)
    print("subscribe rc:", rc)

    rc = node.unsubscribe(topic, cb)
    print("unsubscribe rc:", rc)

    rc = node.stop_node(cb)
    print("stop rc:", rc)

    rc = node.destroy(cb)
    print("destroy rc:", rc)