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

    def set_event_callback(self, py_callback):
        def c_cb(ret, char_p, length, userData):
            msg = ffi.buffer(char_p, length)[:]
            py_callback(ret, msg)

        cb = CallbackType(c_cb)

        lib.logosdelivery_set_event_callback(
            self.ctx,
            cb,
            ffi.NULL,
        )

        self._event_cb_handler = cb
