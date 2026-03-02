import json
import threading
import logging
from cffi import FFI
from pathlib import Path

logger = logging.getLogger(__name__)

ffi = FFI()

ffi.cdef(
    """
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
"""
)

_repo_root = Path(__file__).resolve().parents[1]
lib = ffi.dlopen(str(_repo_root / "lib" / "liblogosdelivery.so"))

CallbackType = ffi.callback("void(int, const char*, size_t, void*)")


def _new_cb_state():
    return {
        "done": threading.Event(),
        "ret": None,
        "msg": None,
    }


def _wait_cb(state, op_name: str, timeout_s: float = 20.0):
    finished = state["done"].wait(timeout_s)
    if not finished:
        raise TimeoutError(f"{op_name}: timeout waiting for callback after {timeout_s}s")

    ret = state["ret"]
    msg = state["msg"] or b""

    if ret is None:
        raise RuntimeError(f"{op_name}: callback fired but ret is None")

    if ret != 0:
        raise RuntimeError(f"{op_name}: failed (ret={ret}) msg={msg!r}")


class NodeWrapper:
    def __init__(self, ctx, config_buffer, event_cb_handler):
        self.ctx = ctx
        self._config_buffer = config_buffer
        self._event_cb_handler = event_cb_handler

    @staticmethod
    def _make_cb(py_callback, state=None):
        def c_cb(ret, char_p, length, userData):
            msg = ffi.buffer(char_p, length)[:]

            if state is not None and not state["done"].is_set():
                state["ret"] = int(ret)
                state["msg"] = msg
                state["done"].set()

            if py_callback is not None:
                py_callback(int(ret), msg)

        return CallbackType(c_cb)

    @classmethod
    def create_node(
        cls,
        config: dict,
        create_cb=None,
        event_cb=None,
        *,
        timeout_s: float = 20.0,
    ):
        config_json = json.dumps(config, separators=(",", ":"), ensure_ascii=False)
        config_buffer = ffi.new("char[]", config_json.encode("utf-8"))

        state = _new_cb_state()
        create_c_cb = cls._make_cb(create_cb, state)

        ctx = lib.logosdelivery_create_node(
            config_buffer,
            create_c_cb,
            ffi.NULL,
        )

        if ctx == ffi.NULL:
            raise RuntimeError("create_node: ctx is NULL")

        _wait_cb(state, "create_node", timeout_s)

        event_cb_handler = None
        if event_cb is not None:
            event_cb_handler = cls._make_cb(event_cb, state=None)
            lib.logosdelivery_set_event_callback(
                ctx,
                event_cb_handler,
                ffi.NULL,
            )

        return cls(ctx, config_buffer, event_cb_handler)

    def start_node(self, start_cb=None, *, timeout_s: float = 20.0):
        state = _new_cb_state()
        cb = self._make_cb(start_cb, state)

        rc = int(lib.logosdelivery_start_node(self.ctx, cb, ffi.NULL))
        if rc != 0:
            raise RuntimeError(f"start_node: immediate call failed (ret={rc})")

        _wait_cb(state, "start_node", timeout_s)
        return 0

    def stop_node(self, stop_cb=None, *, timeout_s: float = 20.0):
        state = _new_cb_state()
        cb = self._make_cb(stop_cb, state)

        rc = int(lib.logosdelivery_stop_node(self.ctx, cb, ffi.NULL))
        if rc != 0:
            raise RuntimeError(f"stop_node: immediate call failed (ret={rc})")

        _wait_cb(state, "stop_node", timeout_s)
        return 0

    def destroy(self, destroy_cb=None, *, timeout_s: float = 20.0):
        state = _new_cb_state()
        cb = self._make_cb(destroy_cb, state)

        rc = int(lib.logosdelivery_destroy(self.ctx, cb, ffi.NULL))
        if rc != 0:
            raise RuntimeError(f"destroy: immediate call failed (ret={rc})")

        _wait_cb(state, "destroy", timeout_s)
        return 0

    def stop_and_destroy(self, cb=None, *, timeout_s: float = 20.0):
        self.stop_node(stop_cb=cb, timeout_s=timeout_s)
        self.destroy(destroy_cb=cb, timeout_s=timeout_s)
        return 0

    def subscribe_content_topic(self, content_topic: str, subscribe_cb=None, *, timeout_s: float = 20.0):
        state = _new_cb_state()
        cb = self._make_cb(subscribe_cb, state)

        rc = int(
            lib.logosdelivery_subscribe(
                self.ctx,
                cb,
                ffi.NULL,
                content_topic.encode("utf-8"),
            )
        )
        if rc != 0:
            raise RuntimeError(f"subscribe_content_topic: immediate call failed (ret={rc})")

        _wait_cb(state, f"subscribe({content_topic})", timeout_s)
        return 0

    def unsubscribe_content_topic(self, content_topic: str, unsubscribe_cb=None, *, timeout_s: float = 20.0):
        state = _new_cb_state()
        cb = self._make_cb(unsubscribe_cb, state)

        rc = int(
            lib.logosdelivery_unsubscribe(
                self.ctx,
                cb,
                ffi.NULL,
                content_topic.encode("utf-8"),
            )
        )
        if rc != 0:
            raise RuntimeError(f"unsubscribe_content_topic: immediate call failed (ret={rc})")

        _wait_cb(state, f"unsubscribe({content_topic})", timeout_s)
        return 0