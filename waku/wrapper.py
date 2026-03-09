import json
import threading
import logging
from cffi import FFI
from pathlib import Path
from result import Result, Ok, Err

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


def _wait_cb(state, op_name: str, timeout_s: float = 20.0) -> Result[int, str]:
    finished = state["done"].wait(timeout_s)
    if not finished:
        return Err(f"{op_name}: timeout waiting for callback after {timeout_s}s")

    ret = state["ret"]
    msg = state["msg"] or b""

    if ret is None:
        return Err(f"{op_name}: callback fired but ret is None")

    if ret != 0:
        return Err(f"{op_name}: failed (ret={ret}) msg={msg!r}")

    return Ok(ret)


class NodeWrapper:
    def __init__(self, ctx, config_buffer, event_cb_handler):
        self.ctx = ctx
        self._config_buffer = config_buffer
        self._event_cb_handler = event_cb_handler

    @staticmethod
    def _make_waiting_cb(state):
        def c_cb(ret, char_p, length, userData):
            msg = ffi.buffer(char_p, length)[:] if char_p != ffi.NULL else b""

            if not state["done"].is_set():
                state["ret"] = int(ret)
                state["msg"] = msg
                state["done"].set()

        return CallbackType(c_cb)

    @staticmethod
    def _make_event_cb(py_callback):
        def c_cb(ret, char_p, length, userData):
            msg = ffi.buffer(char_p, length)[:] if char_p != ffi.NULL else b""
            py_callback(int(ret), msg)

        return CallbackType(c_cb)

    @classmethod
    def create_node(
        cls,
        config: dict,
        event_cb=None,
        *,
        timeout_s: float = 20.0,
    ) -> Result["NodeWrapper", str]:
        config_json = json.dumps(config, separators=(",", ":"), ensure_ascii=False)
        config_buffer = ffi.new("char[]", config_json.encode("utf-8"))

        state = _new_cb_state()
        create_cb = cls._make_waiting_cb(state)

        ctx = lib.logosdelivery_create_node(
            config_buffer,
            create_cb,
            ffi.NULL,
        )

        if ctx == ffi.NULL:
            return Err("create_node: ctx is NULL")

        wait_result = _wait_cb(state, "create_node", timeout_s)
        if wait_result.is_err():
            return Err(wait_result.err())

        event_cb_handler = None
        if event_cb is not None:
            event_cb_handler = cls._make_event_cb(event_cb)
            lib.logosdelivery_set_event_callback(
                ctx,
                event_cb_handler,
                ffi.NULL,
            )

        return Ok(cls(ctx, config_buffer, event_cb_handler))

    @classmethod
    def create_and_start(
        cls,
        config: dict,
        event_cb=None,
        *,
        timeout_s: float = 20.0,
    ) -> Result["NodeWrapper", str]:
        node_result = cls.create_node(
            config=config,
            event_cb=event_cb,
            timeout_s=timeout_s,
        )
        if node_result.is_err():
            return Err(node_result.err())

        node = node_result.ok_value
        start_result = node.start_node(timeout_s=timeout_s)
        if start_result.is_err():
            return Err(start_result.err())

        return Ok(node)

    def start_node(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_start_node(self.ctx, cb, ffi.NULL)
        if rc != 0:
            return Err(f"start_node: immediate call failed (ret={rc})")

        return _wait_cb(state, "start_node", timeout_s)

    def stop_node(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_stop_node(self.ctx, cb, ffi.NULL)
        if rc != 0:
            return Err(f"stop_node: immediate call failed (ret={rc})")

        return _wait_cb(state, "stop_node", timeout_s)

    def destroy(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_destroy(self.ctx, cb, ffi.NULL)
        if rc != 0:
            return Err(f"destroy: immediate call failed (ret={rc})")

        return _wait_cb(state, "destroy", timeout_s)

    def stop_and_destroy(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        stop_result = self.stop_node(timeout_s=timeout_s)
        if stop_result.is_err():
            return Err(stop_result.err())

        return self.destroy(timeout_s=timeout_s)

    def subscribe_content_topic(self, content_topic: str, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_subscribe(
            self.ctx,
            cb,
            ffi.NULL,
            content_topic.encode("utf-8"),
        )
        if rc != 0:
            return Err(f"subscribe_content_topic: immediate call failed (ret={rc})")

        return _wait_cb(state, f"subscribe({content_topic})", timeout_s)

    def unsubscribe_content_topic(self, content_topic: str, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_unsubscribe(
            self.ctx,
            cb,
            ffi.NULL,
            content_topic.encode("utf-8"),
        )
        if rc != 0:
            return Err(f"unsubscribe_content_topic: immediate call failed (ret={rc})")

        return _wait_cb(state, f"unsubscribe({content_topic})", timeout_s)