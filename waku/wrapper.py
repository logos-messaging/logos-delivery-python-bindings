import json
import threading
import logging
from time import sleep

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

int logosdelivery_send(
    void *ctx,
    FFICallBack callback,
    void *userData,
    const char *messageJson
);

int logosdelivery_get_available_node_info_ids(
    void *ctx,
    FFICallBack callback,
    void *userData
);

int logosdelivery_get_node_info(
    void *ctx,
    FFICallBack callback,
    void *userData,
    const char *nodeInfoId
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


def _wait_cb_raw(state, op_name: str, timeout_s: float = 20.0):
    finished = state["done"].wait(timeout_s)
    if not finished:
        return Err(f"{op_name}: timeout waiting for callback after {timeout_s}s")

    ret = state["ret"]
    msg = state["msg"] or b""

    if ret is None:
        return Err(f"{op_name}: callback fired but ret is None")

    return Ok((ret, msg))


def _wait_cb_status(state, op_name: str, timeout_s: float = 20.0) -> Result[int, str]:
    wait_result = _wait_cb_raw(state, op_name, timeout_s)
    if wait_result.is_err():
        return Err(wait_result.err())

    ret, msg = wait_result.ok_value
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

        wait_result = _wait_cb_status(state, "create_node", timeout_s)
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

        return _wait_cb_status(state, "start_node", timeout_s)

    def stop_node(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_stop_node(self.ctx, cb, ffi.NULL)
        if rc != 0:
            return Err(f"stop_node: immediate call failed (ret={rc})")

        return _wait_cb_status(state, "stop_node", timeout_s)

    def destroy(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_destroy(self.ctx, cb, ffi.NULL)
        if rc != 0:
            return Err(f"destroy: immediate call failed (ret={rc})")

        return _wait_cb_status(state, "destroy", timeout_s)

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

        return _wait_cb_status(state, f"subscribe({content_topic})", timeout_s)

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

        return _wait_cb_status(state, f"unsubscribe({content_topic})", timeout_s)

    def send_message(self, message: dict, *, timeout_s: float = 20.0) -> Result[str, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        message_json = json.dumps(message, separators=(",", ":"), ensure_ascii=False)

        rc = lib.logosdelivery_send(
            self.ctx,
            cb,
            ffi.NULL,
            message_json.encode("utf-8"),
        )

        if rc < 0:
            return Err(f"send_message: immediate call failed (ret={rc})")

        wait_result = _wait_cb_raw(state, "send_message", timeout_s)
        if wait_result.is_err():
            return Err(wait_result.err())

        cb_ret, cb_msg = wait_result.ok_value
        if cb_ret != 0:
            return Err(f"send_message: failed (ret={cb_ret}) msg={cb_msg!r}")

        request_id = cb_msg.decode("utf-8") if cb_msg else ""
        return Ok(request_id)

    def get_available_node_info_ids(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        node_info_id = lib.logosdelivery_get_available_node_info_ids(
            self.ctx,
            cb,
            ffi.NULL,
        )

        if node_info_id < 0:
            return Err(f"get_available_node_info_ids: immediate call failed (ret={node_info_id})")

        wait_result = _wait_cb_raw(state, "get_available_node_info_ids", timeout_s)
        if wait_result.is_err():
            return Err(wait_result.err())

        return Ok(node_info_id)

    def get_node_info(self, node_info_id: str, *, timeout_s: float = 20.0) -> Result[dict, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_get_node_info(
            self.ctx,
            cb,
            ffi.NULL,
            node_info_id.encode("utf-8"),
        )

        if rc < 0:
            return Err(f"get_node_info: immediate call failed (ret={rc})")

        wait_result = _wait_cb_raw(state, "get_node_info", timeout_s)
        if wait_result.is_err():
            return Err(wait_result.err())

        cb_ret, cb_msg = wait_result.ok_value
        if cb_ret != 0:
            return Err(f"get_node_info: failed (ret={cb_ret}) msg={cb_msg!r}")

        if not cb_msg:
            return Err("get_node_info: empty response")

        try:
            result = json.loads(cb_msg.decode("utf-8"))
        except Exception as e:
            return Err(f"get_node_info: failed to parse response: {e}")

        return Ok(result)

    def debug_get_node_info(self, node_info_id: str, *, timeout_s: float = 20.0) -> Result[tuple, str]:
        state = _new_cb_state()
        cb = self._make_waiting_cb(state)

        rc = lib.logosdelivery_get_node_info(
            self.ctx,
            cb,
            ffi.NULL,
            node_info_id.encode("utf-8"),
        )

        print(f"[DEBUG] get_node_info immediate rc={rc}")

        wait_result = _wait_cb_raw(state, "get_node_info", timeout_s)
        if wait_result.is_err():
            return Err(wait_result.err())

        cb_ret, cb_msg = wait_result.ok_value
        print(f"[DEBUG] get_node_info callback ret={cb_ret}, msg={cb_msg}")

        return Ok((rc, cb_ret, cb_msg))
    
def main():
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

    topic = "/test/1/chat/proto"

    message = {
        "contentTopic": topic,
        "payload": "SGVsbG8=",
        "ephemeral": False,
    }

    node_result = NodeWrapper.create_and_start(config)
    sleep(20)
    if node_result.is_err():
        print(node_result.err())
        return

    node = node_result.ok_value

    print(node.subscribe_content_topic(topic))
    print(node.send_message(message))
    #print(node.get_available_node_info_ids())
    print(node.stop_node())
    print(node.destroy())


if __name__ == "__main__":
    main()

