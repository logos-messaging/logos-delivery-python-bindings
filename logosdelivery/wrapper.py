import json
import threading

import cbor2
from cffi import FFI
from pathlib import Path
from result import Result, Ok, Err

ffi = FFI()

ffi.cdef(
    """
typedef void (*FFICallback)(int callerRet, const char *msg, size_t len, void *userData);

void *logosdelivery_create_node(
    const uint8_t *reqCbor,
    size_t reqCborLen,
    FFICallback onCreated,
    void *userData
);

int logosdelivery_destroy(void *ctx);
int logosdelivery_shutdown(void);
const char *logosdelivery_version(void);

int logosdelivery_start_node(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_stop_node(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_get_connection_status(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_get_available_node_info_ids(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_get_available_configs(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);

int logosdelivery_subscribe(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_unsubscribe(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_send(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_get_node_info(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);

int logosdelivery_set_service_discovery_plugin(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_get_discovery_requirements(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_clear_service_discovery_plugin(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);

uint64_t logosdelivery_add_event_listener(
    void *ctx,
    const char *eventName,
    FFICallback callback,
    void *userData
);

int logosdelivery_remove_event_listener(
    void *ctx,
    uint64_t listenerId
);

int logosdelivery_channel_create(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_channel_exists(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_channel_send(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
int logosdelivery_channel_close(void *ctx, FFICallback callback, void *userData, const uint8_t *reqCbor, size_t reqCborLen);
"""
)

_repo_root = Path(__file__).resolve().parents[1]
lib = ffi.dlopen(str(_repo_root / "lib" / "liblogosdelivery.so"))

# Non-terminal progress tick (~every 5s while a request is in flight), always
# followed by a terminal RET_OK/RET_ERR. _on_reply drops it so a slow call
# (start_node most of all) is not latched as a result.
RET_STALE_WARN = 3

# The library can call back with any handle in this set. _on_reply removes a
# request's handle after the final RET_OK or RET_ERR. destroy() removes a node's
# event handle after the library destroys the node.
_global_set = set()


def _new_handle(obj):
    handle = ffi.new_handle(obj)
    _global_set.add(handle)
    return handle


@ffi.callback("void(int, const char*, size_t, void*)")
def _on_reply(ret, char_p, length, user_data):
    ret = int(ret)
    if ret == RET_STALE_WARN:
        return

    state = ffi.from_handle(user_data)
    msg = ffi.buffer(char_p, length)[:] if char_p != ffi.NULL else b""

    # RET_OK carries a CBOR text string, RET_ERR the raw error text.
    if ret == 0:
        try:
            text = cbor2.loads(msg)
            if not isinstance(text, str):
                raise TypeError(f"expected string, got {type(text).__name__}")
        except Exception as exc:
            ret = -1
            text = f"invalid CBOR reply: {exc}"
    else:
        text = msg.decode("utf-8", errors="replace")

    state["ret"] = ret
    state["text"] = text
    state["done"].set()
    _global_set.discard(user_data)


@ffi.callback("void(int, const char*, size_t, void*)")
def _event_callback(ret, char_p, length, user_data):
    msg = ffi.buffer(char_p, length)[:] if char_p != ffi.NULL else b""
    ffi.from_handle(user_data)(int(ret), msg)


# Every event the library emits. Since 0.3.0 a listener is registered per event
# name, so an `event_cb` that wants them all registers once per name.
EVENT_NAMES = (
    "onMessageSent",
    "onMessageError",
    "onMessageQueued",
    "onMessagePropagated",
    "onMessageReceived",
    "onConnectionStatusChange",
    "onTopicHealthChange",
    "onConnectionChange",
    "onReceivedMessage",
    "onChannelMessageReceived",
    "onChannelMessageSent",
    "onChannelMessageError",
    "onChannelMessageLost",
)


def _encode_request(fields: dict):
    payload = cbor2.dumps(fields)
    return ffi.new("uint8_t[]", payload), len(payload)


def _new_cb_state():
    return {
        "done": threading.Event(),
        "ret": None,
        "text": "",
    }


def _await_reply(state, op_name: str, timeout_s: float) -> Result[str, str]:
    """Waits for the terminal reply. Ok carries the reply text."""
    if not state["done"].wait(timeout_s):
        return Err(f"{op_name}: timeout after {timeout_s}s")

    if state["ret"] != 0:
        return Err(f"{op_name}: callback failed (ret={state['ret']}): {state['text']}")

    return Ok(state["text"])


def _immediate_failure(op_name: str, rc: int, state) -> str:
    """Non-zero return: the callback already ran synchronously with the reason."""
    reason = state["text"] if state["done"].is_set() else ""
    return f"{op_name}: immediate call failed (ret={rc})" + (
        f": {reason}" if reason else ""
    )


def _json_reply(op_name: str, text: str) -> Result:
    try:
        return Ok(json.loads(text))
    except ValueError as e:
        return Err(f"{op_name}: invalid JSON reply: {e}")


def version() -> str:
    """Version and git commit hash of the loaded library. Needs no node."""
    return ffi.string(lib.logosdelivery_version()).decode("utf-8")


def shutdown() -> Result[int, str]:
    """Stops every node the library still holds and joins their threads.

    Call it before the process exits while a node is still alive. Afterwards
    destroy() on a remaining NodeWrapper returns Err."""
    rc = lib.logosdelivery_shutdown()
    if rc != 0:
        return Err(f"shutdown: a node was left running (ret={rc})")

    return Ok(rc)


class NodeWrapper:
    def __init__(self, ctx, config_buffer, event_cb_handler, listener_ids=()):
        self.ctx = ctx
        self._config_buffer = config_buffer
        self._event_cb_handler = event_cb_handler
        self._listener_ids = tuple(listener_ids)

    @classmethod
    def create_node(
        cls,
        config: dict,
        event_cb=None,
        *,
        timeout_s: float = 20.0,
    ) -> Result["NodeWrapper", str]:
        config_json = json.dumps(config, separators=(",", ":"), ensure_ascii=False)
        config_buffer, config_len = _encode_request({"configJson": config_json})

        state = _new_cb_state()
        user_data = _new_handle(state)

        ctx = lib.logosdelivery_create_node(
            config_buffer, config_len, _on_reply, user_data
        )

        reply = _await_reply(state, "create_node", timeout_s)
        if reply.is_err():
            # The library claims the context before the constructor runs, and
            # only destroy releases it.
            if ctx != ffi.NULL:
                lib.logosdelivery_destroy(ctx)
            return Err(reply.err())

        event_cb_handler = None
        listener_ids = []
        if event_cb is not None:
            event_cb_handler = _new_handle(event_cb)
            for event_name in EVENT_NAMES:
                listener_id = lib.logosdelivery_add_event_listener(
                    ctx,
                    event_name.encode("utf-8"),
                    _event_callback,
                    event_cb_handler,
                )
                if listener_id == 0:
                    cls(ctx, config_buffer, event_cb_handler, listener_ids).destroy()
                    return Err(f"create_node: add_event_listener({event_name}) failed")
                listener_ids.append(listener_id)

        return Ok(cls(ctx, config_buffer, event_cb_handler, listener_ids))

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
            # The caller drops the node here, so tear it down before its
            # callbacks outlive the wrapper that owns them.
            node.destroy(timeout_s=timeout_s)
            return Err(start_result.err())

        return Ok(node)

    def _request(
        self, c_fn, op_name: str, fields: dict, timeout_s: float
    ) -> Result[str, str]:
        """Sends one request and waits for its terminal reply. Ok carries the
        reply text."""
        state = _new_cb_state()
        req, req_len = _encode_request(fields)
        user_data = _new_handle(state)
        rc = c_fn(self.ctx, _on_reply, user_data, req, req_len)
        if rc != 0:
            return Err(_immediate_failure(op_name, rc, state))

        return _await_reply(state, op_name, timeout_s)

    def start_node(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        return self._request(
            lib.logosdelivery_start_node, "start_node", {}, timeout_s
        ).map(lambda _: 0)

    def stop_node(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        return self._request(
            lib.logosdelivery_stop_node, "stop_node", {}, timeout_s
        ).map(lambda _: 0)

    def destroy(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        if self.ctx == ffi.NULL:
            return Ok(0)

        # Drop the listeners first so the event thread cannot reach the Python
        # callback once the context is gone.
        for listener_id in self._listener_ids:
            lib.logosdelivery_remove_event_listener(self.ctx, listener_id)
        self._listener_ids = ()

        rc = lib.logosdelivery_destroy(self.ctx)
        if rc != 0:
            return Err(f"destroy: call failed (ret={rc})")

        self.ctx = ffi.NULL
        _global_set.discard(self._event_cb_handler)
        return Ok(rc)

    def stop_and_destroy(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        stop_result = self.stop_node(timeout_s=timeout_s)

        # Destroy even when the stop fails: a node that keeps its context alive
        # also keeps calling back into a wrapper the caller is about to drop.
        destroy_result = self.destroy(timeout_s=timeout_s)

        if stop_result.is_err():
            return Err(stop_result.err())

        return destroy_result

    def get_connection_status(self, *, timeout_s: float = 20.0) -> Result[str, str]:
        # `Disconnected`, `PartiallyConnected` or `Connected`.
        return self._request(
            lib.logosdelivery_get_connection_status,
            "get_connection_status",
            {},
            timeout_s,
        )

    def subscribe_content_topic(
        self, content_topic: str, *, timeout_s: float = 20.0
    ) -> Result[int, str]:
        return self._request(
            lib.logosdelivery_subscribe,
            f"subscribe({content_topic})",
            {"contentTopicStr": content_topic},
            timeout_s,
        ).map(lambda _: 0)

    def unsubscribe_content_topic(
        self, content_topic: str, *, timeout_s: float = 20.0
    ) -> Result[int, str]:
        return self._request(
            lib.logosdelivery_unsubscribe,
            f"unsubscribe({content_topic})",
            {"contentTopicStr": content_topic},
            timeout_s,
        ).map(lambda _: 0)

    def send_message(
        self, message: dict, *, timeout_s: float = 20.0
    ) -> Result[str, str]:
        """Ok carries the request id."""
        message_json = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
        return self._request(
            lib.logosdelivery_send,
            "send_message",
            {"messageJson": message_json},
            timeout_s,
        )

    def get_available_node_info_ids(
        self, *, timeout_s: float = 20.0
    ) -> Result[list[str], str]:
        op_name = "get_available_node_info_ids"
        return self._request(
            lib.logosdelivery_get_available_node_info_ids, op_name, {}, timeout_s
        ).and_then(lambda text: _json_reply(op_name, text))

    def get_node_info(
        self, node_info_id: str, *, timeout_s: float = 20.0
    ) -> Result[str, str]:
        # The item is a plain string, not JSON: a peer id, an ENR URI, a
        # comma-separated multiaddress list or the Prometheus metrics text.
        # MyMixPubKey is legitimately empty when mix is not mounted.
        return self._request(
            lib.logosdelivery_get_node_info,
            f"get_node_info({node_info_id})",
            {"nodeInfoId": node_info_id},
            timeout_s,
        )

    def get_available_configs(self, *, timeout_s: float = 20.0) -> Result[dict, str]:
        op_name = "get_available_configs"
        return self._request(
            lib.logosdelivery_get_available_configs, op_name, {}, timeout_s
        ).and_then(lambda text: _json_reply(op_name, text))

    def set_service_discovery_plugin(
        self, plugin_ptr: int, *, timeout_s: float = 20.0
    ) -> Result[str, str]:
        """plugin_ptr is the raw address of an `LdServiceDiscoveryPlugin`,
        borrowed for the call."""
        return self._request(
            lib.logosdelivery_set_service_discovery_plugin,
            "set_service_discovery_plugin",
            {"pluginPtr": plugin_ptr},
            timeout_s,
        )

    def get_discovery_requirements(
        self, *, timeout_s: float = 20.0
    ) -> Result[dict, str]:
        # {"externalServiceDiscovery": bool, "bootstrapNodes": [multiaddr, ...]}
        op_name = "get_discovery_requirements"
        return self._request(
            lib.logosdelivery_get_discovery_requirements, op_name, {}, timeout_s
        ).and_then(lambda text: _json_reply(op_name, text))

    def clear_service_discovery_plugin(
        self, *, timeout_s: float = 20.0
    ) -> Result[str, str]:
        return self._request(
            lib.logosdelivery_clear_service_discovery_plugin,
            "clear_service_discovery_plugin",
            {},
            timeout_s,
        )

    def destroy_keep_ctx(self, *, timeout_s: float = 20.0) -> Result[int, str]:
        """Destroy the node without nilling self.ctx afterwards.

        Lets a library-contract test reach the C side with a dangling-but-non-nil
        pointer, instead of relying on the binding's defensive nil-out.
        """
        rc = lib.logosdelivery_destroy(self.ctx)
        if rc != 0:
            return Err(f"destroy_keep_ctx: call failed (ret={rc})")

        _global_set.discard(self._event_cb_handler)
        return Ok(rc)

    def channel_create(
        self,
        channel_id: str,
        content_topic: str,
        sender_id: str,
        *,
        encrypt_fn: int = 0,
        decrypt_fn: int = 0,
        crypto_user_data: int = 0,
        timeout_s: float = 20.0,
    ) -> Result[str, str]:
        """encrypt_fn, decrypt_fn and crypto_user_data are raw addresses. The
        library uses them until destroy(). The caller keeps them valid until then."""
        return self._request(
            lib.logosdelivery_channel_create,
            f"channel_create({channel_id})",
            {
                "channelIdStr": channel_id,
                "contentTopicStr": content_topic,
                "senderIdStr": sender_id,
                "encryptFn": encrypt_fn,
                "decryptFn": decrypt_fn,
                "userData": crypto_user_data,
            },
            timeout_s,
        )

    def channel_exists(
        self, channel_id: str, *, timeout_s: float = 20.0
    ) -> Result[bool, str]:
        # A missing channel is `"false"`, not an error.
        return self._request(
            lib.logosdelivery_channel_exists,
            f"channel_exists({channel_id})",
            {"channelIdStr": channel_id},
            timeout_s,
        ).map(lambda text: text == "true")

    def channel_send(
        self, channel_id: str, message: dict, *, timeout_s: float = 20.0
    ) -> Result[str, str]:
        message_json = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
        return self._request(
            lib.logosdelivery_channel_send,
            f"channel_send({channel_id})",
            {"channelIdStr": channel_id, "messageJson": message_json},
            timeout_s,
        )

    def channel_close(
        self, channel_id: str, *, timeout_s: float = 20.0
    ) -> Result[str, str]:
        return self._request(
            lib.logosdelivery_channel_close,
            f"channel_close({channel_id})",
            {"channelIdStr": channel_id},
            timeout_s,
        )
