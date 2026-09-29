# Logos Delivery Python Bindings

Python bindings for `liblogosdelivery`, the C library of [logos-delivery](https://github.com/logos-messaging/logos-delivery). `waku/wrapper.py` binds its C ABI with cffi; `NodeWrapper` is the entry point.

The binding loads the library from `lib/` next to the `waku` package, so use it from a checkout of this repo.

## Set up

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requiremets.txt
```

## Build the library

`vendor/logos-delivery` pins the logos-delivery commit this binding targets. Build `liblogosdelivery` there (prerequisites in logos-delivery's [`library/BUILD.md`](https://github.com/logos-messaging/logos-delivery/blob/master/library/BUILD.md)) and copy it into `lib/`, where `wrapper.py` loads it as `lib/liblogosdelivery.so`:

```bash
git submodule update --init vendor/logos-delivery
make -C vendor/logos-delivery liblogosdelivery
cp vendor/logos-delivery/build/liblogosdelivery.so lib/
```

On macOS the build produces `liblogosdelivery.dylib`, so copy that and link it under the `.so` name:

```bash
cp vendor/logos-delivery/build/liblogosdelivery.dylib lib/
ln -sf liblogosdelivery.dylib lib/liblogosdelivery.so
```

## Use

From the repository root:

```python
from waku import NodeWrapper, version

print(version())

node = NodeWrapper.create_and_start(
    {"mode": "Core", "clusterId": 198, "numShardsInNetwork": 1},
    event_cb=lambda ret, msg: print(msg.decode()),
).unwrap()
print(node.get_connection_status().unwrap())
node.stop_and_destroy()
```

Every method returns a `Result` from the [`result`](https://pypi.org/project/result/) package: `Ok` with the reply, or `Err` with the reason. `event_cb` runs on the library's event thread for every event in `EVENT_NAMES`.

## Update logos-delivery

Check out the new commit in `vendor/logos-delivery`, rebuild the library as above, and update `waku/wrapper.py` if the C ABI changed.
