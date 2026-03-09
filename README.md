
# Waku Python Bindings

## Introduction

Exposes a Waku module that can be used within Python projects.
It is fundamentally a wrapper around [nwaku](https://github.com/waku-org/nwaku)

This repo has been tested with Python3 in Ubuntu.

If need a different setup, don't hesitate con contact us on Discord. The Discord server can be found at https://docs.waku.org/.

## Prepare the development environment

Run the following commands from the root folder:
```bash
mkdir venv
python3 -m venv venv/
source venv/bin/activate
./venv/bin/python -m pip install -r requiremets.txt
```

## Create a Py-Waku package

Run the following commands from the root folder:
```bash
source venv/bin/activate
./venv/bin/python3 -m build
```

## Test the package

For that, we have a very simple example in `tests/waku_example.py`.

In order to use the waku module, please install it from the local `dist/` folder, that can be created by following the
instructions from the previous section.

The following command is an example on how to install the local
package to your local virtual env.

```bash
./venv/bin/python3 -m pip install dist/waku-0.0.1-cp310-cp310-linux_x86_64.whl
```

Current limitations of nwaku cbindings do not allow you to use DNS name in the multiaddress of a peer you want to connect to.
Due to that, we recommend to run another local node to connect to other peers and then connect to this local node from the py-waku.

```bash
docker run -i -t -p 60000:60000 -p 9000:9000/udp -p 8646:8645 harbor.status.im/wakuorg/nwaku:v0.24.0 --dns-discovery:true --dns-discovery-url:enrtree://ANEDLO25QVUGJOUTQFRYKWX6P4Z4GKVESBMHML7DZ6YK4LGS5FC5O@prod.wakuv2.nodes.status.im --discv5-discovery --rest --rest-address=0.0.0.0
```

Once this node is up, get the multiaddress

```bash
LOCAL_PEER_MA=$(curl http://127.0.0.1:8646/debug/v1/info | jq -r ".listenAddresses[0]")
NODEKEY=$(openssl rand -hex 32)
```

You can run the `tests/waku_example.py` now as

```bash
./venv/bin/python3 tests/waku_example.py --peer ${LOCAL_PEER_MA} --key ${NODEKEY} -p 70000
```

Apart from seeing messages going through the relay protocol, you can also publish a message and see it being received by the py-waku node

```bash
curl http://127.0.0.1:8646/relay/v1/messages/%2Fwaku%2F2%2Fdefault-waku%2Fproto -H "Content-Type: application/json" -d '{"payload": "'$(echo "Hello!" | base64)'", "contentTopic": "/hello/0/pywaku/plain"}'
```

## Update the libwaku.so library

Given that `Py-Waku` conforms a wrapper around `libwaku.so`,
it is likely that you would need to upgrade it.
For that, you will need to update the submodule pointer
to a more recent nwaku version:

1. ```cd vendor/nwaku```
2. Check out to the commit/tag as you wish

Then, follow the following steps from the root folder
to rebuild the `libwaku.so` library:

```bash
cd vendor/liblogosdelivery
```
```bash
make liblogosdelivery -j8
```
```bash
cd ../../
```
```bash
cp vendor/liblogosdelivery/build/liblogosdelivery.so lib/
```

