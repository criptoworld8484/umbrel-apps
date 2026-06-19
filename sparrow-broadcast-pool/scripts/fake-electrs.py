#!/usr/bin/env python3
"""Minimal fake electrs for handshake testing."""
import json
import socket
import threading

def handle(conn):
    buf = b""
    while True:
        data = conn.recv(4096)
        if not data:
            break
        buf += data
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(msg, list):
                reqs = msg
            else:
                reqs = [msg]
            resps = []
            for req in reqs:
                mid = req.get("method")
                rid = req.get("id")
                if mid == "server.version":
                    resps.append({"jsonrpc": "2.0", "result": ["electrs 0.9.14", "1.4"], "id": rid})
                elif mid == "server.features":
                    resps.append({
                        "jsonrpc": "2.0",
                        "result": {
                            "genesis_hash": "00000008819873e925632181568121be59ecd5df7a9c348375d874564ae96f681",
                            "protocol_max": "1.4",
                            "protocol_min": "1.0",
                        },
                        "id": rid,
                    })
                elif mid == "blockchain.headers.subscribe":
                    resps.append({
                        "jsonrpc": "2.0",
                        "result": {
                            "height": 309255,
                            "hex": "00" * 160,
                        },
                        "id": rid,
                    })
                else:
                    resps.append({"jsonrpc": "2.0", "error": {"code": -32601, "message": "unknown"}, "id": rid})
            out = resps[0] if len(resps) == 1 else resps
            conn.sendall((json.dumps(out) + "\n").encode())

def main():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 59999))
    s.listen(5)
    print("fake electrs on 59999", flush=True)
    while True:
        c, _ = s.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()

if __name__ == "__main__":
    main()
