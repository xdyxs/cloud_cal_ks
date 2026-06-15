import os
import socket
import redis
from flask import Flask, jsonify
import pandas as pd
import numpy as np

app = Flask(__name__)

REDIS_HOST = os.environ.get("REDIS_HOST", "redis-svc")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")


def get_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
    )


@app.route("/api/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok"})


@app.route("/api/counter", methods=["GET"])
def counter():
    try:
        r = get_redis()
        count = r.incr("visit_count")
        return jsonify({"count": count, "host": socket.gethostname()})
    except Exception as e:
        return jsonify({"error": str(e), "host": socket.gethostname()}), 500


@app.route("/api/data", methods=["GET"])
def data_sample():
    df = pd.DataFrame({"x": np.random.rand(5), "y": np.random.rand(5)})
    return jsonify({"data": df.to_dict(orient="records")})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
