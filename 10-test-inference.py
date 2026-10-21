"""
Production MLOps Inference Test Client
Sends structured prediction requests to a Seldon Core Ingress service,
validates HTTP response codes, parses class probabilities, and benchmarks latency.
"""

import argparse
import json
import time
import urllib.error
import urllib.request

DEFAULT_URL = "http://flighs-ontime.192.168.49.2.nip.io/api/v1.0/predictions"

# Default feature vector for Chapter 10 Flight Delay Model
DEFAULT_PAYLOAD = {
    "data": {
        "names": [
            "month", "day", "day_of_week", "airline", "flight_number",
            "origin_airport", "destination_airport", "scheduled_time",
            "elapsed_time", "air_time", "distance", "ORIG_CITY",
            "ORIG_STATE", "ORIG_LATITUDE", "ORIG_LONGITUDE", "DEST_CITY",
            "DEST_STATE", "DEST_LATITUDE", "DEST_LONGITUDE",
            "scheduled_departure_hour", "scheduled_departure_minute",
            "scheduled_arrival_hour", "scheduled_arrival_minute"
        ],
        "ndarray": [
            [11, 30, 1, "WN", 552, "LGA", "DAL", 250.0, 244.0, 221.0, 1381,
             "New York", "NY", 40.77724, -73.87261, "Dallas", "TX", 32.84711, -96.85177,
             6, 15, 9, 25]
        ]
    }
}


def send_prediction(url, payload, timeout=10):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    start_time = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency_ms = (time.time() - start_time) * 1000.0
            body = json.loads(resp.read().decode("utf-8"))
            return {
                "success": True,
                "status_code": resp.getcode(),
                "latency_ms": latency_ms,
                "response": body
            }
    except urllib.error.HTTPError as e:
        latency_ms = (time.time() - start_time) * 1000.0
        err_body = e.read().decode("utf-8") if e.fp else str(e)
        return {
            "success": False,
            "status_code": e.code,
            "latency_ms": latency_ms,
            "error": err_body
        }
    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000.0
        return {
            "success": False,
            "status_code": None,
            "latency_ms": latency_ms,
            "error": str(e)
        }


def main():
    parser = argparse.ArgumentParser(description="Test Seldon Core Model Inference Endpoint")
    parser.add_argument("--url", default=DEFAULT_URL, help="Inference URL endpoint")
    parser.add_argument("--count", type=int, default=5, help="Number of requests to send")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay in seconds between requests")
    parser.add_argument("--timeout", type=float, default=10.0, help="Per-request timeout in seconds")
    args = parser.parse_args()

    print("=" * 70)
    print(f"Target URL: {args.url}")
    print(f"Sending {args.count} prediction requests (delay={args.delay}s, timeout={args.timeout}s)...")
    print("=" * 70)

    latencies = []
    successes = 0

    for i in range(args.count):
        res = send_prediction(args.url, DEFAULT_PAYLOAD, timeout=args.timeout)
        if res["success"]:
            successes += 1
            latencies.append(res["latency_ms"])
            pred = res["response"].get("data", {}).get("ndarray", [])
            names = res["response"].get("data", {}).get("names", [])
            print(f"[{i+1}/{args.count}] HTTP {res['status_code']} | Latency: {res['latency_ms']:.1f}ms | Classes: {names} -> {pred}")
        else:
            print(f"[{i+1}/{args.count}] FAILED | HTTP {res['status_code']} | Error: {res.get('error')}")

        if i < args.count - 1 and args.delay > 0:
            time.sleep(args.delay)

    print("=" * 70)
    print("Benchmark Summary:")
    print(f"Total Requests: {args.count}")
    print(f"Successful:     {successes}/{args.count} ({successes/args.count*100:.1f}%)")
    if latencies:
        print(f"Latency Min:    {min(latencies):.1f} ms")
        print(f"Latency Avg:    {sum(latencies)/len(latencies):.1f} ms")
        print(f"Latency Max:    {max(latencies):.1f} ms")
    print("=" * 70)


if __name__ == "__main__":
    main()
