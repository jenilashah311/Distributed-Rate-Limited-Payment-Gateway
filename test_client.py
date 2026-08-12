import time
import uuid
import threading
import requests

BASE_URL = "http://localhost:8082/payments"

def run_tests():
    print("==================================================")
    # 1. Test Single Request (Cache MISS)
    print("Test 1: Single request (Expected Cache MISS)...")
    key_1 = str(uuid.uuid4())
    headers = {
        "Idempotency-Key": key_1,
        "X-Client-Id": "client-test-1",
        "Content-Type": "application/json"
    }
    payload = {"amount": 150.0, "currency": "USD"}
    
    response = requests.post(BASE_URL, json=payload, headers=headers)
    print(f"Status: {response.status_code}")
    print(f"X-Cache: {response.headers.get('X-Cache')}")
    print(f"Response: {response.text}")
    print("--------------------------------------------------")

    # 2. Test Duplicate Request (Cache HIT)
    print("Test 2: Duplicate request with same key (Expected Cache HIT)...")
    response_dup = requests.post(BASE_URL, json=payload, headers=headers)
    print(f"Status: {response_dup.status_code}")
    print(f"X-Cache: {response_dup.headers.get('X-Cache')}")
    print(f"Response: {response_dup.text}")
    print("--------------------------------------------------")

    # 3. Test Concurrent Requests (Race Handling)
    print("Test 3: Concurrent duplicate requests (Expected 1 MISS, rest HIT)...")
    key_2 = str(uuid.uuid4())
    headers_concurrent = {
        "Idempotency-Key": key_2,
        "X-Client-Id": "client-test-2",
        "Content-Type": "application/json"
    }
    
    results = []
    threads = []
    
    def send_request():
        try:
            res = requests.post(BASE_URL, json=payload, headers=headers_concurrent)
            results.append((res.status_code, res.headers.get("X-Cache")))
        except Exception as e:
            results.append((0, str(e)))

    # Launch 5 concurrent threads
    for _ in range(5):
        t = threading.Thread(target=send_request)
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    for i, res in enumerate(results):
        print(f"Thread {i+1} - Status: {res[0]}, X-Cache: {res[1]}")
    print("--------------------------------------------------")

    # 4. Test Sliding-Window Rate Limiting (Max 10 requests per 10 seconds)
    print("Test 4: Rate limit testing (Expect 429 Too Many Requests after 10 requests)...")
    client_id = f"client-rate-{uuid.uuid4()}" # Use fresh client to avoid mixing counts
    
    for i in range(12):
        headers_rate = {
            "Idempotency-Key": str(uuid.uuid4()),
            "X-Client-Id": client_id,
            "Content-Type": "application/json"
        }
        res = requests.post(BASE_URL, json=payload, headers=headers_rate)
        print(f"Request {i+1} - Status: {res.status_code}, Response: {res.json() if res.status_code == 429 else 'SUCCESS'}")
    print("==================================================")

if __name__ == "__main__":
    run_tests()
