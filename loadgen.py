#!/usr/bin/env python3
import asyncio
import hashlib
import json
import random
import time
from collections import defaultdict
from typing import Dict, Tuple

import aiohttp

############################################################
# CONFIG
############################################################

ORDER_IP = "142.1.46.58"
ORDER_PORT = 13000

RUN_CORRECTNESS_SUITE = True
RUN_LOAD_PHASE = True
AUTO_TEST_TPS = True

DURATION_SECONDS = 600
TARGET_TPS = 100
WORKERS = 512

REQUEST_TIMEOUT = 5

SEED_USERS = 2000
SEED_PRODUCTS = 2000

WAIT_AFTER_SEED_SECONDS = 2
VERIFY_SEED_SAMPLE = True

# workload mix must sum to 100
MIX_USER_GET = 15
MIX_PRODUCT_GET = 15
MIX_ORDER_POST = 15
MIX_USER_PURCHASED = 10
MIX_USER_UPDATE = 10
MIX_PRODUCT_UPDATE = 10
MIX_USER_CREATE = 5
MIX_USER_DELETE = 5
MIX_PRODUCT_CREATE = 5
MIX_PRODUCT_DELETE = 10

TPS_LEVELS = [4000]
# TPS_LEVELS = [2, 10, 25, 50, 100, 175, 250, 500, 750, 1000, 2500, 4000, 8000, 10000, 20000]

GRADE_THRESHOLDS = {
    2: 0.2,
    10: 1,
    25: 1,
    50: 1,
    100: 1,
    175: 1,
    250: 1,
    500: 1,
    750: 1,
    1000: 1,
    2500: 1,
    4000: 1,
}

PERSIST_USER_ID = 990001
PERSIST_PRODUCT_ID = 990002
PERSIST_USER_EMAIL = "persist-user@test.com"
PERSIST_USER_PASSWORD_RAW = "persistpw"
PERSIST_PRODUCT_NAME = "persist-product"

MAX_FAILURE_PRINTS = 10

############################################################
# HELPERS
############################################################

async def require(
    session: aiohttp.ClientSession,
    *,
    method: str,
    path: str,
    payload=None,
    expected_desc: str,
    check,
):
    resp = await do_request(session, method, base_url(path), payload)

    ok, detail = check(resp)

    if not ok:
        print("\n================ CORRECTNESS FAILURE ================")
        print(f"Request       : {method} {path}")
        print(f"Payload       : {json.dumps(payload, ensure_ascii=False) if payload is not None else 'None'}")
        print(f"Expected      : {expected_desc}")
        print(f"Actual Status : {resp.status}")
        print(f"Actual Body   : {resp.body}")
        if detail:
            print(f"Why Failed    : {detail}")
        print("====================================================\n")
        raise RuntimeError(f"{method} {path} failed")

    return resp

def base_url(path: str) -> str:
    return f"http://{ORDER_IP}:{ORDER_PORT}{path}"

def hash_password(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest().upper()

def valid_order_error(body: str, expected: str) -> bool:
    try:
        obj = json.loads(body)
        return obj.get("status") == expected
    except Exception:
        return False

def parse_user(body: str) -> Tuple[dict, bool]:
    try:
        obj = json.loads(body)
        return obj, isinstance(obj, dict) and "id" in obj
    except Exception:
        return {}, False

def parse_product(body: str) -> Tuple[dict, bool]:
    try:
        obj = json.loads(body)
        return obj, isinstance(obj, dict) and "id" in obj
    except Exception:
        return {}, False

def parse_order(body: str) -> Tuple[dict, bool]:
    try:
        obj = json.loads(body)
        return obj, isinstance(obj, dict) and obj.get("status") is not None
    except Exception:
        return {}, False

def parse_purchased(body: str) -> Tuple[dict, bool]:
    try:
        obj = json.loads(body)
        return obj, isinstance(obj, dict)
    except Exception:
        return {}, False

############################################################
# HTTP
############################################################

class HttpResponse:
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body

async def do_request(session: aiohttp.ClientSession, method: str, url: str, body=None) -> HttpResponse:
    try:
        if body is None:
            async with session.request(method, url) as resp:
                text = await resp.text()
                return HttpResponse(resp.status, text)
        else:
            async with session.request(method, url, json=body) as resp:
                text = await resp.text()
                return HttpResponse(resp.status, text)
    except Exception as e:
        return HttpResponse(500, str(e))

############################################################
# FAILURE REPORTING
############################################################

class FailureReporter:
    def __init__(self, max_prints: int = MAX_FAILURE_PRINTS):
        self.max_prints = max_prints
        self.printed = 0
        self.counts = defaultdict(int)
        self.lock = asyncio.Lock()

    async def report(
        self,
        *,
        reason: str,
        api: str,
        method: str,
        payload,
        expected: str,
        actual_status: int,
        actual_body: str,
    ):
        async with self.lock:
            self.counts[reason] += 1
            if self.printed >= self.max_prints:
                return
            self.printed += 1

            print("\n================ FAILED TEST CASE ================")
            print(f"Reason        : {reason}")
            print(f"Request API   : {method} {api}")
            print(f"Payload       : {json.dumps(payload, ensure_ascii=False) if payload is not None else 'None'}")
            print(f"Expected      : {expected}")
            print(f"Actual Status : {actual_status}")
            print(f"Actual Body   : {actual_body}")
            print("==================================================\n")

    def print_summary(self):
        if not self.counts:
            print("\nNo failed test cases were recorded.\n")
            return

        print("\n================ FAILURE SUMMARY ================\n")
        for reason, count in sorted(self.counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"{reason}: {count}")
        print()

############################################################
# PURCHASE TRACKER
############################################################

class PurchaseTracker:
    def __init__(self):
        self._data: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    async def add(self, user_id: int, product_id: int, qty: int):
        async with self._lock:
            self._data[user_id][product_id] += qty

    async def snapshot(self, user_id: int) -> Dict[int, int]:
        async with self._lock:
            return dict(self._data.get(user_id, {}))

############################################################
# DYNAMIC CREATE/DELETE STORE
############################################################

class DynamicStore:
    _global_user_id = 300000
    _global_product_id = 400000
    _global_id_lock = asyncio.Lock()

    def __init__(self):
        self._users: Dict[int, dict] = {}
        self._products: Dict[int, dict] = {}
        self._lock = asyncio.Lock()

    async def allocate_user_payload(self):
        async with DynamicStore._global_id_lock:
            uid = DynamicStore._global_user_id
            DynamicStore._global_user_id += 1

        return {
            "command": "create",
            "id": uid,
            "username": f"dynu{uid}",
            "email": f"dynu{uid}@test.com",
            "password": "pw",
        }

    async def allocate_product_payload(self):
        async with DynamicStore._global_id_lock:
            pid = DynamicStore._global_product_id
            DynamicStore._global_product_id += 1

        return {
            "command": "create",
            "id": pid,
            "name": f"dynp{pid}",
            "description": "dynamic",
            "price": 10.00,
            "quantity": 100,
        }

    async def mark_user_created(self, payload: dict):
        async with self._lock:
            self._users[payload["id"]] = payload

    async def mark_product_created(self, payload: dict):
        async with self._lock:
            self._products[payload["id"]] = payload

    async def pop_user_delete_payload(self):
        async with self._lock:
            if not self._users:
                return None
            uid, created = self._users.popitem()
            return {
                "command": "delete",
                "id": uid,
                "username": created["username"],
                "email": created["email"],
                "password": created["password"],
            }

    async def pop_product_delete_payload(self):
        async with self._lock:
            if not self._products:
                return None
            pid, created = self._products.popitem()
            return {
                "command": "delete",
                "id": pid,
                "name": created["name"],
                "price": created["price"],
                "quantity": created["quantity"],
            }

############################################################
# CORRECTNESS
############################################################

async def correctness_suite(session: aiohttp.ClientSession):
    print("Running correctness suite...")

    uid = 910001
    pid = 920001

    payload = {
        "command": "create",
        "id": uid,
        "username": "tester",
        "email": "tester@example.com",
        "password": "secret",
    }

    resp = await require(
        session,
        method="POST",
        path="/user",
        payload=payload,
        expected_desc="status=200 and created user with correct fields",
        check=lambda resp: (
            (lambda u_ok: (
                True, ""
            ) if (
                resp.status == 200
                and (u := parse_user(resp.body))[1]
                and u[0].get("id") == uid
                and u[0].get("username") == "tester"
                and u[0].get("email") == "tester@example.com"
                and u[0].get("password") == hash_password("secret")
            ) else (False, "status/body did not match expected user payload"))(True)
        )
    )

    u, ok = parse_user(resp.body)
    if not ok or u.get("id") != uid or u.get("username") != "tester" or u.get("email") != "tester@example.com" or u.get("password") != hash_password("secret"):
        raise RuntimeError(f"create user bad body={resp.body}")

    resp = await do_request(session, "GET", base_url(f"/user/{uid}"))
    if resp.status != 200:
        raise RuntimeError(f"get user status={resp.status} body={resp.body}")

    u, ok = parse_user(resp.body)
    if not ok or u.get("id") != uid:
        raise RuntimeError(f"get user bad body={resp.body}")

    resp = await do_request(session, "POST", base_url("/product"), {
        "command": "create",
        "id": pid,
        "name": "widget",
        "description": "desc",
        "price": 10.00,
        "quantity": 20,
    })
    if resp.status != 200:
        raise RuntimeError(f"create product status={resp.status} body={resp.body}")

    p, ok = parse_product(resp.body)
    if not ok or p.get("id") != pid or p.get("quantity") != 20:
        raise RuntimeError(f"create product bad body={resp.body}")

    resp = await do_request(session, "POST", base_url("/order"), {
        "command": "place order",
        "user_id": uid,
        "product_id": pid,
        "quantity": 3,
    })
    if resp.status != 200:
        raise RuntimeError(f"place order status={resp.status} body={resp.body}")

    order, ok = parse_order(resp.body)
    if not ok or order.get("product_id") != pid or order.get("user_id") != uid or order.get("quantity") != 3 or order.get("status") != "Success":
        raise RuntimeError(f"place order bad body={resp.body}")

    resp = await do_request(session, "GET", base_url(f"/user/purchased/{uid}"))
    if resp.status != 200:
        raise RuntimeError(f"user/purchased status={resp.status} body={resp.body}")

    pm, ok = parse_purchased(resp.body)
    if not ok or pm.get(str(pid)) != 3:
        raise RuntimeError(f"user/purchased bad body={resp.body}")

    resp = await do_request(session, "POST", base_url("/order"), {
        "command": "place order",
        "user_id": uid,
        "product_id": pid,
        "quantity": 999999,
    })
    if resp.status != 400 or not valid_order_error(resp.body, "Exceeded quantity limit"):
        raise RuntimeError(f"expected exceeded quantity, got status={resp.status} body={resp.body}")

    resp = await do_request(session, "POST", base_url("/user"), {
        "command": "placeeee oooorder",
        "id": 123,
        "username": "x",
        "email": "x@test.com",
        "password": "pw",
    })
    if resp.status != 404:
        raise RuntimeError(f"expected invalid command 404, got status={resp.status} body={resp.body}")

    resp = await do_request(session, "POST", base_url("/product"), {
        "command": "create",
        "id": 999100,
        "name": "bad-product",
        "price": 10.00,
        "quantity": 1,
    })
    if resp.status != 400:
        raise RuntimeError(f"expected missing field 400, got status={resp.status} body={resp.body}")

    resp = await do_request(session, "GET", base_url("/user/not-an-int"))
    if resp.status != 400:
        raise RuntimeError(f"expected malformed GET 400, got status={resp.status} body={resp.body}")

    resp = await do_request(session, "GET", base_url("/does-not-exist"))
    if resp.status != 404:
        raise RuntimeError(f"expected bad route 404, got status={resp.status} body={resp.body}")

    resp = await do_request(session, "POST", base_url("/user"), {
        "command": "create",
        "id": PERSIST_USER_ID,
        "username": "persist-user",
        "email": PERSIST_USER_EMAIL,
        "password": PERSIST_USER_PASSWORD_RAW,
    })
    if resp.status not in (200, 409):
        raise RuntimeError(f"persist user create failed status={resp.status} body={resp.body}")

    resp = await do_request(session, "POST", base_url("/product"), {
        "command": "create",
        "id": PERSIST_PRODUCT_ID,
        "name": PERSIST_PRODUCT_NAME,
        "description": "left in system for DB check",
        "price": 10.00,
        "quantity": 500,
    })
    if resp.status not in (200, 409):
        raise RuntimeError(f"persist product create failed status={resp.status} body={resp.body}")

    resp = await do_request(session, "POST", base_url("/order"), {
        "command": "place order",
        "user_id": PERSIST_USER_ID,
        "product_id": PERSIST_PRODUCT_ID,
        "quantity": 2,
    })
    if resp.status != 200:
        raise RuntimeError(f"persist order failed status={resp.status} body={resp.body}")

    print("Correctness suite passed.")
    print("Persistence markers created:")
    print(f"  user_id={PERSIST_USER_ID}, email={PERSIST_USER_EMAIL}")
    print(f"  product_id={PERSIST_PRODUCT_ID}, name={PERSIST_PRODUCT_NAME}")

############################################################
# SEED
############################################################

async def seed_users_and_products(session: aiohttp.ClientSession):
    print("Seeding users/products through OrderService...")

    async def seed_user(uid: int):
        payload = {
            "command": "create",
            "id": uid,
            "username": f"u{uid}",
            "email": f"u{uid}@test.com",
            "password": "pw",
        }
        resp = await do_request(session, "POST", base_url("/user"), payload)
        if resp.status != 200:
            raise RuntimeError(
                f"seed user failed: id={uid}, status={resp.status}, body={resp.body}"
            )

    async def seed_product(pid: int):
        payload = {
            "command": "create",
            "id": pid,
            "name": f"p{pid}",
            "description": "d",
            "price": 10.00,
            "quantity": 1000000,
        }
        resp = await do_request(session, "POST", base_url("/product"), payload)
        if resp.status != 200:
            raise RuntimeError(
                f"seed product failed: id={pid}, status={resp.status}, body={resp.body}"
            )

    batch = []
    for i in range(1, SEED_USERS + 1):
        batch.append(seed_user(100000 + i))
        if len(batch) >= 20:
            await asyncio.gather(*batch)
            batch.clear()
    if batch:
        await asyncio.gather(*batch)

    batch = []
    for i in range(1, SEED_PRODUCTS + 1):
        batch.append(seed_product(200000 + i))
        if len(batch) >= 20:
            await asyncio.gather(*batch)
            batch.clear()
    if batch:
        await asyncio.gather(*batch)

    print("Seeding done.")

async def verify_seed_sample(session: aiohttp.ClientSession):
    sample_users = [100001, 100100, 101000, 102000]
    sample_products = [200001, 200100, 200510, 201000]

    for uid in sample_users:
        resp = await do_request(session, "GET", base_url(f"/user/{uid}"))
        if resp.status != 200:
            raise RuntimeError(f"seed verification failed for user {uid}: {resp.status} {resp.body}")

    for pid in sample_products:
        resp = await do_request(session, "GET", base_url(f"/product/{pid}"))
        if resp.status != 200:
            raise RuntimeError(f"seed verification failed for product {pid}: {resp.status} {resp.body}")

    print("Seed verification passed.")

############################################################
# LOAD
############################################################

class Stats:
    def __init__(self):
        self.sent = 0
        self.ok = 0
        self.fail = 0
        self.lock = asyncio.Lock()

    async def add_sent(self):
        async with self.lock:
            self.sent += 1

    async def add_ok(self):
        async with self.lock:
            self.ok += 1

    async def add_fail(self):
        async with self.lock:
            self.fail += 1

    async def snapshot(self):
        async with self.lock:
            return self.sent, self.ok, self.fail

async def fail_and_report(
    reporter: FailureReporter,
    *,
    reason: str,
    method: str,
    path: str,
    payload,
    expected: str,
    resp: HttpResponse,
) -> bool:
    await reporter.report(
        reason=reason,
        api=path,
        method=method,
        payload=payload,
        expected=expected,
        actual_status=resp.status,
        actual_body=resp.body,
    )
    return False

async def perform_one(
    session: aiohttp.ClientSession,
    rng: random.Random,
    tracker: PurchaseTracker,
    dyn: DynamicStore,
    reporter: FailureReporter,
) -> bool:
    x = rng.randint(0, 99)
    user_id = 100000 + 1 + rng.randint(0, SEED_USERS - 1)
    product_id = 200000 + 1 + rng.randint(0, SEED_PRODUCTS - 1)

    t1 = MIX_USER_GET
    t2 = t1 + MIX_PRODUCT_GET
    t3 = t2 + MIX_ORDER_POST
    t4 = t3 + MIX_USER_PURCHASED
    t5 = t4 + MIX_USER_UPDATE
    t6 = t5 + MIX_PRODUCT_UPDATE
    t7 = t6 + MIX_USER_CREATE
    t8 = t7 + MIX_USER_DELETE
    t9 = t8 + MIX_PRODUCT_CREATE
    t10 = t9 + MIX_PRODUCT_DELETE

    if x < t1:
        path = f"/user/{user_id}"
        resp = await do_request(session, "GET", base_url(path))
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="user_get_status",
                method="GET",
                path=path,
                payload=None,
                expected=f"status=200 and body.id={user_id}",
                resp=resp,
            )
        u, ok = parse_user(resp.body)
        if not (ok and u.get("id") == user_id):
            return await fail_and_report(
                reporter,
                reason="user_get_body",
                method="GET",
                path=path,
                payload=None,
                expected=f"JSON user with id={user_id}",
                resp=resp,
            )
        return True

    if x < t2:
        path = f"/product/{product_id}"
        resp = await do_request(session, "GET", base_url(path))
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="product_get_status",
                method="GET",
                path=path,
                payload=None,
                expected=f"status=200 and body.id={product_id}",
                resp=resp,
            )
        p, ok = parse_product(resp.body)
        if not (ok and p.get("id") == product_id):
            return await fail_and_report(
                reporter,
                reason="product_get_body",
                method="GET",
                path=path,
                payload=None,
                expected=f"JSON product with id={product_id}",
                resp=resp,
            )
        return True

    if x < t3:
        qty = 1 + rng.randint(0, 2)
        path = "/order"
        payload = {
            "command": "place order",
            "user_id": user_id,
            "product_id": product_id,
            "quantity": qty,
        }
        resp = await do_request(session, "POST", base_url(path), payload)
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="order_post_status",
                method="POST",
                path=path,
                payload=payload,
                expected=f"status=200 and order status=Success for user_id={user_id}, product_id={product_id}, quantity={qty}",
                resp=resp,
            )
        o, ok = parse_order(resp.body)
        if not (ok and o.get("status") == "Success" and o.get("user_id") == user_id and o.get("product_id") == product_id and o.get("quantity") == qty):
            return await fail_and_report(
                reporter,
                reason="order_post_body",
                method="POST",
                path=path,
                payload=payload,
                expected=f"JSON order with status=Success, user_id={user_id}, product_id={product_id}, quantity={qty}",
                resp=resp,
            )
        await tracker.add(user_id, product_id, qty)
        return True

    if x < t4:
        path = f"/user/purchased/{user_id}"
        resp = await do_request(session, "GET", base_url(path))
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="user_purchased_status",
                method="GET",
                path=path,
                payload=None,
                expected="status=200 and correct purchased aggregate map",
                resp=resp,
            )
        got, ok = parse_purchased(resp.body)
        if not ok:
            return await fail_and_report(
                reporter,
                reason="user_purchased_parse",
                method="GET",
                path=path,
                payload=None,
                expected="valid JSON object mapping product_id -> quantity",
                resp=resp,
            )
        expect = await tracker.snapshot(user_id)
        for pid, qty in expect.items():
            if got.get(str(pid)) != qty:
                return await fail_and_report(
                    reporter,
                    reason="user_purchased_mismatch",
                    method="GET",
                    path=path,
                    payload=None,
                    expected=f"product {pid} should have quantity {qty}",
                    resp=resp,
                )
        return True

    if x < t5:
        path = "/user"
        payload = {
            "command": "update",
            "id": user_id,
            "email": f"u{user_id}+{rng.randint(0, 100000)}@test.com",
        }
        resp = await do_request(session, "POST", base_url(path), payload)
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="user_update_status",
                method="POST",
                path=path,
                payload=payload,
                expected=f"status=200 and JSON user with id={user_id}",
                resp=resp,
            )
        u, ok = parse_user(resp.body)
        if not (ok and u.get("id") == user_id):
            return await fail_and_report(
                reporter,
                reason="user_update_body",
                method="POST",
                path=path,
                payload=payload,
                expected=f"JSON user with id={user_id}",
                resp=resp,
            )
        return True

    if x < t6:
        path = "/product"
        payload = {
            "command": "update",
            "id": product_id,
            "quantity": 1000000 - rng.randint(0, 99),
        }
        resp = await do_request(session, "POST", base_url(path), payload)
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="product_update_status",
                method="POST",
                path=path,
                payload=payload,
                expected=f"status=200 and JSON product with id={product_id}",
                resp=resp,
            )
        p, ok = parse_product(resp.body)
        if not (ok and p.get("id") == product_id):
            return await fail_and_report(
                reporter,
                reason="product_update_body",
                method="POST",
                path=path,
                payload=payload,
                expected=f"JSON product with id={product_id}",
                resp=resp,
            )
        return True

    if x < t7:
        path = "/user"
        payload = await dyn.allocate_user_payload()
        resp = await do_request(session, "POST", base_url(path), payload)
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="user_create_status",
                method="POST",
                path=path,
                payload=payload,
                expected=f"status=200 and created user id={payload['id']}",
                resp=resp,
            )
        u, ok = parse_user(resp.body)
        if not (ok and u.get("id") == payload["id"]):
            return await fail_and_report(
                reporter,
                reason="user_create_body",
                method="POST",
                path=path,
                payload=payload,
                expected=f"JSON user with id={payload['id']}",
                resp=resp,
            )
        await dyn.mark_user_created(payload)
        return True

    if x < t8:
        path = "/user"
        payload = await dyn.pop_user_delete_payload()
        if payload is None:
            return True
        resp = await do_request(session, "POST", base_url(path), payload)
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="user_delete_status",
                method="POST",
                path=path,
                payload=payload,
                expected=f"status=200 for delete user id={payload['id']}",
                resp=resp,
            )
        return True

    if x < t9:
        path = "/product"
        payload = await dyn.allocate_product_payload()
        resp = await do_request(session, "POST", base_url(path), payload)
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="product_create_status",
                method="POST",
                path=path,
                payload=payload,
                expected=f"status=200 and created product id={payload['id']}",
                resp=resp,
            )
        p, ok = parse_product(resp.body)
        if not (ok and p.get("id") == payload["id"]):
            return await fail_and_report(
                reporter,
                reason="product_create_body",
                method="POST",
                path=path,
                payload=payload,
                expected=f"JSON product with id={payload['id']}",
                resp=resp,
            )
        await dyn.mark_product_created(payload)
        return True

    if x < t10:
        path = "/product"
        payload = await dyn.pop_product_delete_payload()
        if payload is None:
            return True
        resp = await do_request(session, "POST", base_url(path), payload)
        if resp.status != 200:
            return await fail_and_report(
                reporter,
                reason="product_delete_status",
                method="POST",
                path=path,
                payload=payload,
                expected=f"status=200 for delete product id={payload['id']}",
                resp=resp,
            )
        return True

    return False

def grade_for_ok_tps(ok_tps: float):
    passed = []
    total_marks = 0.0
    for threshold in sorted(GRADE_THRESHOLDS):
        if ok_tps >= threshold:
            passed.append(threshold)
            total_marks += GRADE_THRESHOLDS[threshold]
    return passed, total_marks

async def live_tps_monitor(stats: Stats, duration_seconds: int):
    prev_sent = 0
    prev_ok = 0
    prev_fail = 0

    print("\n================ LIVE TPS PER SECOND ================\n")

    for sec in range(1, duration_seconds + 1):
        await asyncio.sleep(1)
        sent, ok, fail = await stats.snapshot()

        sent_delta = sent - prev_sent
        ok_delta = ok - prev_ok
        fail_delta = fail - prev_fail

        fail_rate = (fail_delta / sent_delta * 100) if sent_delta else 0.0

        print(
            f"second={sec:>2} | "
            f"sent_tps={sent_delta:>6} | "
            f"ok_tps={ok_delta:>6} | "
            f"fail_tps={fail_delta:>6} | "
            f"fail_rate={fail_rate:>6.2f}%"
        )

        prev_sent = sent
        prev_ok = ok
        prev_fail = fail

class PerSecondStats:
    def __init__(self):
        self.ok_per_second = []
        self.fail_per_second = []
        self.sent_per_second = []

async def worker(
    session: aiohttp.ClientSession,
    wid: int,
    queue: asyncio.Queue,
    stats: Stats,
    tracker: PurchaseTracker,
    dyn: DynamicStore,
    reporter: FailureReporter,
):
    rng = random.Random(time.time_ns() + wid * 7919)
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        ok = await perform_one(session, rng, tracker, dyn, reporter)
        if ok:
            await stats.add_ok()
        else:
            await stats.add_fail()
        queue.task_done()

async def run_load(
    session: aiohttp.ClientSession,
    tracker: PurchaseTracker,
    dyn: DynamicStore,
    reporter: FailureReporter,
    target_tps: int
):
    print(f"Running load phase: target={target_tps} TPS, workers={WORKERS}, duration={DURATION_SECONDS}s")

    stats = Stats()
    queue = asyncio.Queue(maxsize=max(target_tps * 2, 1000))

    worker_tasks = [
        asyncio.create_task(worker(session, i, queue, stats, tracker, dyn, reporter))
        for i in range(WORKERS)
    ]

    monitor_task = asyncio.create_task(live_tps_monitor(stats, DURATION_SECONDS))

    start = time.perf_counter()
    sent = 0

    while True:
        elapsed = time.perf_counter() - start
        if elapsed >= DURATION_SECONDS:
            break

        expected_sent = int(elapsed * target_tps)
        while sent < expected_sent:
            await queue.put(1)
            sent += 1
            await stats.add_sent()

        await asyncio.sleep(0.001)

    await queue.join()
    await monitor_task

    end = time.perf_counter()
    actual_elapsed = end - start

    for _ in worker_tasks:
        await queue.put(None)
    await asyncio.gather(*worker_tasks)

    sent_tps = stats.sent / actual_elapsed
    ok_tps = stats.ok / actual_elapsed
    fail_tps = stats.fail / actual_elapsed
    fail_rate = (stats.fail / stats.sent * 100) if stats.sent else 0.0

    print(
        f"Load done. sent={stats.sent} ok={stats.ok} fail={stats.fail} "
        f"sent_tps={sent_tps:.2f} ok_tps={ok_tps:.2f} fail_tps={fail_tps:.2f} "
        f"fail_rate={fail_rate:.2f}%"
    )

    return {
        "target_tps": target_tps,
        "elapsed": actual_elapsed,
        "sent": stats.sent,
        "ok": stats.ok,
        "fail": stats.fail,
        "sent_tps": sent_tps,
        "ok_tps": ok_tps,
        "fail_tps": fail_tps,
        "fail_rate": fail_rate,
    }

async def auto_test_tps(session: aiohttp.ClientSession):
    print("\n================ AUTO TPS TEST ================\n")

    results = []
    best_ok_tps = 0.0
    best_target = None
    reporter = FailureReporter()

    tracker = PurchaseTracker()

    for tps in TPS_LEVELS:
        print(f"\n--- Testing target TPS = {tps} ---")

        dyn = DynamicStore()
        result = await run_load(session, tracker, dyn, reporter, tps)
        results.append(result)

        if result["ok_tps"] > best_ok_tps:
            best_ok_tps = result["ok_tps"]
            best_target = tps

        if result["fail_rate"] > 50:
            print("\nStopping test early because fail rate exceeded 50%\n")
            break

    print("\n================ TPS SUMMARY ================\n")
    for r in results:
        passed, marks = grade_for_ok_tps(r["ok_tps"])
        print(
            f"target={r['target_tps']:>5} | "
            f"ok_tps={r['ok_tps']:>8.2f} | "
            f"fail_rate={r['fail_rate']:>6.2f}% | "
            f"passed={passed} | marks={marks:.1f}"
        )

    passed, marks = grade_for_ok_tps(best_ok_tps)

    print("\n================ FINAL RESULT ================\n")
    print(f"Best target tested     : {best_target}")
    print(f"Best successful TPS    : {best_ok_tps:.2f}")
    print(f"Thresholds passed      : {passed}")
    print(f"Marks from thresholds  : {marks:.1f}")
    print("Persistence markers still expected in DB:")
    print(f"  user_id={PERSIST_USER_ID}")
    print(f"  product_id={PERSIST_PRODUCT_ID}")

    reporter.print_summary()

############################################################
# MAIN
############################################################

async def main():
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=100000, limit_per_host=100000, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        if RUN_CORRECTNESS_SUITE:
            try:
                await correctness_suite(session)
            except Exception as e:
                print("\n================ CORRECTNESS SUITE ERROR ================\n")
                print(type(e).__name__ + ":", str(e))
                print()
                raise

        await seed_users_and_products(session)

        print(f"Waiting {WAIT_AFTER_SEED_SECONDS}s for periodic DB flush...")
        await asyncio.sleep(WAIT_AFTER_SEED_SECONDS)

        if VERIFY_SEED_SAMPLE:
            await verify_seed_sample(session)

        if AUTO_TEST_TPS:
            await auto_test_tps(session)
        elif RUN_LOAD_PHASE:
            tracker = PurchaseTracker()
            dyn = DynamicStore()
            reporter = FailureReporter()
            await run_load(session, tracker, dyn, reporter, TARGET_TPS)

if __name__ == "__main__":
    asyncio.run(main())