import os
import json
import uuid
import socket
import threading
from datetime import datetime
from pathlib import Path
from functools import wraps
from typing import Any, Dict, List, Tuple

from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# -----------------------------
# Config
# -----------------------------
API_KEY = os.getenv("API_KEY", "CHANGE_ME")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", socket.gethostname())
PORT = int(os.getenv("PORT", "5000"))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# You can override the DB file path if needed:
#   set DB_PATH=C:\path\to\db.json
DB_PATH = Path(os.getenv("DB_PATH", str(DATA_DIR / "db.json")))

_lock = threading.Lock()


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


# -----------------------------
# Auth (X-API-Key)
# -----------------------------
def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        provided = request.headers.get("X-API-Key", "")
        if not provided or provided != API_KEY:
            return jsonify({"error": "Unauthorized", "message": "Missing or invalid API key"}), 401
        return f(*args, **kwargs)
    return decorated


# -----------------------------
# DB helpers (db.json format)
# db.json structure (your file):
# {
#   "products":[{"id":"p1","name":"Notebook","price":12.5,"createdAt":"..."}],
#   "orders":[{"id":"o1","customer":"Fatima","items":[{"productId":"p1","qty":2}],"status":"NEW","createdAt":"..."}]
# }
# -----------------------------
def ensure_db_exists():
    if not DB_PATH.exists():
        DB_PATH.write_text(json.dumps({"products": [], "orders": []}, indent=2), encoding="utf-8")
        return
    content = DB_PATH.read_text(encoding="utf-8").strip()
    if content == "":
        DB_PATH.write_text(json.dumps({"products": [], "orders": []}, indent=2), encoding="utf-8")


def load_db() -> Dict[str, Any]:
    ensure_db_exists()
    data = json.loads(DB_PATH.read_text(encoding="utf-8"))
    if "products" not in data or not isinstance(data["products"], list):
        data["products"] = []
    if "orders" not in data or not isinstance(data["orders"], list):
        data["orders"] = []
    return data


def save_db(data: Dict[str, Any]) -> None:
    DB_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def index_by_id(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for it in items:
        if isinstance(it, dict) and "id" in it:
            out[str(it["id"])] = it
    return out


def get_body() -> Dict[str, Any]:
    b = request.get_json(silent=True)
    return b if isinstance(b, dict) else {}


def bad_request(msg: str):
    return jsonify({"error": "Bad Request", "message": msg}), 400


def not_found(msg: str):
    return jsonify({"error": "Not Found", "message": msg}), 404


# -----------------------------
# Unprotected health
# -----------------------------
@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "time": now_iso(),
        "instance_name": INSTANCE_NAME,
        "hostname": socket.gethostname(),
        "db_path": str(DB_PATH)
    }), 200


# =========================================================
# PRODUCTS
# =========================================================
@app.get("/products")
@require_api_key
def list_products():
    data = load_db()
    products = data["products"]
    return jsonify(products), 200


@app.post("/products")
@require_api_key
def create_product():
    body = get_body()
    name = body.get("name")
    price = body.get("price")

    if not name:
        return bad_request("Field 'name' is required")
    if price is None:
        return bad_request("Field 'price' is required")

    try:
        price_val = float(price)
    except Exception:
        return bad_request("Field 'price' must be a number")

    with _lock:
        data = load_db()
        products = data["products"]
        p_index = index_by_id(products)

        # Allow custom id, else generate p-<uuid>
        pid = str(body.get("id") or f"p-{uuid.uuid4().hex[:8]}")
        if pid in p_index:
            return bad_request("Product id already exists")

        product = {
            "id": pid,
            "name": str(name),
            "price": price_val,
            "createdAt": now_iso()
        }
        products.append(product)
        save_db(data)

    return jsonify(product), 201


@app.get("/products/<pid>")
@require_api_key
def get_product(pid: str):
    data = load_db()
    p_index = index_by_id(data["products"])
    if pid not in p_index:
        return not_found("product not found")
    return jsonify(p_index[pid]), 200


@app.put("/products/<pid>")
@require_api_key
def update_product(pid: str):
    body = get_body()

    with _lock:
        data = load_db()
        products = data["products"]
        p_index = index_by_id(products)

        if pid not in p_index:
            return not_found("product not found")

        # Update fields if provided
        if "name" in body:
            p_index[pid]["name"] = str(body["name"])
        if "price" in body:
            try:
                p_index[pid]["price"] = float(body["price"])
            except Exception:
                return bad_request("Field 'price' must be a number")

        # Write back by rebuilding list in same order
        for i, p in enumerate(products):
            if str(p.get("id")) == pid:
                products[i] = p_index[pid]
                break

        save_db(data)
        return jsonify(p_index[pid]), 200


@app.delete("/products/<pid>")
@require_api_key
def delete_product(pid: str):
    with _lock:
        data = load_db()
        products = data["products"]
        orders = data["orders"]

        before = len(products)
        products[:] = [p for p in products if str(p.get("id")) != pid]
        if len(products) == before:
            return not_found("product not found")

        # Optional: remove product from any orders (or keep as history; choose one)
        # Here: remove any order items referencing deleted product
        for o in orders:
            if isinstance(o, dict) and isinstance(o.get("items"), list):
                o["items"] = [it for it in o["items"] if str(it.get("productId")) != pid]

        save_db(data)

    return jsonify({"deleted": pid}), 200


# =========================================================
# ORDERS
# =========================================================
@app.get("/orders")
@require_api_key
def list_orders():
    data = load_db()
    return jsonify(data["orders"]), 200


@app.post("/orders")
@require_api_key
def create_order():
    body = get_body()
    customer = body.get("customer")
    items = body.get("items")
    status = body.get("status", "NEW")

    if not customer:
        return bad_request("Field 'customer' is required")
    if not isinstance(items, list) or len(items) == 0:
        return bad_request("Field 'items' must be a non-empty list")

    with _lock:
        data = load_db()
        products_index = index_by_id(data["products"])
        orders = data["orders"]
        o_index = index_by_id(orders)

        # Validate items: each needs productId and qty
        normalized_items = []
        for it in items:
            if not isinstance(it, dict):
                return bad_request("Each item must be an object with productId and qty")
            product_id = str(it.get("productId", "")).strip()
            qty = it.get("qty")
            if not product_id:
                return bad_request("Each item must include productId")
            if product_id not in products_index:
                return bad_request(f"productId does not exist: {product_id}")
            try:
                qty_val = int(qty)
                if qty_val <= 0:
                    return bad_request("qty must be a positive integer")
            except Exception:
                return bad_request("qty must be an integer")

            normalized_items.append({"productId": product_id, "qty": qty_val})

        oid = str(body.get("id") or f"o-{uuid.uuid4().hex[:8]}")
        if oid in o_index:
            return bad_request("Order id already exists")

        order = {
            "id": oid,
            "customer": str(customer),
            "items": normalized_items,
            "status": str(status),
            "createdAt": now_iso()
        }
        orders.append(order)
        save_db(data)

    return jsonify(order), 201


@app.get("/orders/<oid>")
@require_api_key
def get_order(oid: str):
    data = load_db()
    o_index = index_by_id(data["orders"])
    if oid not in o_index:
        return not_found("order not found")
    return jsonify(o_index[oid]), 200


@app.put("/orders/<oid>")
@require_api_key
def update_order(oid: str):
    body = get_body()

    with _lock:
        data = load_db()
        orders = data["orders"]
        o_index = index_by_id(orders)

        if oid not in o_index:
            return not_found("order not found")

        # Allow updating status/customer (and optionally items)
        if "status" in body:
            o_index[oid]["status"] = str(body["status"])
        if "customer" in body:
            o_index[oid]["customer"] = str(body["customer"])

        if "items" in body:
            if not isinstance(body["items"], list) or len(body["items"]) == 0:
                return bad_request("items must be a non-empty list")

            products_index = index_by_id(data["products"])
            new_items = []
            for it in body["items"]:
                if not isinstance(it, dict):
                    return bad_request("Each item must be an object with productId and qty")
                product_id = str(it.get("productId", "")).strip()
                qty = it.get("qty")
                if not product_id:
                    return bad_request("Each item must include productId")
                if product_id not in products_index:
                    return bad_request(f"productId does not exist: {product_id}")
                try:
                    qty_val = int(qty)
                    if qty_val <= 0:
                        return bad_request("qty must be a positive integer")
                except Exception:
                    return bad_request("qty must be an integer")
                new_items.append({"productId": product_id, "qty": qty_val})

            o_index[oid]["items"] = new_items

        # Write back into list
        for i, o in enumerate(orders):
            if str(o.get("id")) == oid:
                orders[i] = o_index[oid]
                break

        save_db(data)
        return jsonify(o_index[oid]), 200


@app.delete("/orders/<oid>")
@require_api_key
def delete_order(oid: str):
    with _lock:
        data = load_db()
        orders = data["orders"]
        before = len(orders)
        orders[:] = [o for o in orders if str(o.get("id")) != oid]
        if len(orders) == before:
            return not_found("order not found")
        save_db(data)

    return jsonify({"deleted": oid}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
