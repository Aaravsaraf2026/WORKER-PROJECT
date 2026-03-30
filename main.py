"""
Example FastAPI app demonstrating the Redis Queue system.
Fits your existing project structure — drop this in as main.py.

Run:
    uvicorn main:app --reload

Test endpoints in order:
    1. GET  /                        → health check
    2. POST /seed                    → seed products to DB
    3. GET  /products                → list products
    4. POST /orders                  → push an order to the queue
    5. GET  /orders/stats            → queue depth + DLQ info
    6. GET  /healthz                 → redis + service health
    7. POST /orders/replay-dlq       → replay any failed orders
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

# ── Your existing imports (unchanged) ────────────────────────────────────────
from database.db.core import get_db, init_db
from database.models import Products, User

# ── Queue imports ─────────────────────────────────────────────────────────────
from repo.queue.worker import (
    MessagePriority,
    cleanup_thread,
    connection_manager,
    health_checker,
    message_reclaimer,
    shutdown_manager,
)
from repo.queue.async_worker import (
    async_connection_manager,
    async_get_queue_stats,
    async_health_check,
    async_push_work,
    async_replay_dlq,
    run_async_worker,
)

# ── Logging (your existing setup) ─────────────────────────────────────────────
from core.logging import setup_logging

setup_logging()
logger = logging.getLogger("app")


# ══════════════════════════════════════════════════════════════════════════════
# QUEUE HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

async def handle_order(msg: dict):
    """
    Processes a single order message from the queue.
    run_async_worker calls this for every message — ack/delete are automatic.
    Raise any exception to trigger a retry (up to max_retries).
    """
    logger.info(
        f"Processing order | product_id={msg.get('product_id')} "
        f"| qty={msg.get('quantity')} | trace={msg.get('_trace_id')}"
    )

    # Simulate processing — replace with your real logic:
    #   await charge_payment(msg)
    #   await update_inventory(msg)
    #   await send_confirmation_email(msg)
    await asyncio.sleep(0.1)

    logger.info(f"Order complete | id={msg['_id']}")


async def handle_order_failed(exc: Exception, msg: dict):
    """Called after all retries are exhausted. Message goes to DLQ automatically."""
    logger.error(
        f"Order permanently failed | id={msg['_id']} "
        f"| retry_count={msg.get('_retry_count')} | error={exc}"
    )
    # Hook for alerting: Sentry, PagerDuty, Slack, etc.


# ══════════════════════════════════════════════════════════════════════════════
# LIFESPAN
# ══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────────────
    shutdown_manager.embedded = True   # prevents double signal registration

    await init_db()

    # Sync Redis client — used by background threads (reclaimer, health checker)
    try:
        app.state.redis = connection_manager.get_client()
    except RuntimeError as e:
        logger.error(str(e))
        os._exit(1)

    # Async Redis client — used by route handlers
    await async_connection_manager.initialize()

    # Background services
    health_checker.start()      # pings Redis every 60s
    message_reclaimer.start()   # reclaims timed-out messages, routes to DLQ
    cleanup_thread.start()      # prunes inactive queue metadata

    # Async worker task — consumes from "orders" queue
    order_worker = asyncio.create_task(
        run_async_worker(
            queue_name="orders",
            handler=handle_order,
            poll_interval_ms=1000,
            max_retries=3,
            retry_delay=1.0,
            on_error=handle_order_failed,
        )
    )

    logger.info("App started")
    yield  # ← app is live

    # ── Shutdown ──────────────────────────────────────────────────────────────
    order_worker.cancel()
    await asyncio.gather(order_worker, return_exceptions=True)

    shutdown_manager.shutdown()
    await async_connection_manager.close()
    logger.info("App shut down cleanly")


app = FastAPI(title="Queue Example App", lifespan=lifespan)


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMAS
# ══════════════════════════════════════════════════════════════════════════════

class OrderRequest(BaseModel):
    product_id: int
    quantity: int = 1
    customer_email: str
    priority: str = "normal"   # critical | high | normal | low


class OrderResponse(BaseModel):
    message_id: str
    product_id: int
    queue: str = "orders"
    status: str = "queued"


# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/")
def root():
    logger.info("Root endpoint hit")
    return {"message": "Hello", "docs": "/docs"}


# ── Seed (your existing endpoint, unchanged) ──────────────────────────────────
@app.post("/seed")
async def seed_products(db: AsyncSession = Depends(get_db)):
    products = [
        Products(Product_name="iPhone",     product_id=104, price=80000),
        Products(Product_name="Laptop",     product_id=105, price=60000),
        Products(Product_name="Headphones", product_id=106, price=2000),
    ]
    db.add_all(products)
    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(400, "Duplicate product_id")
    return {"seeded": len(products)}


# ── Products (your existing endpoint, renamed for clarity) ────────────────────
@app.get("/products")
async def get_products(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Products))
    return result.scalars().all()


# ── Push an order to the queue ────────────────────────────────────────────────
@app.post("/orders", response_model=OrderResponse)
async def create_order(
    order: OrderRequest,
    db: AsyncSession = Depends(get_db),
):
    # Validate product exists in DB before queuing
    result = await db.execute(
        select(Products).where(Products.product_id == order.product_id)
    )
    product = result.scalar_one_or_none()
    if not product:
        raise HTTPException(404, f"Product {order.product_id} not found")

    # Map string priority to enum
    priority_map = {
        "critical": MessagePriority.CRITICAL,
        "high":     MessagePriority.HIGH,
        "normal":   MessagePriority.NORMAL,
        "low":      MessagePriority.LOW,
    }
    priority = priority_map.get(order.priority.lower(), MessagePriority.NORMAL)

    msg_id = await async_push_work(
        queue_name="orders",
        data={
            "product_id":     order.product_id,
            "product_name":   product.Product_name,
            "price":          product.price,
            "quantity":       order.quantity,
            "customer_email": order.customer_email,
            "total":          product.price * order.quantity,
        },
        priority=priority,
    )

    if msg_id is None:
        # push_work returns None when rate-limited or backpressure is active
        raise HTTPException(429, "Queue is busy — try again in a moment")

    logger.info(f"Order queued | msg_id={msg_id} | product={order.product_id}")
    return OrderResponse(message_id=msg_id, product_id=order.product_id)


# ── Queue stats ───────────────────────────────────────────────────────────────
@app.get("/orders/stats")
async def order_queue_stats():
    stats = await async_get_queue_stats("orders")
    return {
        "queue":         stats["queue_name"],
        "pending":       stats["length"],
        "dlq":           stats["dlq_length"],
        "not_acked_yet": stats["pending_count"],
        "consumers":     stats["consumer_count"],
    }


# ── Replay DLQ ────────────────────────────────────────────────────────────────
@app.post("/orders/replay-dlq")
async def replay_order_dlq():
    replayed = await async_replay_dlq("orders", max_messages=100)
    return {"replayed": replayed, "queue": "orders"}


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/healthz")
async def healthz():
    return await async_health_check()