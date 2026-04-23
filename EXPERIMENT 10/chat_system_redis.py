"""
Real-time chat system design demo using Redis + python-socketio.

"""

from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List

import redis
import socketio


class TerminalSocketHub:


    def __init__(self) -> None:
        self.sio = socketio.Server(async_mode="threading", logger=False, engineio_logger=False)
        self.room_members: Dict[str, set[str]] = {}
        self.handlers: Dict[str, callable] = {}

    def register_user(self, username: str, handler) -> None:
        self.handlers[username] = handler

    def join_room(self, username: str, room: str) -> None:
        self.room_members.setdefault(room, set()).add(username)

    def leave_room(self, username: str, room: str) -> None:
        if room in self.room_members:
            self.room_members[room].discard(username)

    def emit_to_room(self, room: str, event: str, payload: dict) -> None:
        # This emit keeps the code aligned with Socket.IO message flow concepts.
        self.sio.emit(event, payload, room=room)
        for username in sorted(self.room_members.get(room, set())):
            handler = self.handlers.get(username)
            if handler:
                handler(event, payload)


class ChatSystemRedisDemo:
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, db: int = 0) -> None:
        self.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.socket_hub = TerminalSocketHub()
        self.user_pubsubs: Dict[str, redis.client.PubSub] = {}
        self.pubsub_threads: List[threading.Thread] = []
        self.stop_event = threading.Event()
        self.message_ids: List[str] = []

    def connect(self) -> None:
        self.redis.ping()
        print("Redis connected")

    def reset_demo_keys(self) -> None:
        keys = self.redis.keys("chat:*")
        if keys:
            self.redis.delete(*keys)

    def create_user(self, username: str) -> None:
        # STRING with TTL is ideal for quick online/offline presence checks.
        self.redis.set(f"chat:user:{username}:status", "online", ex=30)
        self.socket_hub.register_user(username, self._terminal_socket_handler(username))

    def _terminal_socket_handler(self, username: str):
        def handler(event: str, payload: dict) -> None:
            print(
                f"[socket.io -> {username}] event={event} room={payload['room']} "
                f"from={payload['sender']} content={payload['content']}"
            )

        return handler

    def heartbeat(self, username: str) -> None:
        self.redis.set(f"chat:user:{username}:status", "online", ex=30)

    def join_room(self, username: str, room: str) -> None:
        room_users_key = f"chat:room:{room}:users"
        channel_name = f"chat:{room}"

        # SET gives O(1)-style membership operations and avoids duplicates.
        self.redis.sadd(room_users_key, username)
        self.socket_hub.join_room(username, room)
        self.heartbeat(username)

        # Each user has a dedicated Pub/Sub listener to show real-time delivery.
        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(channel_name)
        self.user_pubsubs[username] = pubsub

        listener_thread = threading.Thread(
            target=self._listen_pubsub,
            args=(username, pubsub),
            daemon=True,
            name=f"pubsub-listener-{username}",
        )
        listener_thread.start()
        self.pubsub_threads.append(listener_thread)

        print(f"{username} joined room={room} (channel={channel_name})")

    def leave_room(self, username: str, room: str) -> None:
        room_users_key = f"chat:room:{room}:users"
        self.redis.srem(room_users_key, username)
        self.socket_hub.leave_room(username, room)

    def _listen_pubsub(self, username: str, pubsub: redis.client.PubSub) -> None:
        while not self.stop_event.is_set():
            try:
                message = pubsub.get_message(timeout=1.0)
            except Exception:
                # On shutdown, pubsub sockets can close while threads are polling.
                if self.stop_event.is_set():
                    break
                raise
            if message and message.get("type") == "message":
                channel = message["channel"]
                raw = message["data"]
                print(f"[pubsub -> {username}] channel={channel} message={raw}")
            time.sleep(0.05)

    def enqueue_message(self, sender: str, room: str, content: str) -> None:
        self.heartbeat(sender)
        message_id = str(uuid.uuid4())
        payload = {
            "message_id": message_id,
            "sender": sender,
            "room": room,
            "content": content,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.message_ids.append(message_id)

        # LIST works well as a durable FIFO queue for worker-based processing.
        self.redis.rpush("chat:queue", json.dumps(payload))
        print(f"[queue] enqueued message_id={message_id} room={room} sender={sender}")

        # HASH stores structured per-message delivery metadata in one key.
        self.redis.hset(
            f"chat:log:{message_id}",
            mapping={
                "sender": sender,
                "room": room,
                "content": content,
                "timestamp": payload["timestamp"],
                "status": "queued",
            },
        )

    def process_queue(self) -> None:
        while True:
            depth_before = self.redis.llen("chat:queue")
            if depth_before == 0:
                break

            print(f"[queue] depth before={depth_before}")
            packed = self.redis.lpop("chat:queue")
            if not packed:
                break
            payload = json.loads(packed)
            room = payload["room"]
            sender = payload["sender"]
            content = payload["content"]
            message_id = payload["message_id"]
            timestamp = payload["timestamp"]

            history_key = f"chat:messages:{room}"
            history_item = json.dumps(payload)

            # LIST keeps ordered chat history and supports trimming to recent N.
            self.redis.lpush(history_key, history_item)
            self.redis.ltrim(history_key, 0, 19)

            room_users_key = f"chat:room:{room}:users"
            room_users = self.redis.smembers(room_users_key)

            for user in room_users:
                if user != sender:
                    unread_key = f"chat:unread:{user}"
                    # Integer counter tracks unread notifications efficiently.
                    self.redis.incr(unread_key)

            self.redis.publish(f"chat:{room}", history_item)

            socket_payload = {
                "message_id": message_id,
                "sender": sender,
                "room": room,
                "content": content,
                "timestamp": timestamp,
            }
            self.socket_hub.emit_to_room(room, "chat_message", socket_payload)

            self.redis.hset(f"chat:log:{message_id}", mapping={"status": "delivered"})
            depth_after = self.redis.llen("chat:queue")
            print(f"[queue] depth after={depth_after}")

            time.sleep(0.2)

    def mark_read(self, username: str) -> None:
        self.redis.set(f"chat:unread:{username}", 0)
        print(f"[unread] reset for {username}")

    def print_history(self, room: str) -> None:
        rows = self.redis.lrange(f"chat:messages:{room}", 0, 19)
        print(f"\nHistory for room={room} (newest first):")
        for row in rows:
            item = json.loads(row)
            print(f" - {item['timestamp']} {item['sender']}: {item['content']}")

    def print_all_keys(self) -> None:
        print("\nAll Redis keys created:")
        for key in sorted(self.redis.keys("chat:*")):
            print(f" - {key}")

    def print_delivery_logs(self) -> None:
        print("\nDelivery logs:")
        for message_id in self.message_ids:
            log_key = f"chat:log:{message_id}"
            fields = self.redis.hgetall(log_key)
            print(f" - {log_key}: {fields}")

    def cleanup(self) -> None:
        self.stop_event.set()
        for pubsub in self.user_pubsubs.values():
            try:
                pubsub.close()
            except Exception:
                pass
        for thread in self.pubsub_threads:
            thread.join(timeout=1.0)


def run_demo() -> None:
    chat = ChatSystemRedisDemo()
    try:
        chat.connect()
        chat.reset_demo_keys()

        users = ["alice", "bob", "carol"]
        rooms = ["general", "tech"]

        print(f"Users: {users}")
        print(f"Rooms: {rooms}")

        for user in users:
            chat.create_user(user)

        chat.join_room("alice", "general")
        chat.join_room("bob", "general")
        chat.join_room("carol", "tech")

        messages = [
            ("alice", "general", "Hi Bob, welcome to general."),
            ("bob", "general", "Hey Alice! Good to be here."),
            ("carol", "tech", "Anyone using Redis streams?"),
            ("alice", "general", "Queue worker is processing now."),
            ("carol", "tech", "Socket delivery looks real-time."),
        ]

        print("\nEnqueuing 5 messages...")
        for sender, room, content in messages:
            chat.enqueue_message(sender, room, content)

        print("\nProcessing queue...")
        chat.process_queue()

        # Demonstrate unread reset behavior.
        chat.mark_read("bob")
        chat.mark_read("carol")

        chat.print_history("general")
        chat.print_history("tech")
        chat.print_all_keys()
        chat.print_delivery_logs()
    finally:
        chat.cleanup()


if __name__ == "__main__":
    run_demo()
