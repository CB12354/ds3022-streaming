"""Bluesky -> Kafka topic "bsky_posts".

  python bsky_producer.py live        # PUSH: new and updated events from the EMSC WebSocket

Adapted from Professor Magee's stream-seismic.py (learn-kafka).
"""
import asyncio
import json
import logging
import sys
import time

import requests
from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bsky_posts"
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


def send(producer, message):
    """One Kafka record per message. Key = the event id, so every
    version of the same post goes to the same partition, in order."""
    commit = message.get('commit', {})
    action = commit.get('operation', message.get('kind', 'unknown'))
    did, rkey = message.get('did'), commit.get('rkey')
    key = f'{did}/{rkey}' if did and rkey else did
    envelope = {'action' : action, 'data' : message}
    producer.produce(topic=TOPIC, key=key, value=json.dumps(envelope))
    return key

async def live(producer):
    """PUSH: keep one connection open; EMSC sends each new or updated event."""
    cursor = None     # time_us of the last message produced, to resume from after a drop
    gap_start = None  # wall-clock time of the last drop; None while connected
    skipped = 0
    sent = 0
    while True:
        url = WSS_URL + (f'&cursor={cursor}' if cursor else "")
        try:
            ws = await websocket_connect(url, ping_interval=15)
        except Exception as e:
            logging.warning("Connection failed (%r), retrying after 5s timeout", e)
            await asyncio.sleep(5)
            continue
        if gap_start is None or cursor is None:
            logging.info("Connected. Post firehose incoming...")
        else:
            outage = time.time() - gap_start
            behind = time.time() - cursor / 1e6
            logging.warning("Reconnected after %.0f s offline; replaying %.0f s of events from cursor %s",
                            outage, behind, cursor)
            gap_start = None
        while True:
            text = await ws.read_message()
            if text is None:
                logging.warning("The server closed the connection")
                break
            try:
                message = json.loads(text)
                send(producer, message)
                cursor = message.get("time_us", cursor)
                sent += 1
                if sent % 1000 == 0:
                    logging.info("Produced %d messages (%d skipped), cursor %s", sent, skipped, cursor)
            except (ValueError, KeyError, TypeError) as e:
                skipped += 1
                logging.warning("Skipped unreadable message No. %d: %r", skipped, e)
        gap_start = time.time()
        logging.warning("Connection lost at cursor %s", cursor)
        await asyncio.sleep(5)


def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
