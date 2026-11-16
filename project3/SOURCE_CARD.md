# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name: Cameron Berryman**
**Date: 16 Nov 2026**

## 1. The source
- Source and URL: Bluesky (Secure Websocket, wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post)
- Method (WebSocket push, REST polling or REST history pull): WS Push
  - FILTER BY commit.collection == "app.bsky.feed.post" for accepted items. Keep an eye out for post deletions.
- Account, key or User-Agent needed: Not applicable, no account needed and access is free
- Limits I know about (rate limits, paging, reconnects): No rate limits, limited access out of courtesy. Reconnection targets an older message using a cursor (`?cursor=`). The maximum page size is 1 day for the firehose, so there is a 1-day replay window.

## 2. One message

```json
{
    "action": "create",
    "data": {
        "did": "did:plc:sjknt7vstcnafe4wvhezb7rq",
        "time_us": 1794762538654802,
        "cursor": 27217902773,
        "kind": "commit",
        "commit": {
            "rev": "3n2eivszouz2o",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3n2eivs56ik2h",
            "record": {
                "$type": "app.bsky.feed.post",
                "createdAt": "2026-11-15T17:08:57.532Z",
                "langs": [
                    "en"
                ],
                "reply": {
                    "parent": {
                        "cid": "bafyreielz62aphpmzdtwq2elmgo3tglurzh3bemijxtjfuo6de6223xnpm",
                        "uri": "at://did:plc:hervmc2h2fuuae4hlzjkwexc/app.bsky.feed.post/3n2eibyw4jk24"
                    },
                    "root": {
                        "cid": "bafyreigz7rrnqlokdk2rw5ayvtiy5uiifr3brk4tsfdp5qfn7uhriovjoa",
                        "uri": "at://did:plc:sjknt7vstcnafe4wvhezb7rq/app.bsky.feed.post/3n2eeeileyc2w"
                    }
                },
                "text": "Kindness. Pass it on 💜"
            },
            "cid": "bafyreifttqblym3ifg3fu7oyt4uynot4dbwncvmb7ys7wsh36hsj2tk4y4"
        }
    }
}
```

## 3. Volume maths
- Measured rate (from the probe): 64.64 per second
- Live capture needed for 50,000: 773.5 seconds
- History / backfill available? How many days? *Limited, available for 1 day* 
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): Easily doable within an evening, capture time for 50k posts is very short and maintenance on Bluesky is conducted sporadically but tends to be short.

## 4. Topic design
- Topic name: bsky_posts (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: Key is did combined with commit.rkey so it's by post instead of by user
  - (commit.operation tells what happens, being create or delete)
- Why this key (ordering, spread across partitions): 
  - Message volume is extremely high so spreading should be about even for each. Any commit-type option (which contains all posts) has an rkey, so if commit is missing then it implies the action is not a post and should be ignored. 
  - Some users will post way more than others (bot accounts, corporate accounts, etc.) so splits by individual posts would be more evenly split than by account

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander)
  - Same partition and offset
- What makes two records **the same thing** (the same trade, post, aircraft position)?
  - Same did, rkey, operation and time_us. CID can't be used because that's missing from deletions. 
- If the same thing can arrive in several versions, which one do I keep?

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: *How long in characters, grouped by language, are Bluesky posts on average? What does the distribution look like?*
  - Not every post has a language, but most do.

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
