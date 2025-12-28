## General
- Time problems
    - Local clock drift
    - use a monotonic clock for timeouts / intervals
    - Exchanges sometimes emit non-monotonic timestamps; do not assume exchange time always increases
- Subscription quotas and connection policies are easy to violate accidentally
    - per connection max channels / symbols / messages policies vary
    - some venues require multiple websocket connections once scaling symbol count
    - if easy to DDoS oneself during deployments; avoid exceeding IP limits
- Heartbeats are exchange-specific
    - Some exchanges expect client pings, some send pings and expect poings, some send app-level heartbeat messages
    - A TCP connection can stay "open" while the feed is effectively dead (NAT timeouts, half-open sockets). Only checking ws.closed is not enought
- One slow consoumer can freeze the whole adapter
    - beware of bursty feeds
    - related trap: synchronous logging / heavy serialization inside receive task
- Shared mutable state races
    - prefer immutable snapshots or copy-on-write views



## Live Data Specific
- Cross- stream ordering is not guaranteed
    - Trades may arrive before the corresponding book move (or vice versa)
    - if one merges multiple subscription into one unified stream and assume causal ordering, one ocasionally gets nonsense features/signals
- Symbol/instrument metadata changes mid-run.
    - Tick size, lot-size, contract multiplier, min notional, leverage limits, even symbol status can change without warning
    - If you cache metadata at startup and never refresh, you can trade with invalid rounding/validation later.
- Vendor library quirks
    - If you rely on 3rd-party WS clients/exchange SDKs:
    - Some quietly drop large frames or choke on compression.
    - Some do reconnection internally and you end up double-subscribed (looks like “random duplicates”).
    - Some don’t expose enough info to correctly handle exchange-specific heartbeat semantics.
- Corrupted data
    - missing values in message
    - Wrong values in message
    - missing message
    - delayed message
