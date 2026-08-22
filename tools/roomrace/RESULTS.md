# roomrace — recorded runs (2026-08-22)

Verbatim output of `./tools/roomrace/run.sh` plus an instant-client (`-render-ms 0`) sweep.
Host: 4 vCPU, NATS 2.11-alpine in Docker.

## Realistic client (`-render-ms 30`)

```
### SINGLE SERVER ###

=== roomrace: single-server ===
client B  -> nats://127.0.0.1:4222
services  -> nats://127.0.0.1:4222
iterations per point: 40   client render delay: 30ms

subscription interest window (Subscribe() -> publisher actually reaches it)
  samples 40, delivered 40 | median 40us  p95 1538us  max 1598us | Flush() RTT median 295us

scenario                                       fix                       +0ms   +10ms   +20ms   +25ms   +28ms   +30ms   +32ms   +35ms   +40ms
---------------------------------------------------------------------------------------------------------------------------------------------
dm / client drops unknown room                 none (today)              100%    100%    100%    100%     98%      0%      0%      0%      0%
dm / client buffers unknown room               client tolerates            0%      0%      0%      0%      0%      0%      0%      0%      0%
channel / subscribe on update                  none (today)              100%    100%    100%    100%    100%     70%      0%      0%      0%
channel / subscribe + flush                    client flush              100%    100%    100%    100%    100%     60%      0%      0%      0%
channel / subscribe + flush + backfill         client backfill             0%      0%      0%      0%      0%      0%      0%      0%      0%
channel / server join grace window             server grace window         0%      0%      0%      0%      0%      0%      0%      0%      0%
channel / grace window, client still drops     server only               100%    100%    100%    100%     92%     12%      0%      0%      0%

(cell = % of first messages user B never saw; columns = delay between
 subscription.update and the first message)

detail (totals across all delays)                  live  recover   missed  dropped    dupes
-------------------------------------------------------------------------------------------
dm / client drops unknown room                      161        0      199      199        0
dm / client buffers unknown room                    360        0        0        0        0
channel / subscribe on update                       132        0      228        0        0
channel / subscribe + flush                         136        0      224        0        0
channel / subscribe + flush + backfill              145      215        0        0        0
channel / server join grace window                  360        0        0        0      142
channel / grace window, client still drops          158        0      202      202      138


### 3-NODE CLUSTER (client on node a, services on node b) ###

=== roomrace: 3-node cluster (B on a, services on b) ===
client B  -> nats://127.0.0.1:4223
services  -> nats://127.0.0.1:4224
iterations per point: 40   client render delay: 30ms

subscription interest window (Subscribe() -> publisher actually reaches it)
  samples 40, delivered 40 | median 1446us  p95 1809us  max 4968us | Flush() RTT median 254us

scenario                                       fix                       +0ms   +10ms   +20ms   +25ms   +28ms   +30ms   +32ms   +35ms   +40ms
---------------------------------------------------------------------------------------------------------------------------------------------
dm / client drops unknown room                 none (today)              100%    100%    100%    100%    100%      0%      0%      0%      0%
dm / client buffers unknown room               client tolerates            0%      0%      0%      0%      0%      0%      0%      0%      0%
channel / subscribe on update                  none (today)              100%    100%    100%    100%    100%     98%      0%      0%      0%
channel / subscribe + flush                    client flush              100%    100%    100%    100%    100%     95%      0%      0%      0%
channel / subscribe + flush + backfill         client backfill             0%      0%      0%      0%      0%      0%      0%      0%      0%
channel / server join grace window             server grace window         0%      0%      0%      0%      0%      0%      0%      0%      0%
channel / grace window, client still drops     server only               100%    100%    100%    100%    100%      0%      0%      0%      0%

(cell = % of first messages user B never saw; columns = delay between
 subscription.update and the first message)

detail (totals across all delays)                  live  recover   missed  dropped    dupes
-------------------------------------------------------------------------------------------
dm / client drops unknown room                      160        0      200      200        0
dm / client buffers unknown room                    360        0        0        0        0
channel / subscribe on update                       121        0      239        0        0
channel / subscribe + flush                         122        0      238        0        0
channel / subscribe + flush + backfill              119      241        0        0        0
channel / server join grace window                  360        0        0        0      121
channel / grace window, client still drops          160        0      200      200      120

DONE

[exited with code 0]
```

## Instant client (`-render-ms 0`) — isolates the transport window

```
=== roomrace: instant client, single server ===
client B  -> nats://127.0.0.1:4222
services  -> nats://127.0.0.1:4222
iterations per point: 60   client render delay: 0ms


=== roomrace: instant client, 3-node cluster (B on a, services on b) ===
client B  -> nats://127.0.0.1:4223
services  -> nats://127.0.0.1:4224
iterations per point: 60   client render delay: 0ms

subscription interest window (Subscribe() -> publisher actually reaches it)
  samples 40, delivered 40 | median 1443us  p95 1610us  max 1676us | Flush() RTT median 251us

scenario                                       fix                       +0ms    +1ms    +2ms    +3ms    +5ms   +10ms
---------------------------------------------------------------------------------------------------------------------
dm / client drops unknown room                 none (today)                0%      0%      0%      0%      0%      0%
dm / client buffers unknown room               client tolerates            0%      0%      0%      0%      0%      0%
channel / subscribe on update                  none (today)               98%      0%      0%      0%      0%      0%
channel / subscribe + flush                    client flush              100%      0%      0%      0%      0%      0%
channel / subscribe + flush + backfill         client backfill             0%      0%      0%      0%      0%      0%
channel / server join grace window             server grace window         0%      0%      0%      0%      0%      0%
channel / grace window, client still drops     server only                 2%      0%      0%      0%      0%      0%

(cell = % of first messages user B never saw; columns = delay between
 subscription.update and the first message)

detail (totals across all delays)                  live  recover   missed  dropped    dupes
-------------------------------------------------------------------------------------------
dm / client drops unknown room                      360        0        0        0        0
dm / client buffers unknown room                    360        0        0        0        0
channel / subscribe on update                       301        0       59        0        0
channel / subscribe + flush                         300        0       60        0        0
channel / subscribe + flush + backfill              283       77        0        0       15
channel / server join grace window                  360        0        0        0      300
channel / grace window, client still drops          359        0        1        1      300

```
