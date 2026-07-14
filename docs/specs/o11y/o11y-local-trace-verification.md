# Local o11y Trace Verification

This checklist verifies the trace shape that the chat repo can produce with
`github.com/flywindy/o11y` v0.8.0 JetStream `Fetch()` and request/reply
reply-link support.
It intentionally does not replace `docs/specs/o11y/o11y-trace-design.md`; use this
as the repeatable local smoke procedure.

## Preconditions

- The local o11y backend is up (Tempo + Prometheus + Loki + Grafana):
  ```bash
  docker compose -f docker-local/compose.deps.yaml up -d   # creates chat-local
  docker compose -f docker-local/compose.o11y.yaml  up -d
  ```
  (see `docker-local/compose.o11y.yaml` — collector on `:4318`, so services'
  default `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` just works).
- **`O11Y_ENABLED=true` on every backend service** — observability is OFF by
  default (zero-impact). `make dev` sets this automatically; for docker-compose
  add `- O11Y_ENABLED=true`. Without it, services export nothing and Tempo/Loki
  stay empty.
- `OTEL_ENABLED=true` for `chat-frontend`.
- Backend services use `pkg/obs.Init` and `pkg/natsutil.Connect`.
- NATS tracing gate is enabled by `pkg/natsutil.Connect` **only when
  `O11Y_ENABLED=true`** (otherwise the hot path stays native-cost).

Useful local endpoints:

- Frontend: `http://localhost:3000`
- Grafana: `http://localhost:3001` (anonymous admin; Tempo/Prometheus/Loki pre-wired)
- Tempo API: `http://localhost:3200`
- Prometheus: `http://localhost:9090`

## Query Hints

Start in Grafana Explore with the Tempo datasource and a recent time range.
Useful filters:

```traceql
{ resource.service.name = "chat-frontend" }
{ name =~ "nats publish .*" }
{ name =~ "nats request .*" }
{ name =~ "nats receive .*" }
{ span.messaging.destination.name =~ "chat.*" }
```

For HTTP noise checks:

```traceql
{ name = "OPTIONS" }
```

That query should not return auth/portal/upload preflight traces after CORS is
registered before `o11y/gin`.

## Last Local Verification Result

### 2026-07-11 full Compose verification

Environment: the local frontend plus `compose.deps.yaml`, `compose.o11y.yaml`,
and `compose.services.yaml`, with all affected images rebuilt. The sample-data
seed populated 10 users, 6 rooms, 23 subscriptions, 6 room keys, and 4 Valkey
restricted-room entries. Browser telemetry used `X-Debug=trace`.

Result: **A, B, C, E, and F passed; G passed for the backend fan-out and browser
receive legs; D was not executable with the one-site local topology.** Each
NATS hop remains a separate trace connected by links, as designed.

Representative trace IDs:

| Scenario | Leg | Trace ID |
|---|---|---|
| A/C group send | browser publish | `af6c213b496c6826bde76768a022c4bf` |
| A/C group send | gatekeeper | `0cd1848dfb3c0b2dfa935152f18f9cc9` |
| A/C group send | message-worker | `aabbfcababe0c9d3f687f253a2937f1f` |
| A/C group send | broadcast-worker | `d062dc9e4a02d7814b8145c6a1ee16c3` |
| A/C group send | notification-worker | `36c488d1053678ba2b32f21466163d7b` |
| A/C group send | search consume / bulk | `545745a7eac3cb16cf3275dec7ad07a9` / `40fb1d555f1ce4dda046270891122126` |
| A/C group send | browser receive/render | `7a6a1b5d1fb592a1c474fd15607e6fd6` |
| B room switch | browser history / backend history | `4a47d4cf9e1c6478e85748080fde745` / `3d17646f6d267a464e45decb6583fe06` |
| B room switch | browser read / backend read | `550e041872d947e9987ca21dc25189bd` / `af3b71651a806ada6ee4957f0d143e15` |
| E first DM create | browser / room-service / room-worker | `db6e4f06a9b7e93f92434e32c56800a0` / `5518e1441c7810802ceedfee2967b941` / `b427e9857967b0ed34d5142a7b8057ea` |
| E DM send | browser / gatekeeper / message-worker | `b9244fbe0ba626fbb6fca2cdbe8f1671` / `211709ca2b062944ff34c7dc4df29efb` / `bfe6eefd17ce201f82c18a8450579a01` |
| E DM send | broadcast / notification / search bulk / browser receive | `19e1fdb019cc9559a4663463eea6a335` / `ebacc68d569abdd14aa045d5ff129a81` / `9514d29db11ce15b0179257fe7c0679c` / `a7ec215737ad3b3efc90420be5ae017` |
| F create channel | browser / room-service / room-worker | `42deecabb8f7227116a913f0771904bc` / `d03571daee6f2b2543fd6a9d665425b1` / `9ab533a8c399801f8ae7fd95c81167de` |
| F add Carol | browser / room-service / room-worker / async receive | `c038e2a4bd672d27dfe2c16e9af61f2e` / `fee56b4389c93786429667f7aa321ba` / `65c8f2a8561c77b4d30d1b2901243e5a` / `3e059b13ba6347a7014deb331b4f497b` |
| G edit | history / broadcast / search consume / bulk / browser receive | `1cd5a144f820ab183e36fe6b58c1f1f5` / `de08643f4a0f00c2c760427a2a404e7f` / `2d29ae9cbd931f6ad85c7a84d80a9e10` / `1c28d31b46b29bdb495eee5a7ef48aec` / `55b343fd350c72cf4fd6a7c36965da46` |
| G delete | history / broadcast / search consume / bulk / browser receive | `7406f4dfee6ceb945fdc677544c7ea71` / `d9ecae042aaaec16c827a3f03a6cb0fc` / `3d07a1456191292321f58f69fbd4591c` / `53f8fbd6947fe553de99c60348bae49d` / `7d930bc1761835395c1ffc393a4bab10` |

Log correlation evidence for request
`779886d5-96c6-4f5d-ae74-dd844b03d539`:

- gatekeeper trace `0cd1848dfb3c0b2dfa935152f18f9cc9`: four admitted
  FLOW/DEBUG records in Loki (`received`, subscription decision, large-room
  decision, canonical publish), all with the same `trace_id` and `span_id`;
- message-worker trace `aabbfcababe0c9d3f687f253a2937f1f`: four admitted
  records; broadcast trace `d062dc9e4a02d7814b8145c6a1ee16c3`: three;
- Grafana Tempo -> Loki returned the four gatekeeper records in the configured
  `-2s/+2s` range, and expanding a Loki row exposed `View trace`, which opened
  the same Tempo trace. This verifies both jump directions against OTLP
  structured metadata, not a regex over the log body.

Known limits and findings from this run:

- Scenario D needs a second site/supercluster. No `outbox-worker` or
  `inbox-worker` trace was generated in the one-site stack, so cross-site links
  are **not verified**, rather than treated as passed.
- Scenario G was triggered through the repo's NATS debug client because the
  hover-only edit/delete controls were not automatable in the in-app browser.
  Backend Cassandra/canonical/search fan-out and browser receive/render passed;
  the browser requester span was therefore not part of this G run.
- A channel name alone is insufficient: room-service requires at least one
  non-owner member. The reused `empty request` error text can be misleading,
  but a channel with Alice and a later Carol add completed successfully.
- Frontend search originally omitted `siteId` from search subjects and returned
  503. The corrected `search.<siteId>.rooms/messages` subjects passed in the UI
  and generated search-service metrics.

Code and configuration gates from the same checkout:

- frontend Vitest suite: 65 files and 677 tests passed;
- frontend typecheck and production build passed (the build retained the
  existing chunk-size and non-module `config.js` warnings);
- focused Go tests for the changed observability packages and services passed;
- `go vet ./...` and all four local Compose `config --quiet` checks passed;
- `go test ./...` had one remaining failure in
  `tools/loadgen/TestPresenceSustained_EmbeddedEndToEnd`: the 2.34-second run
  collected no latency sample (`P95Ms=0`). The same failure reproduced in
  isolation; it is outside the o11y integration paths and remains a repository
  test-harness risk rather than being reported as a passing gate.

### 2026-07-08 historical verification

Verified locally on 2026-07-08 with the Docker Compose local stack
(`compose.deps.yaml`, `compose.o11y.yaml`, `compose.services.yaml`) and
`github.com/flywindy/o11y` v0.8.0.

Result: **pass for the expected linked-trace model**. Each NATS hop appears as
its own trace and is correlated by span links, while browser-to-backend HTTP
continues as parent/child in the same trace. The exact trace IDs are
run-specific; use the request IDs below plus `messaging.destination.name` and
span links in Tempo when comparing another run.

Request IDs generated by the scenario probe:

| Scenario | Request ID |
|---|---|
| A/C group send | `019f4254-a5cf-7e4a-82bc-fae89f170566` |
| B history get | `019f4254-b95b-77d1-ad46-c09330374dc7` |
| B message read | `019f4254-b967-7e15-abc2-d21112eaba87` |
| D inbox | `019f4254-b998-77fe-96b6-decb26dd384e` |
| E DM send | `019f4254-c964-79bb-b1cc-40ce6010f42b` |
| F create channel | `019f4254-dcf0-7b3e-bf72-a8a6be4a9dba` |
| F add member | `019f4254-dd21-7a48-b441-65e4913d20a8` |
| G edit message | `019f4254-fc8e-7ee1-a993-24a2e932bc05` |
| G delete message | `019f4255-102d-7ea2-9bd3-cfe2ff33426f` |

Tempo evidence captured from that run:

| Service | Trace IDs |
|---|---|
| scenario-probe | `6c5c0e0667e9af682a216edaf7c6d0dd`, `cde1c6787a4625e4ebe0c90caef56722`, `54dbe14b12e659ccbddf6d3a57e97531`, `a92bc5d4fbb1ac3e483705b721c68923`, `52979cc5d7df2a46896164c3fe1a03f`, `378cec129531eba6a9b7aeb1de5b9265`, `874c8442090776d1d9745d4df0598b12`, `94ee2656e20daace87538ca306db1d42`, `5da6327d029c14f9e4f938d66e33a879` |
| message-gatekeeper | `563d0f8b1210f6fe0aee74ffbbbe3a45`, `cdf8f81350dc0b961e741c524f6fc628` |
| message-worker | `f7b12dfd27524d7760966150b6a30601`, `631c69b41bef2def3b445b9e530330d1`, `b7bb035a5d4f23fcf4154fc2b3222bd1`, `c03480227f6ec9cf1858f4d2fe12af07` |
| broadcast-worker | `52c3599023904e053e95ecf37d5349c8`, `964b08e9364257f6e613d5be28a12a47`, `ffb25697ec90fe2c94ec0289cc844c39`, `14851635f42c3f4905c2c48ae451c6c1`, `597a227e4240e2e99c36c5c9aea92f80`, `9a83668d1fe8574272fb72d9ac03ad2f` |
| notification-worker | `44ef09d181c1c61cd885884ea027d38a`, `a6610b27e7f0c3e9c20e33434765b622`, `a3f739308c062e540b168fcede390b8e`, `9bbe0a08696cf1c57611597a3cdd62f6` |
| search-sync-worker | `915105d198d132e20b5ab164476fb3a3`, `ec2cea9ac136015d18a5bc7b923883aa`, `324ecc916e78267d38505e9d0834d966`, `3762e2b13e45abc4aa00ff3885a9813`, `1b15022212a57a4d2460f103a2892534`, `6674a2665378016f9443bd29c9ca13ae` |
| history-service | `952d266e6cf72c98b331286a8247efc3`, `9c5ad5969b074fcbd2832e58dcb034ab`, `bf7c4fb6a77876f69820142f38dc45d6` |
| room-service | `118156a8039cb0fe53d3136e15557a93`, `226424220c2256ff1e7d0341f71d7b29`, `6ed412a562046bb301a2ef67d4147b07` |
| room-worker | `760761a6064422b91fa3f79bc741eef7`, `b037ec736624ab51b61ebe707a27709a` |
| inbox-worker | `4731eb17169987aead8728bcf93eed37` |

Observed pass conditions:

- Browser/frontend emitted outbound NATS spans and receive-side NATS spans.
- Backend NATS hops were separate traces connected by span links, as designed.
- `search-sync-worker` `Fetch()` work appeared with linked consumer spans after
  the o11y v0.8.0 upgrade.
- Request/reply flows emitted requester-side reply receive spans when the
  request path used the o11y NATS facade.
- CORS preflight traces no longer dominated the trace list.

## Scenario A: Send A Room Message

Action:

1. Open the frontend as `alice`.
2. Open a room.
3. Send a message.
4. If possible, open another browser/session as a second room member to receive
   the live event.

Expected visible constellation:

- `chat-frontend`: `nats publish chat.user.<account>.room.<roomID>.<siteID>.msg.send`
  with `messaging.operation.name=publish`.
- `message-gatekeeper`: a consumer/process span linked to the browser publish
  span, plus Mongo/Valkey spans and a producer span for
  `chat.msg.canonical.<siteID>.created`.
- `message-worker`: a consumer/process span linked to the gatekeeper producer
  span, plus Cassandra/Mongo spans.
- `broadcast-worker`: a consumer/process span linked to the gatekeeper producer
  span, plus room metadata lookup spans and room-event producer spans.
- `notification-worker`: a consumer/process span linked to the gatekeeper
  producer span, plus notification lookup/publish spans.
- `search-sync-worker`: a JetStream `Fetch()` consumer span linked to the
  gatekeeper producer span, followed by `search-sync bulk flush` with links to
  the source message spans and Elasticsearch bulk spans underneath it.
- Recipient `chat-frontend`, when a subscribed browser receives the room event:
  `nats receive chat.room.<roomID>.event.*` with a link to the upstream NATS
  producer context.

## Scenario B: Open Or Switch A Room

Action:

1. Open the frontend as `alice`.
2. Switch to a room with existing messages.

Expected visible constellation:

- `chat-frontend`: `nats request <history-subject>` or
  `nats request_async_result <subject>`, with `chat.request_id`.
- `history-service`: consumer/process span linked to the browser request span,
  plus Mongo access checks and Cassandra read spans.
- Read receipt/update path, if triggered: frontend publish/request span and a
  linked backend consumer span.

Expected:

- The requester span captures user-perceived NATS request duration.
- Backend Go request/reply clients using `o11y/nats.Conn.Request` should emit a
  requester-side `receive <subject>` span when the responder replied through
  `Conn.Respond`.

## Scenario C: Receive A Live Message

Action:

1. Keep Bob's browser subscribed to a room.
2. Send a message from Alice.

Expected:

- Bob's browser emits `nats receive <actual-room-event-subject>`.
- The receive span has `messaging.subscription.name` set to the subscription
  pattern and `messaging.destination.name` set to the concrete message subject.
- The receive span is a consumer span and should be linked to the upstream NATS
  producer context when the message carries `traceparent`.

## Pass/Fail Summary

Pass:

- NATS browser spans include concrete subjects in the span name.
- Browser receive-side spans appear for subscribed events.
- Auth/portal/upload `OPTIONS` traces are absent or no longer dominate Explore.
- Backend NATS consumers are correlated by span links rather than a single
  forced trace ID.

Still worth checking explicitly:

- `search-sync-worker` consume appears as a linked consumer span.
- Go service-to-service request/reply has a requester-side reply receive span
  when both sides use the o11y NATS facade.
