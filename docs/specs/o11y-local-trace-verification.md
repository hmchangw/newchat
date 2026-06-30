# Local o11y Trace Verification

This checklist verifies the trace shape that the chat repo can produce before
the o11y SDK grows JetStream `Fetch()` and request/reply reply-link support.
It intentionally does not replace `docs/specs/o11y-trace-design.md`; use this
as the repeatable local smoke procedure.

## Preconditions

- Local stack is running with Tempo and Grafana.
- `OTEL_ENABLED=true` for `chat-frontend`.
- Backend services use `pkg/obs.Init` and `pkg/natsutil.Connect`.
- NATS tracing gates are enabled by `pkg/natsutil.Connect`.

Useful local endpoints:

- Frontend: `http://localhost:3000`
- Grafana: `http://localhost:3001`
- Tempo API: `http://localhost:3200`

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
- Recipient `chat-frontend`, when a subscribed browser receives the room event:
  `nats receive chat.room.<roomID>.event.*` with a link to the upstream NATS
  producer context.

Known gap:

- `search-sync-worker` consume is still absent from the constellation until the
  o11y SDK/facade wraps JetStream pull `Fetch()`.

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

Known gap:

- The requester span captures user-perceived NATS request duration, but the
  reply message itself is not linked back as its own span until request/reply
  reply correlation is added.

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

Expected fail until SDK work lands:

- `search-sync-worker` consume appears as a linked consumer span.
- Request/reply reply message has its own backward correlation to the requester.
