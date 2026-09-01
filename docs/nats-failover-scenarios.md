# NATS Failover: Who Connects Where, and Why

A plain-language companion to
[`docs/superpowers/specs/2026-08-15-nats-site-failover-design.md`](./superpowers/specs/2026-08-15-nats-site-failover-design.md).
That spec is the authority on mechanism; this page explains the scenarios and
the reasoning, for anyone who needs to operate, review, or extend the system
without reading the whole design.

**The short version:** when a site's NATS cluster dies but the rest of the site
is healthy, its **users spread out across every surviving site**, while its
**backend services all move to one designated partner site**. Those are opposite
strategies on purpose, and the reason is that they carry opposite kinds of load.

---

## 1. Some vocabulary

| Term | What it means here |
|------|--------------------|
| **Site** | A self-contained deployment: its own NATS cluster, MongoDB, Cassandra, and services. Roughly "one office/region's backend." |
| **Home site** | The site a user belongs to. Their rooms, subscriptions, and message history live in that site's databases. |
| **Supercluster** | All the sites' NATS clusters joined together, so a message published at one site can reach another. |
| **Buddy** | Each site's designated partner site, which hosts its standby streams. Assigned in a ring: A→B, B→C, C→A. |
| **Stream** | A durable queue in NATS. Everything about it — the data, the disks — lives on one specific cluster. |
| **Subject** | The address a message is published to. Publishers use subjects; they never name streams. |

Two facts drive nearly everything below:

- **A stream lives on one cluster.** If that cluster is down, the stream is
  unavailable and there is no way to move it elsewhere — moving a stream
  requires the cluster it currently lives on to be healthy.
- **Subjects route themselves.** A publisher names a subject, and the
  supercluster delivers it to whichever cluster has a stream or subscriber
  listening for it. The publisher never needs to know where that is.

---

## 2. Normal operation

Everyone connects to their home site. A user in the US connects to the US
cluster; US services connect to the US cluster; US messages flow through US
streams into US databases.

Cross-site traffic — a message in a room whose members span two sites — is
carried between clusters by the supercluster.

---

## 3. The failure this design handles

**One site's NATS cluster goes down. Everything else at that site stays up.**

Think: a bad NATS deploy, a lost quorum, a config mistake. The pods are running.
MongoDB, Cassandra, Valkey and Elasticsearch are all fine. Only the message bus
is gone.

This is not the "the whole datacenter burned down" scenario. This design depends
entirely on the site's **databases still being reachable**, because the goal is
that the site's own services keep doing the site's own work against the site's
own data. If the databases are gone too, none of this applies.

Today, without this design, that site's users go completely offline — NATS is
the only way into the chat system.

---

## 4. Where the users go: spread across everyone

A displaced user's app does this:

1. Notice the home cluster isn't answering (about 15 seconds).
2. Ask the portal service for the list of sites.
3. Shuffle that list randomly and try them in order. First one that accepts wins.
4. Stay there until that one also fails.
5. Meanwhile, quietly retry home every so often. When home comes back, go home.

**Why random rather than "the best one"?** Because the goal is to avoid
flattening any single cluster. If all of site A's users landed on one partner
site, that site now carries double the connections — the exact problem we're
avoiding. Random spreading splits them roughly evenly with no coordination
between clients, no health checks, and no central decision.

**Why not pick the closest site by latency?** Because a site's users are all in
roughly the same place — that's *why* they share a home site. If they all
measured latency, they'd all get the same answer and all pile onto the same
partner. Latency ranking would quietly recreate the very hotspot that random
spreading exists to prevent.

---

## 5. Where the services go: all to the buddy

Meanwhile, the site's backend services connect to **one** designated partner —
the buddy — where a set of standby streams has been sitting empty, pre-created,
waiting for exactly this.

Crucially, those services **still read and write their own site's databases**.
That's the property that makes this safe: a US user's message, sent during a US
NATS outage, is still validated by US services and still stored in the US
Cassandra. Nothing lands in the wrong place, and nothing has to be reconciled
afterwards.

**Why not spread the services out too?** Because the work doesn't follow the
connection — it follows the streams. The standby streams are all on the buddy,
so the buddy does that work no matter where the services happen to be connected.
Spreading them wouldn't lighten the buddy's load by a single byte; it would just
add network hops to reach the same streams. On top of that, a site has maybe
tens of service connections versus thousands of user connections, so there's no
capacity pressure to relieve in the first place.

---

## 6. So why the opposite strategies?

Because "load" means two different things.

| | Users | Services |
|---|---|---|
| What their connection costs | Thousands of live connections, subscription state, a copy of every message they should see | A few dozen connections, no fan-out |
| Where their work happens | Wherever they connect | On the buddy, where the streams are |
| Therefore | **Spread** — moving them moves real load | **Pin** — moving them moves nothing |

A user's connection *is* the load, so spreading users spreads load. A service's
connection is just a pipe to the streams, so spreading services spreads nothing
and costs latency.

---

## 7. The subtle part: local vs global rooms

This is the piece that catches people, and it's worth understanding because it
explains a decision that otherwise looks arbitrary.

To save memory across the supercluster, rooms whose members all live at one site
are published on a **local** address that deliberately does **not** travel
between sites. Only rooms with members at multiple sites use a **global**
address that does.

That optimization quietly assumes **a user is connected to their home site.**
Normally true. But a displaced user is, by definition, connected somewhere else
— so all their local-only rooms would go silent. Worse, it would be *partly*
silent: users who happened to land on the same cluster as the relocated service
would still get messages, and users elsewhere wouldn't. Same room, some people
hearing it, some not.

**The fix:** during failover, everything publishes to the global address,
ignoring the local/global distinction entirely. This is safe and correct, not a
hack — if the home cluster is down, there are no users left at home for a
local-only address to reach. Nothing is lost by going global.

**And this is what makes spreading possible at all.** Without it, the only
cluster a displaced user could usefully connect to would be the buddy — the one
place the local address still reaches them. Every other site would be silent, so
everyone would have to pile onto the buddy, and the hotspot would be back. The
global-routing rule and the user-spreading rule are one mechanism, not two.

---

## 8. A related case that is *not* failover: travelling

**"I work in the US and I'm visiting Japan. Should my app connect to Japan?"**

No — it should connect to the US, and it does so automatically today.

Two reasons:

- **Your local-only rooms wouldn't reach you.** Same problem as section 7, for
  the same reason: you'd be connected somewhere other than home.
- **It wouldn't be faster anyway.** Your data and the services that answer your
  requests are all in the US. Connecting to Japan doesn't avoid the long trip
  across the Pacific — it just adds an extra hop in front of it.

The useful thing to take from this: "connect to the nearest site to reduce
latency" cannot be added as a feature without first reworking the local/global
room split. That would be a bigger change than the failover one, because
failover only needs global routing *during an incident*, while a travel feature
would need it permanently — giving up exactly the savings the local addressing
was introduced to capture.

---

## 9. What still works during an outage, and what doesn't

| | During the outage |
|---|---|
| Sending and receiving messages | ✅ Works |
| Reading history, room lists, search, profiles | ✅ Works |
| Messages to and from users at other sites | ✅ Works |
| Federation events *out* to other sites | ✅ Works |
| Federation events *in* from other sites | ✅ Works — redirected to a standby inbox |
| Creating rooms, inviting members | ❌ Stalls until recovery |
| Search index updates for federation events | ❌ Stalls until recovery |

Room creation and invites are deliberately left out of the first version. It's a
smaller, separate piece of work that can be added later if the stall turns out
to matter.

---

## 10. Coming back

Nothing has to be switched back by hand.

Services hold **two** connections the whole time — one home, one to the buddy —
so when home recovers, its lane simply starts delivering again. Any backlog that
piled up drains on its own alongside the failover traffic.

Users drift home gradually, each on their own retry timer, so they trickle back
rather than stampeding.

One deliberate accommodation: for a while after home recovers, room events are
published to **both** the local and global addresses. Servers recover in
seconds, users take up to five minutes to notice, and during that gap some users
are home and some are still on a partner site. Publishing to both keeps
everybody covered until the stragglers return. This mirrors an existing pattern
in the codebase used when a room changes between local and global.

How long that lasts is `FAILOVER_REVERT_GRACE`, 30 minutes by default. It and
the client's five-minute reconnect cap are a **coupled pair, not two independent
knobs**: the window exists only to outlast the client backoff. Raising the
client cap without raising the window reopens exactly the silent gap the
dual-publishing was added to close — and the symptom would be a handful of users
receiving nothing for a few minutes, with no error logged anywhere.

During recovery, messages may briefly arrive slightly out of order, because two
lanes are draining at once. **Stored history is unaffected** — it's ordered by
timestamp when read, not by arrival — so this is cosmetic and short-lived. It
was an accepted trade against the alternatives, which were either stalling
recovery or adding permanent complexity to the message hot path.

---

## 11. Quick reference

| Question | Answer |
|---|---|
| Where do displaced users connect? | A randomly chosen surviving site; they stay put until it too fails |
| Where do displaced services connect? | The buddy, always |
| Does a user's app know about buddies? | No — it never sees the concept |
| Does another site need to know who my buddy is? | No — it publishes to an address, and the network finds the stream |
| What does each site need configured? | Its own buddy. Two values. Never the full map |
| Can two sites use the same stream name? | No — names are unique across the whole supercluster |
| Where does data written during an outage go? | The outage site's own databases, as always |
| Can a stream be moved to another cluster mid-incident? | No. That's why the standby streams are created ahead of time |
| Does any of this need a manual switch? | No. Failover and recovery are both automatic |
