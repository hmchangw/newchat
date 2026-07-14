"""Render two architecture diagrams for the bot-platform login spec.

- chat-ns.png: legacy `chat` namespace + Istio VirtualService 0->100 cutover.
- wsp-ns.png:  nextgen `wsp` namespace + bot platform topology.

All hostnames and siteIds are generic placeholders (no internal company strings).
"""
from __future__ import annotations
import os
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Polygon

HERE = os.path.dirname(os.path.abspath(__file__))


def box(ax, x, y, w, h, text, *, fc="#ffffff", ec="#37474f", lw=1.2,
        fontsize=9, fontweight="normal", fontstyle="normal",
        family="sans-serif", ha="center", va="center", alpha=1.0):
    """Rounded rectangle with centered text. Returns (cx, cy) for arrow targets."""
    p = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.15",
                       fc=fc, ec=ec, lw=lw, alpha=alpha)
    ax.add_patch(p)
    ax.text(x + w / 2, y + h / 2, text, ha=ha, va=va,
            fontsize=fontsize, fontweight=fontweight, fontstyle=fontstyle,
            family=family, wrap=True, alpha=alpha)
    return x + w / 2, y + h / 2


def rect(ax, x, y, w, h, text, *, fc="#ffffff", ec="#37474f", lw=1.2,
         fontsize=9, fontweight="normal", family="sans-serif", va="center"):
    """Plain rectangle (no rounded corners). For YAML-style blocks."""
    p = patches.Rectangle((x, y), w, h, fc=fc, ec=ec, lw=lw)
    ax.add_patch(p)
    ax.text(x + 0.15, y + h - 0.25, text, ha="left", va="top",
            fontsize=fontsize, fontweight=fontweight, family=family,
            wrap=True)
    return x + w / 2, y + h / 2


def diamond(ax, x, y, w, h, text, *, fc="#fffde7", ec="#f9a825", lw=1.4,
            fontsize=9):
    """Decision diamond. Returns center."""
    cx, cy = x + w / 2, y + h / 2
    pts = [(cx, y + h), (x + w, cy), (cx, y), (x, cy)]
    p = Polygon(pts, fc=fc, ec=ec, lw=lw)
    ax.add_patch(p)
    ax.text(cx, cy, text, ha="center", va="center", fontsize=fontsize, wrap=True)
    return cx, cy


def cyl(ax, x, y, w, h, text, *, fc="#eceff1", ec="#37474f", lw=1.2,
        fontsize=9):
    """Cylinder shape (database)."""
    rx = w / 2
    ry = h * 0.12
    # body
    p = patches.Rectangle((x, y + ry), w, h - 2 * ry, fc=fc, ec=ec, lw=lw)
    ax.add_patch(p)
    # bottom ellipse
    e1 = patches.Ellipse((x + rx, y + ry), w, 2 * ry, fc=fc, ec=ec, lw=lw)
    ax.add_patch(e1)
    # top ellipse
    e2 = patches.Ellipse((x + rx, y + h - ry), w, 2 * ry, fc=fc, ec=ec, lw=lw)
    ax.add_patch(e2)
    # Hide the vertical seam where the rectangle meets the bottom ellipse.
    seam = patches.Rectangle((x, y + ry - 0.005), w, 0.01, fc=fc, ec=fc, lw=0)
    ax.add_patch(seam)
    ax.text(x + rx, y + h / 2, text, ha="center", va="center", fontsize=fontsize)
    return x + rx, y + h / 2


def arrow(ax, src, dst, *, color="#37474f", lw=1.3, style="-|>", label=None,
          label_offset=(0, 0.18), label_fs=8, ls="-"):
    a = FancyArrowPatch(src, dst, arrowstyle=style, mutation_scale=14,
                        color=color, lw=lw, ls=ls,
                        shrinkA=4, shrinkB=4)
    ax.add_patch(a)
    if label:
        mx = (src[0] + dst[0]) / 2 + label_offset[0]
        my = (src[1] + dst[1]) / 2 + label_offset[1]
        ax.text(mx, my, label, ha="center", va="center",
                fontsize=label_fs, color=color, family="monospace",
                bbox=dict(fc="white", ec="none", pad=1))


def actor(ax, x, y, label, *, color="#1565c0"):
    """Stick figure for human user."""
    ax.add_patch(patches.Circle((x, y + 0.4), 0.12, fc=color, ec=color))  # head
    ax.plot([x, x], [y + 0.28, y - 0.15], color=color, lw=2)  # body
    ax.plot([x - 0.18, x + 0.18], [y + 0.0, y + 0.0], color=color, lw=2)  # arms
    ax.plot([x, x - 0.15], [y - 0.15, y - 0.4], color=color, lw=2)  # left leg
    ax.plot([x, x + 0.15], [y - 0.15, y - 0.4], color=color, lw=2)  # right leg
    ax.text(x, y - 0.55, label, ha="center", va="top", fontsize=9, color=color)
    return x, y - 0.05


def bot_icon(ax, x, y, label, *, color="#ad1457"):
    """Simple robot icon — square head with antennae."""
    # head
    ax.add_patch(patches.Rectangle((x - 0.2, y), 0.4, 0.32, fc="#fce4ec",
                                   ec=color, lw=1.5))
    # eyes
    ax.add_patch(patches.Circle((x - 0.08, y + 0.18), 0.04, fc=color))
    ax.add_patch(patches.Circle((x + 0.08, y + 0.18), 0.04, fc=color))
    # mouth
    ax.plot([x - 0.08, x + 0.08], [y + 0.07, y + 0.07], color=color, lw=1.5)
    # antennae
    ax.plot([x - 0.1, x - 0.1], [y + 0.32, y + 0.42], color=color, lw=1.5)
    ax.plot([x + 0.1, x + 0.1], [y + 0.32, y + 0.42], color=color, lw=1.5)
    ax.add_patch(patches.Circle((x - 0.1, y + 0.44), 0.025, fc=color))
    ax.add_patch(patches.Circle((x + 0.1, y + 0.44), 0.025, fc=color))
    ax.text(x, y - 0.18, label, ha="center", va="top", fontsize=9, color=color)
    return x, y


# ===========================================================================
# DIAGRAM 1 — namespace: chat (legacy) — Istio cutover view
# ===========================================================================

def diagram_chat_ns():
    fig, ax = plt.subplots(figsize=(20, 14))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 14)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_facecolor("#fafafa")

    # Title + namespace label
    ax.text(10, 13.5, "Namespace: chat (legacy)",
            ha="center", va="center", fontsize=17, fontweight="bold",
            color="#263238")
    ax.text(10, 13.0, "Login routing during Istio VirtualService 0→100 cutover to wsp namespace",
            ha="center", va="center", fontsize=11, fontstyle="italic",
            color="#546e7a")

    # Bot icons + URLs (top right)
    bot_icon(ax, 11.5, 11.5, "Bot SDK")
    bot_icon(ax, 15.5, 11.5, "Bot SDK")
    ax.text(11.5, 12.2,
            "https://<siteId>.chat.<domain>/api/v1/login\nhttps://<siteId>.chat.<domain>/api/v1/chat.message",
            ha="center", va="bottom", fontsize=7.5, family="monospace",
            color="#37474f")
    ax.text(15.5, 12.2,
            "https://botplatform-<siteId>.chat.<domain>/api/v2/...",
            ha="center", va="bottom", fontsize=7.5, family="monospace",
            color="#37474f")

    # Actor + Desktop 2.0 (top left)
    actor(ax, 1.8, 11.5, "User")
    bot_icon(ax, 2.6, 11.5, "Desktop 2.0")

    # Desktop 2.0 login URL
    ax.text(2.0, 10.5,
            "POST https://portal.<domain>/v1/login\n{ username, password }",
            ha="center", va="center", fontsize=8, family="monospace",
            color="#37474f",
            bbox=dict(fc="#e3f2fd", ec="#1565c0", lw=1, boxstyle="round,pad=0.3"))

    # Desktop 2.0 box
    box(ax, 1.0, 9.0, 2.2, 0.6, "Desktop 2.0 Login\n(username / password)",
        fc="#e1f5fe", ec="#0277bd", fontsize=9)

    # Decision: require pass change?
    diamond(ax, 1.3, 7.5, 1.6, 1.0, "require\npass\nchange?",
            fc="#fff3e0", ec="#ef6c00", fontsize=8)

    # Two branches
    ax.text(0.4, 7.8, "yes", fontsize=8, color="#ef6c00", fontweight="bold")
    ax.text(3.0, 7.8, "no", fontsize=8, color="#ef6c00", fontweight="bold")

    # Update pwd + Desktop terminal
    box(ax, -0.1, 6.2, 1.4, 0.6, "Update pwd\npage",
        fc="#fff8e1", ec="#f9a825", fontsize=8)
    box(ax, 2.7, 6.2, 1.4, 0.6, "Desktop\n(connected)",
        fc="#e8f5e9", ec="#2e7d32", fontsize=8)

    # Sample portal response JSON (bottom left)
    json_text = (
        '{\n'
        '  "userId":                "<17-char>",\n'
        '  "authToken":             "<43-char base64url>",\n'
        '  "account":               "<botname>.<shortcode>.bot",\n'
        '  "siteId":                "<siteId>",\n'
        '  "authServiceUrl":        "<auth URL home site>",\n'
        '  "baseUrl":               "<chat REST base URL home site>",\n'
        '  "natsUrl":               "<NATS WebSocket URL home site>",\n'
        '  "requirePasswordChange": false\n'
        '}'
    )
    ax.text(0.3, 4.4, "portal /v1/login response (8 fields):",
            fontsize=9, fontweight="bold", color="#0277bd")
    ax.text(0.2, 4.2, json_text, ha="left", va="top", fontsize=7.5,
            family="monospace", color="#37474f",
            bbox=dict(fc="#f5f5f5", ec="#9e9e9e", lw=0.8, boxstyle="round,pad=0.3"))

    # tchat-gateway (top center)
    gw = box(ax, 12.0, 9.5, 2.5, 0.8, "tchat-gateway\n(Istio ingress)",
             fc="#e8eaf6", ec="#3949ab", fontsize=10, fontweight="bold")

    # Arrows from bots to gateway
    arrow(ax, (11.5, 11.3), (12.8, 10.3))
    arrow(ax, (15.5, 11.3), (14.0, 10.3))

    # Istio VirtualService YAML blocks
    yaml1 = (
        "match:\n"
        "  uri: { exact: /api/v1/login }\n"
        "route:\n"
        "  - destination:\n"
        "      host: svc-<siteId>-botplatform.chat.svc.cluster.local\n"
        "      weight: 0           # legacy off\n"
        "  - destination:\n"
        "      host: svc-<siteId>-botplatform.wsp.svc.cluster.local\n"
        "      weight: 100          # nextgen on"
    )
    yaml2 = (
        "match:\n"
        "  uri: { prefix: /api/v2 }\n"
        "route:\n"
        "  - destination:\n"
        "      host: svc-<siteId>-botplatform.chat.svc.cluster.local\n"
        "      weight: 0\n"
        "  - destination:\n"
        "      host: svc-<siteId>-botplatform.wsp.svc.cluster.local\n"
        "      weight: 100"
    )
    rect(ax, 5.5, 6.5, 5.5, 2.3, yaml1, fc="#fff8e1", ec="#f57f17",
         lw=1.2, fontsize=7.5, family="monospace")
    rect(ax, 12.5, 6.5, 5.5, 2.3, yaml2, fc="#fff8e1", ec="#f57f17",
         lw=1.2, fontsize=7.5, family="monospace")

    # Routes from gateway to YAML
    arrow(ax, gw, (8.25, 8.8), label="match\n/api/v1/login", label_fs=7,
          label_offset=(0.5, 0.0))
    arrow(ax, gw, (15.25, 8.8), label="match\n/api/v2/*", label_fs=7,
          label_offset=(0.5, 0.0))

    # Destinations: legacy (faded, weight 0)
    box(ax, 5.5, 4.0, 3.0, 0.9,
        "tchat 1.0 server\n(legacy — weight 0)",
        fc="#ffebee", ec="#c62828", fontsize=9, alpha=0.55)
    cyl(ax, 6.3, 2.0, 1.4, 1.0, "MongoDB", fc="#fce4ec", ec="#c62828",
        fontsize=8)
    arrow(ax, (7.0, 4.0), (7.0, 3.05), color="#c62828", lw=1.0, ls="--")

    # Destinations: nextgen (full color, weight 100)
    box(ax, 12.5, 4.0, 3.0, 0.9,
        "botplatform\n(nextgen — weight 100)",
        fc="#e8f5e9", ec="#2e7d32", fontsize=9, fontweight="bold")
    ax.text(14.0, 3.4,
            "deployed in wsp namespace\nsee wsp-ns diagram →",
            ha="center", va="center", fontsize=8, fontstyle="italic",
            color="#2e7d32")

    arrow(ax, (8.25, 6.5), (7.0, 4.9), color="#c62828", lw=1.0, ls="--")
    arrow(ax, (8.25, 6.5), (14.0, 4.9), color="#2e7d32", lw=1.5)
    arrow(ax, (15.25, 6.5), (14.0, 4.9), color="#2e7d32", lw=1.5)

    # Portal entry (bottom — separate from istio cutover)
    box(ax, 4.5, 9.0, 2.4, 0.7, "portal-service\n(global, GDNS-routed)",
        fc="#e3f2fd", ec="#1565c0", fontsize=9, fontweight="bold")
    # Arrow from Desktop 2.0 → portal
    arrow(ax, (2.1, 9.0), (5.7, 9.2), color="#1565c0", lw=1.3,
          label="POST /v1/login", label_fs=7, label_offset=(0, 0.25))
    # Arrow from portal → Desktop decision
    arrow(ax, (5.7, 9.0), (2.1, 8.5), color="#1565c0", lw=1.2, ls="--",
          label="response", label_fs=7, label_offset=(0, -0.2))

    # Portal forwards east-west to home-site botplatform (in wsp ns)
    arrow(ax, (5.7, 9.5), (12.5, 4.9), color="#2e7d32", lw=1.0, ls=":",
          label="forwards {username,password}\n  east-west to home-site botplatform",
          label_fs=7, label_offset=(2.5, 0.4))

    # Caption at bottom
    ax.text(10, 0.6,
            "Cutover policy: 0→100 direct switch on the VirtualService weight — no canary, no header-based routing.\n"
            "Token wire-format is identical to legacy tchat (opaque base64url, no bp_ prefix), so existing bot SDKs need no code changes.",
            ha="center", va="center", fontsize=9, color="#37474f",
            bbox=dict(fc="#fff9c4", ec="#f9a825", lw=1, boxstyle="round,pad=0.4"))

    out = os.path.join(HERE, "chat-ns.png")
    plt.savefig(out, dpi=140, bbox_inches="tight", facecolor="#fafafa")
    plt.close(fig)
    return out


# ===========================================================================
# DIAGRAM 2 — namespace: wsp (nextgen)
# ===========================================================================

def diagram_wsp_ns():
    fig, ax = plt.subplots(figsize=(20, 15))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 15)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_facecolor("#fafafa")

    # Title
    ax.text(10, 14.5, "Namespace: wsp (nextgen)",
            ha="center", va="center", fontsize=17, fontweight="bold",
            color="#263238")
    ax.text(10, 14.05, "Bot platform + portal + admin topology after cutover",
            ha="center", va="center", fontsize=11, fontstyle="italic",
            color="#546e7a")

    # Top entry points
    ax.text(4.5, 13.2,
            "https://botplatform-<siteId>.wsp.<domain>",
            ha="center", va="bottom", fontsize=8, family="monospace",
            color="#37474f")
    ax.text(10, 13.2,
            "https://portal.<domain>/v1/login",
            ha="center", va="bottom", fontsize=8, family="monospace",
            color="#37474f")
    ax.text(15.5, 13.2,
            "https://admin-<siteId>.wsp.<domain>/login",
            ha="center", va="bottom", fontsize=8, family="monospace",
            color="#37474f")

    # twsp-gateway
    gw = box(ax, 3.0, 11.5, 3.0, 0.8, "twsp-gateway\n(Istio ingress)",
             fc="#e8eaf6", ec="#3949ab", fontsize=10, fontweight="bold")

    # Bot SDK direct → gateway
    bot_icon(ax, 4.5, 13.0, "")
    arrow(ax, (4.5, 12.8), (4.5, 12.3))

    # Portal-service box
    portal = box(ax, 8.5, 11.5, 3.0, 0.8, "portal-service\n(global)",
                 fc="#e3f2fd", ec="#1565c0", fontsize=10, fontweight="bold")

    # Web/Desktop client arrow to portal
    actor(ax, 10, 13.4, "")
    arrow(ax, (10, 12.95), (10, 12.3))

    # Admin Portal Web + Admin Service (right column)
    actor(ax, 15.5, 13.0, "Admin")
    box(ax, 14.0, 11.5, 3.0, 0.8, "Admin Portal Web\n(admin UI for managing bots)",
        fc="#fce4ec", ec="#ad1457", fontsize=9, fontweight="bold")
    arrow(ax, (15.5, 12.55), (15.5, 12.3))

    # Admin Portal -> portal-service (for login)
    arrow(ax, (14.0, 11.7), (11.5, 11.7), color="#ad1457", lw=1.2, ls="--",
          label="login via\nportal /v1/login",
          label_fs=7, label_offset=(0, 0.35))

    # Admin Portal -> Admin Service (operations)
    admin_svc = box(ax, 14.0, 9.0, 3.0, 0.9,
                    "Admin Service\nhttps://admin-api-<siteId>.wsp.<domain>\n/api/v1/bot/<id>/suspend",
                    fc="#fce4ec", ec="#ad1457", fontsize=8)
    arrow(ax, (15.5, 11.5), (15.5, 9.9), color="#ad1457", lw=1.3,
          label="bot ops\n(suspend, rotate)",
          label_fs=7, label_offset=(1.2, 0.0))

    # Nextgen Botplatform (center)
    bp = box(ax, 3.0, 9.0, 6.5, 1.4,
             "Nextgen Botplatform\n"
             "POST /v1/login   (bot SDK — returns legacy {userId, authToken, me})\n"
             "POST /api/v1/login   (alias for legacy wire path)\n"
             "POST /v1/auth/validate   (gateway + auth-service)\n"
             "supports v1 (legacy compat) + v2 API",
             fc="#e8f5e9", ec="#2e7d32", fontsize=8.5, fontweight="bold")

    # Gateway → Botplatform
    arrow(ax, gw, (6.25, 10.4), label="route\n/api/v1, /api/v2",
          label_fs=7, label_offset=(0.5, 0.0))

    # Portal → Botplatform (forwards login)
    arrow(ax, portal, (8.5, 10.0), color="#1565c0", lw=1.3,
          label="forwards login\n(in-cluster + east-west)",
          label_fs=7, label_offset=(1.5, 0.2), ls="--")

    # Botplatform → Mongo (for users + sessions, direct)
    db_users = cyl(ax, 1.0, 7.0, 1.4, 1.0, "users +\nsessions\n(MongoDB)",
                   fc="#e3f2fd", ec="#1565c0", fontsize=7)
    arrow(ax, (3.5, 9.0), (1.7, 8.0), color="#1565c0", lw=1.0,
          label="users find\nsessions read/write",
          label_fs=7, label_offset=(-0.5, 0.3))

    # NATS gateway
    nats_gw = box(ax, 4.5, 7.0, 3.5, 0.9,
                  "NATS gateway\n(JetStream + req/reply broker)",
                  fc="#fff3e0", ec="#ef6c00", fontsize=9, fontweight="bold")
    arrow(ax, (6.0, 9.0), (6.25, 7.9), color="#ef6c00", lw=1.3,
          label="nats req/reply", label_fs=7, label_offset=(0.8, 0.0))

    # Bot Room Service
    brs = box(ax, 2.0, 5.0, 2.5, 0.8, "Bot Room Service",
              fc="#fff3e0", ec="#ef6c00", fontsize=9)
    arrow(ax, (5.0, 7.0), (3.25, 5.8), color="#ef6c00", lw=1.2,
          label="nats req/reply", label_fs=7, label_offset=(-1.0, 0.0))
    cyl(ax, 2.4, 3.2, 1.6, 1.1, "MongoDB\n(rooms,\nsubs)",
        fc="#eceff1", ec="#37474f", fontsize=7)
    arrow(ax, (3.25, 5.0), (3.2, 4.3), color="#37474f", lw=1.0)

    # Bot Msg Handler
    bmh = box(ax, 5.5, 5.0, 3.0, 0.8, "Bot Msg Handler",
              fc="#fff3e0", ec="#ef6c00", fontsize=9)
    arrow(ax, (6.5, 7.0), (7.0, 5.8), color="#ef6c00", lw=1.2,
          label="nats req/reply", label_fs=7, label_offset=(1.2, 0.0))
    cyl(ax, 5.7, 3.2, 1.6, 1.1, "Cassandra\n(messages)",
        fc="#eceff1", ec="#37474f", fontsize=7)
    arrow(ax, (6.5, 5.0), (6.5, 4.3), color="#37474f", lw=1.0)

    # Bot Message Canonical JetStream
    js = box(ax, 9.0, 5.0, 3.8, 0.8,
             "Bot Message Canonical\nJetStream",
             fc="#fff3e0", ec="#ef6c00", fontsize=9)
    arrow(ax, (8.5, 5.4), (9.0, 5.4), color="#ef6c00", lw=1.2,
          label="canonical event", label_fs=7,
          label_offset=(0.5, 0.4))

    # Search Sync Worker → ES
    ssw = box(ax, 9.5, 3.2, 2.5, 0.8, "Search Sync Worker",
              fc="#fff3e0", ec="#ef6c00", fontsize=9)
    arrow(ax, (10.8, 5.0), (10.8, 4.0), color="#ef6c00", lw=1.2)
    cyl(ax, 10.0, 1.5, 1.5, 1.0, "ES",
        fc="#eceff1", ec="#37474f", fontsize=7)
    arrow(ax, (10.8, 3.2), (10.8, 2.5), color="#37474f", lw=1.0)

    # Broadcast Worker
    bw = box(ax, 13.0, 3.2, 2.5, 0.8, "Broadcast Worker",
             fc="#fff3e0", ec="#ef6c00", fontsize=9)
    arrow(ax, (12.5, 5.0), (14.0, 4.0), color="#ef6c00", lw=1.2)

    # Auth-service note (right side)
    box(ax, 14.0, 6.5, 4.0, 1.6,
        "auth-service (existing, extended)\n"
        "POST /auth\n"
        "  ssoToken      → existing OIDC path (UNCHANGED)\n"
        "  authToken     → NEW: validate via botplatform\n"
        "  scope from roles: admin > bot > user",
        fc="#e8eaf6", ec="#3949ab", fontsize=8)
    arrow(ax, (14.0, 7.0), (9.5, 9.5), color="#3949ab", lw=1.0, ls="--",
          label="POST /v1/auth/validate\n(local-site only)",
          label_fs=7, label_offset=(-1.5, 0.3))

    # Bottom caption
    ax.text(10, 0.4,
            "Login is direct Mongo (botplatform reads users + writes sessions). "
            "Bot operations (room, message) flow through NATS req/reply via NATS gateway — not in this PR's scope.\n"
            "Portal cross-site forwarding uses the public botplatform URL of the user's home site (no internal hostnames required).",
            ha="center", va="center", fontsize=9, color="#37474f",
            bbox=dict(fc="#fff9c4", ec="#f9a825", lw=1, boxstyle="round,pad=0.4"))

    out = os.path.join(HERE, "wsp-ns.png")
    plt.savefig(out, dpi=140, bbox_inches="tight", facecolor="#fafafa")
    plt.close(fig)
    return out


if __name__ == "__main__":
    p1 = diagram_chat_ns()
    p2 = diagram_wsp_ns()
    print("Wrote:", p1)
    print("Wrote:", p2)
