# HLstatsX:CE v2

**A modern, resilient evolution of HLstatsX:CE 1.6.19 — built for Source 2, CS2, and the future of competitive multiplayer.**

HLstatsX:CE v2 is a full-scale rework of the classic real-time stats and ranking system for `srcds`-based games. Designed to thrive in both legacy Source 1 environments and modern CS2 servers, this fork introduces asynchronous RCON, hybrid log ingestion (UDP + HTTP), competitive mode awareness, and a scalable, restart-safe architecture.

Whether you're running a community server or a high-performance competitive hub, HLstatsX:CE v2 delivers unmatched reliability, flexibility, and insight.

---

## 🚀 Key Features

### 🌐 Hybrid Log Ingestion (UDP + HTTP)

- Unified listener for both legacy `srcds` UDP logs and CS2’s HTTP-based log stream — **on the same port**
- Powered by `Mojo::IOLoop` for non-blocking, event-driven performance
- Automatic log format detection and normalization
- Mojo::IOLoop powers the listener, so UDP and HTTP log handling is non-blocking.
- Multiple servers can stream logs concurrently without blocking each other.
- Packet parsing is modular and event-driven — no global locks or serialized queues.

### 🧠 Intelligent Packet Parsing

- Modular parser system handles mixed log formats with precision
- Per-server context tracking: map, hostname, difficulty, player state
- Engine-aware dispatching for Source 1 and Source 2 events

### 🔌 Plugin Compatibility

- **Source 1**: Fully supports legacy `hlstatsx.smx` Sourcemod plugin
- **CS2 / Source 2**: Compatible with HLstatsZ plugin for CounterStrikeSharp
  - Emulates Sourcemod-style events (`hlx_sm_*`)
  - Server mod set as `SOURCEMOD` for seamless integration

### 🛠️ Server-Specific Overrides

- Per-server config for:
  - Custom commands
  - Stat weighting and modifiers
  - Competitive mode toggles

---

## 💡 Why HLstatsX:CE v2?

- **Battle-tested**: Trusted across thousands of servers and millions of player sessions
- **Zero client-side footprint**: No in-game installs or plugins required
- **External server support**: Offloads processing to avoid impacting game performance
- **Fully async**: RCON and log ingestion are non-blocking, restart-safe, and ops-friendly
- **Unlimited servers**: Logs concurrently without blocking each other (need a solid database) 
- **Built for CS2**: Handles competitive mode, Source 2 quirks, and HTTP logging natively
- **Native support of Server Log**: HLstatsX reads and parses raw server logs directly—no dependencies, no guesswork. And since HLTV depends on those logs, unless any others ranking system, HLstatsX will always work!

---

## 📦 Installation & Setup
**Windows Perl:**
   * https://strawberryperl.com/release-notes/5.36.1.1-64bit.html

**Additional modules:**
   * Mojolicious
   * MaxMind::DB::Reader
   * GeoIP2::Database::Reader

  **Example installation**
   * Linux: $ curl -L https://cpanmin.us | perl - -M https://cpan.metacpan.org -n Mojolicious
   * Windows cpan> install Mojolicious

**Sourcegold (source 1)**
- server.cfg
```
rcon_password "PaSsWoRd"
log on
logaddress_delall
logaddress_add "64.74.97.164:27500"
```
**Source 2 (CS2)**
- gamemode-server.cfg
```
rcon_password "PaSsWoRd"
log on
logaddress_delall_http
logaddress_add_http "http://64.74.97.164:27500"
```

`Optionally, an additional log address can be used for testing on a different Daemon/Database`
```
logaddress_add_http_delayed 0.0 "http://64.74.97.164:27501"
```
Add to your launch commands -usercon

### 📈 Web Frontend / Webside Options
- Any of your liking. Pretty much all the same anyway
- Most are available on Github

---

## 🤝 Credits
Based on HLstatsX:CE 1.6.19
Maintained and modernized by the community for CS2 and beyond.

