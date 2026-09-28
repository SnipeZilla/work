# 📈 HLstatsZ Web — Next-gen stats frontend for GoldSrc, Source and CS2

Player and clan rankings, awards, live servers and bans, read from the database of the [HLstatsZ daemon](https://github.com/SnipeZilla/HLSTATS-2).

Modern yet familiar: a clean, responsive, AJAX-first layout that keeps the original HLstats feel.

<!-- Screenshot: the home page -->

---

## Features

**Statistics**
- **True Rank** system
- Rankings: global, yesterday, weekends, last 7 days and last *MinActivity* days
- Players, clans, weapons, maps, actions, awards, ranks and ribbons
- Player profiles: Steam profile, charts, hitboxes (superlogs), forum signature (phpBB, XenForo, Invision, Discord)
- Charts with Chart.js or pChart, player map with OpenStreetMap

**Bans**
- SourceBans / SourceBans++ (Source, CS2) and AMXBans (GoldSrc), alone or merged
- Bans, comm blocks and servers with their live status
- Signed-in players see their own status, with *Report a player* and *Appeal a ban* links to your forum or Discord
- Admins kick, ban and unban from the servers list, over RCON
- No SourceBans or AMXBans website needed: bans, admins and servers are managed from HLstatsZ, which also creates their databases

**Community**
- Secure Steam sign-in
- Discord, TeamSpeak 3 and Mumble servers, Steam Community group

**Look**
- 10 themes: Default (Steam blue), Dark, Light, Dark OLED, Matrix, Dust2 and Strike (CS2), Fortress (TF2), and the seasonal Halloween and Noel
- A simplified theme for the in-game MOTD
- Multi languages, included: English (US), French, German, Spanish (Mexico), Portuguese (Brazil), Russian, Albanian
- Plain CSS, JS and PHP, easy to customize

**Admin panel**
- Installer: the database, its tables and your first admin, in one step
- Admins sign in with Steam: no password to share
- Database updater, daemon control, RCON console (also for servers HLstatsZ doesn't track)
- Game, server, award, rank and ban settings
- Database tools: optimize, reset, fix collations, repair double-encoded text

---

## Requirements

- The [HLstatsZ daemon](https://github.com/SnipeZilla/HLSTATS-2): the frontend shows what it records
- PHP 8.3+ with the extensions mysqli, gd (with FreeType), curl, mbstring, intl, sockets, bcmath and xml
- MySQL 8.0+ or MariaDB 10.2+
- A web server: IIS, Apache or nginx

---

## Installation & Setup

### Files
Copy the files to your web server. PHP must be able to write to `hlstatsimg/progress` and `cache/`.

### Configuration
Edit `config.php`: every setting is explained there. The ones to set first:
```php
define('DB_ADDR', 'localhost');
define('DB_USER', 'root');
define('DB_PASS', 'yourpassword');
define('DB_NAME', 'hlstats');
define('SECRET_KEY', '');   // 64 random letters and digits
```
Generate your own secret key with `php -r "echo bin2hex(random_bytes(32));"`.

For Steam sign-in, add a [Steam Web API key](https://steamcommunity.com/dev/apikey) (`STEAM_API`) and the SteamID64 of your admins (`STEAM_ADMIN`).

### Database
Using the daemon's database? There is nothing to install: HLstatsZ opens it as it is.

For a new one, open the site: the installer takes over. It creates the database when it doesn't exist (if `DB_USER` may), then its tables and your first admin, and updates it to the latest version, in one step. To show the site is yours, it asks for the database password of `config.php`.

> [!NOTE]
> From the command line instead: `mysql -u root -p hlstats < sql/install.sql`, then *Admin › Tools › Updater*. The login is then **admin / 123456**: change it in *Admin Users* right away.

### First visit
Open `hlstats.php?mode=admin`. The first page checks PHP, its extensions and the folders, and offers to update the database to the latest version.

> [!TIP]
> Once `STEAM_API` and `STEAM_ADMIN` are set, the password login is off and admins sign in with Steam.

### Bans (optional)
Set `DB_SBNAME` (SourceBans / SourceBans++) and/or `DB_AMXNAME` (AMXBans) in `config.php`, with their table prefix (`DB_SBPREFIX`, `DB_AMXPREFIX`). Their databases must be on the same MySQL server; HLstatsZ's own database will do. Then open *Admin › Bans › Bans Settings*: when a database or its tables don't exist yet, it creates them with that prefix, and makes you the SourceBans owner.

### Web server
`web.config` (IIS) and `.htaccess` (Apache, with `AllowOverride All`) keep the private files out of reach. On nginx, add this before your PHP `location`:
```nginx
location ~ /(cache|sql|updater)/ { deny all; }
location ~ /(config\.php|_error\.txt)$ { deny all; }
```

---

## Upgrading from HLstatsX:CE

HLstatsZ uses the same database.

1. Back up the database.
2. Point `config.php` at it, open the admin panel and click **Update**.
3. Collation errors? *Admin › Tools › Reset DB Collations*.
4. Garbled accents in names (`Ã©` instead of `é`)? *Admin › Tools › Repair Double-Encoded Text*.

---

## Screenshots

<!-- New: the bans dashboard with the "Your status" card -->
<!-- New: the admin RCON console -->

### Weapons
<img width="720" height="701" alt="Weapons page" src="https://github.com/user-attachments/assets/6ca66c72-ba13-4173-8e52-c67262e23fb8" />

### Players
<img width="1135" height="1010" alt="Players ranking" src="https://github.com/user-attachments/assets/31692605-3a53-4260-9038-f9a756c0a0c6" />

### Game
<img width="1214" height="333" alt="Game overview" src="https://github.com/user-attachments/assets/8cb79d51-5a1e-4b43-ad0f-882dc4ccacb2" />

### Awards
<img width="1162" height="949" alt="Awards page" src="https://github.com/user-attachments/assets/c8f8fba4-1f9c-43fa-a30d-41864a91eedd" />

### A theme for everyone
<img width="1166" height="455" alt="The themes" src="https://github.com/user-attachments/assets/510d22e4-5c29-4685-96f0-12e0298fc8a3" />

### Player information
<!-- Update: the new profile header -->
<img width="1165" height="1103" alt="Player profile" src="https://github.com/user-attachments/assets/c060b256-2c79-474e-a2fc-5fb17ddbaf8e" />

### Hitboxes (superlogs)
<img width="1143" height="455" alt="Hitbox statistics" src="https://github.com/user-attachments/assets/177f14bb-4b82-4d46-8f43-0995a6ad9514" />

### Admin — overview
<img width="1172" height="637" alt="Admin panel overview" src="https://github.com/user-attachments/assets/63e3821c-daf9-4d34-bed7-01efad5acb86" />

### Admin — server management
<img width="1356" height="1307" alt="Admin server management" src="https://github.com/user-attachments/assets/526de317-37ce-4a4d-bded-30b17558597d" />

### Discord
<img width="1609" height="332" alt="Discord server widget" src="https://github.com/user-attachments/assets/2447aed3-cd20-43ad-a0d5-9e0bb44d5ddd" />

### Responsive and AJAX
<img width="400" height="832" alt="Phone view" src="https://github.com/user-attachments/assets/a13e6684-e69b-48a4-93c2-11f71ba83627" />

### Translations
<img width="953" height="408" alt="Language picker" src="https://github.com/user-attachments/assets/2df853e9-e509-4d12-8654-15b350f752cd" />

---

## FAQ

**Where do PHP errors go?**  
With `DEBUG` set to `true` in `config.php`, into `_error.txt` next to it.

**How do I turn on a seasonal theme (Halloween, Winter)?**  
Move its folder from `styles/themes/disabled/` up to `styles/themes/`, e.g. `styles/themes/disabled/halloween` to `styles/themes/halloween`. To show it to every visitor, make it the default theme in *HLstats Settings*. Move it back when the season is over.

**Do I need the SourceBans or AMXBans website?**  
No. HLstatsZ shows and manages bans, admins and servers itself. You need their database, which HLstatsZ creates (*Admin › Bans › Bans Settings*), and their plugin on your game servers.

---

## Lineage

```
HLstats (Simon Garner)
  └─ HLstatsX (Tobias Oetzel)
       └─ ELstatsNEO (Malte Bayer)
            └─ HLstatsX:CE (Nicholas Hastings)
                 └─ HLstatsZ (SnipeZilla) ← now you are stuck here
```

## Credits

Based on HLstatsX:CE 1.6.19  
Maintained and modernized by [SnipeZilla](https://github.com/SnipeZilla)  
Validation and help by [ghost-](https://github.com/ghostt187)  
Built with Chart.js, pChart, Leaflet and OpenStreetMap. This product includes GeoLite2 data created by MaxMind, available from https://www.maxmind.com.

Support: [snipezilla.com](https://snipezilla.com) · [AlliedModders forum](https://forums.alliedmods.net/forumdisplay.php?f=156)

Licensed under the GNU General Public License v2 or later.
