# Disclaimer

This project is an experimental automation plugin and patch set for personal media library workflows around MoviePilot, 115 Cloud Drive, CloudDrive2, and optional fallback cloud drives.

Use it at your own risk.

## Account and platform risk

- This project is not affiliated with MoviePilot, 115 Cloud Drive, CloudDrive2, Aliyun Drive, 123 Pan, Quark Drive, Baidu Netdisk, Plex, Emby, or Jellyfin.
- Cloud drive APIs, cookies, web sessions, share-transfer behavior, and rate limits may change at any time.
- Automated search, transfer, copy, mount, or scan behavior may trigger account protection, rate limiting, temporary cooldowns, login expiration, or other platform-side restrictions.
- Users are responsible for complying with the terms of service of every platform they connect.

## Data risk

- This project can create folders, save shared files, copy files across mounted cloud drives, delete temporary paths, and trigger media-library organization.
- Test with non-critical directories before using it on an existing production library.
- Back up MoviePilot configuration, plugin configuration, and important media-library metadata before deployment.

## Compatibility risk

- The repository contains plugin code and MoviePilot patch files. It is not a full MoviePilot distribution.
- MoviePilot upstream changes may break these patch files.
- Do not blindly overwrite a newer MoviePilot installation without comparing upstream changes first.

## Secrets

Never commit runtime secrets such as cookies, tokens, refresh tokens, authorization headers, NAS addresses, SSH credentials, reverse-proxy domains, logs, deployment archives, or scanned login sessions.
