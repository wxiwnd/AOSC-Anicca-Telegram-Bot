# AOSC Anicca Telegram Bot

A Cloudflare Worker that polls Anicca for package updates and posts them to Telegram. Users can subscribe to individual packages for private notifications.

In a private chat, use `/listsub` to see your subscriptions or `/checksub` to see their upstream update details.

## Development

```sh
pnpm install --frozen-lockfile
pnpm test
pnpm check
```

Tests mock the update feed, Telegram, and Workers KV.

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for Worker configuration, KV backup, and the one-time migration.

The Worker identifies updates by package, target version, and path. It saves sent updates and pending private messages in `notification_state`. A check with no new updates posts `Nothing New Happened`. With no pending messages, the check leaves notification state untouched. `lastTs` records the last successful state save.

If Telegram accepts a message and the Worker stops before saving its state, the next run can send the message again.
