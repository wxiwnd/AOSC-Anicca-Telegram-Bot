# Deployment

Copy `wrangler.toml.default` to `wrangler.toml` on a fresh checkout. Fill in the existing Worker name, account ID, compatibility date, and KV namespace ID for `STATE`. Keep the existing Worker Secrets. Set `FETCH_URL` under `[vars]` if the Worker uses a custom update feed.

## Routine releases

Once the KV migration is complete and Cron is back on, deploy with:

```sh
pnpm exec wrangler deploy
```

`keep_vars = true` preserves dashboard variables. To check the local configuration and bundle, run `WRANGLER_SEND_METRICS=false pnpm exec wrangler deploy --dry-run`.

## First KV migration

1. Record the active version ID and Cron schedule. Set `crons = []` and run `pnpm exec wrangler triggers deploy`. Wait for old invocations to finish before backing up. Cron changes can take up to 15 minutes to propagate. Pause manual triggers and subscription changes during the backup. [Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/)
2. Set `CLOUDFLARE_API_TOKEN` with **Workers KV Storage Read** access to the account. Substitute the IDs from `wrangler.toml`. Omit `--execute` for an offline preview.

   ```sh
   npm run backup:kv -- --account-id ACCOUNT_ID --namespace-id KV_NAMESPACE_ID --execute
   ```

   In the new directory under `.backups/`, check that `manifest.json` says `complete: true` and has one entry per `.bin` file. Keep the backup private. It contains the full KV namespace, which may include credentials.
3. With Cron still off, run `pnpm exec wrangler deploy`. Run `/trigger` once as the admin, then check `/debug` and `/listsub`. Verify that pending private messages go out and previously announced versions stay quiet.
4. Restore the previous Cron schedule, run `pnpm exec wrangler triggers deploy`, and watch two runs.

## Rollback

Pause Cron and other KV writes, then back up the current namespace. Copy `signatures` and `queue` from `notification_state` to `last_sigs` and `subs_queue` as JSON arrays. Copy `lastHash` and `lastTs` to `last_hash` and `last_ts` as strings. Delete the corresponding legacy key if its value is `null`.

After confirming the old version is active and the legacy keys are up to date, remove the backed-up `notification_state` key and restore Cron. The next upgrade will then read the latest legacy state. Cloudflare rolls back the Worker version only. [Rollbacks](https://developers.cloudflare.com/workers/versions-and-deployments/rollbacks/)
