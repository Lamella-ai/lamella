# Lamella inventory (2026-04-26)

**Repo state:** `585b8e0`

**Totals:** 8 CLI, 401 routes (242 mutating, 14 AI, 10 Paperless writeback), 6 job kinds, 10 scheduled tasks, 57 env vars, 54 migrations (latest: 54).


## 1. CLI commands

| Command | Target | Purpose | Used by |
|---|---|---|---|
| beancounter-glue | `lamella.main:run` | Legacy alias (cutover window) | docker entrypoint / shell |
| lamella | `lamella.main:run` | Console-script entry point | docker entrypoint / shell |
| lamella.transform.bcg_to_lamella | `lamella.transform.bcg_to_lamella` | Documented `python -m` invocation | docs / human only |
| lamella.transform.reconstruct | `lamella.transform.reconstruct` | Documented `python -m` invocation | docs / human only |
| lamella.transform.migrate_to_ledger | `lamella.transform.migrate_to_ledger` | Documented `python -m` invocation | docs / human only |
| lamella.transform.verify | `lamella.transform.verify` | Documented `python -m` invocation | docs / human only |
| lamella.transform.key_rename | `lamella.transform.key_rename` | Documented `python -m` invocation | docs / human only |
| lamella.transform.backfill_hash | `lamella.transform.backfill_hash` | Documented `python -m` invocation | docs / human only |


## 2. HTTP routes

_401 routes; 242 mutating._


| Method | Path | Handler | Mutates | HTMX | AI | Paperless WB |
|---|---|---|---|---|---|---|
| GET | `/` | `src/lamella/routes/dashboard.py:141` | no | yes | no | no |
| GET | `/accounts` | `src/lamella/routes/accounts_browse.py:165` | no | yes | no | no |
| GET | `/accounts/{account_path:path}` | `src/lamella/routes/accounts_browse.py:354` | no | yes | no | no |
| GET | `/accounts/{account_path:path}/balance-chart.json` | `src/lamella/routes/accounts_browse.py:332` | no | no | no | no |
| GET | `/accounts/{account_path:path}/edit` | `src/lamella/routes/accounts_browse.py:220` | no | yes | no | no |
| POST | `/accounts/{account_path:path}/edit` | `src/lamella/routes/accounts_browse.py:247` | yes | no | no | no |
| POST | `/accounts/{account_path:path}/ensure-companions` | `src/lamella/routes/accounts_browse.py:660` | yes | no | no | no |
| POST | `/accounts/{account_path:path}/opening-balance` | `src/lamella/routes/accounts_browse.py:530` | yes | no | no | no |
| GET | `/ai/audit` | `src/lamella/routes/ai.py:29` | no | yes | yes | no |
| GET | `/ai/cost` | `src/lamella/routes/ai.py:73` | no | yes | no | no |
| GET | `/ai/decisions/{decision_id}` | `src/lamella/routes/ai.py:483` | no | yes | no | no |
| POST | `/ai/retry/{decision_id}` | `src/lamella/routes/ai.py:585` | yes | no | yes | yes |
| GET | `/ai/suggestions` | `src/lamella/routes/ai.py:89` | no | yes | no | no |
| POST | `/ai/suggestions/{decision_id}/reject` | `src/lamella/routes/ai.py:339` | yes | yes | no | no |
| GET | `/api/accounts` | `src/lamella/routes/accounts.py:34` | no | no | no | no |
| GET | `/api/accounts-meta-suggestions` | `src/lamella/routes/accounts.py:98` | no | no | no | no |
| POST | `/api/txn/bulk/ask-ai` | `src/lamella/routes/api_txn.py:602` | yes | no | yes | no |
| POST | `/api/txn/{ref}/ask-ai` | `src/lamella/routes/api_txn.py:422` | yes | no | yes | no |
| POST | `/api/txn/{ref}/classify` | `src/lamella/routes/api_txn.py:134` | yes | no | no | no |
| POST | `/api/txn/{ref}/dismiss` | `src/lamella/routes/api_txn.py:355` | yes | no | no | no |
| GET | `/assets` | `src/lamella/routes/businesses.py:348` | no | yes | no | no |
| GET | `/audit` | `src/lamella/routes/audit.py:55` | no | yes | no | no |
| POST | `/audit/items/{item_id}/accept` | `src/lamella/routes/audit.py:212` | yes | yes | no | no |
| POST | `/audit/items/{item_id}/dismiss` | `src/lamella/routes/audit.py:337` | yes | yes | no | no |
| POST | `/audit/run` | `src/lamella/routes/audit.py:118` | yes | yes | yes | no |
| GET | `/budgets` | `src/lamella/routes/budgets.py:66` | no | yes | no | no |
| POST | `/budgets` | `src/lamella/routes/budgets.py:75` | yes | no | no | no |
| POST | `/budgets/{budget_id}` | `src/lamella/routes/budgets.py:120` | yes | no | no | no |
| POST | `/budgets/{budget_id}/delete` | `src/lamella/routes/budgets.py:149` | yes | no | no | no |
| GET | `/businesses` | `src/lamella/routes/businesses.py:81` | no | yes | no | no |
| GET | `/businesses/{slug}` | `src/lamella/routes/businesses.py:160` | no | yes | no | no |
| GET | `/businesses/{slug}/accounts/edit` | `src/lamella/routes/accounts_browse.py:694` | no | yes | no | no |
| POST | `/businesses/{slug}/accounts/edit` | `src/lamella/routes/accounts_browse.py:725` | yes | no | no | no |
| GET | `/businesses/{slug}/chart/expense-trend.json` | `src/lamella/routes/businesses.py:337` | no | no | no | no |
| GET | `/businesses/{slug}/chart/pnl-monthly.json` | `src/lamella/routes/businesses.py:326` | no | no | no | no |
| GET | `/businesses/{slug}/period` | `src/lamella/routes/businesses.py:305` | no | yes | no | no |
| GET | `/calendar` | `src/lamella/routes/calendar.py:127` | no | yes | no | no |
| POST | `/calendar/{date_str}/ai-audit` | `src/lamella/routes/calendar.py:510` | yes | no | no | no |
| POST | `/calendar/{date_str}/ai-summary` | `src/lamella/routes/calendar.py:398` | yes | no | no | no |
| GET | `/calendar/{date_str}/next` | `src/lamella/routes/calendar.py:701` | no | no | no | no |
| POST | `/calendar/{date_str}/note` | `src/lamella/routes/calendar.py:310` | yes | no | no | no |
| POST | `/calendar/{date_str}/note/{note_id}/delete` | `src/lamella/routes/calendar.py:343` | yes | no | no | no |
| POST | `/calendar/{date_str}/review` | `src/lamella/routes/calendar.py:253` | yes | no | no | no |
| GET | `/calendar/{token}` | `src/lamella/routes/calendar.py:139` | no | no | no | no |
| GET | `/card` | `src/lamella/routes/card.py:484` | no | yes | no | no |
| GET | `/card/categories-for/{slug}` | `src/lamella/routes/card.py:604` | no | no | no | no |
| GET | `/card/recent` | `src/lamella/routes/card.py:1029` | no | no | no | no |
| POST | `/card/undo/{action_id}` | `src/lamella/routes/card.py:939` | yes | no | no | no |
| POST | `/card/{item_id}/save` | `src/lamella/routes/card.py:638` | yes | no | no | no |
| POST | `/card/{item_id}/skip` | `src/lamella/routes/card.py:911` | yes | no | no | no |
| POST | `/dashboard/welcome/dismiss` | `src/lamella/routes/dashboard.py:256` | yes | yes | no | no |
| GET | `/healthz` | `src/lamella/routes/health.py:18` | no | no | no | no |
| GET | `/import` | `src/lamella/routes/import_.py:96` | no | yes | no | no |
| POST | `/import` | `src/lamella/routes/import_.py:109` | yes | no | no | no |
| DELETE | `/import/{import_id}` | `src/lamella/routes/import_.py:506` | yes | no | no | no |
| GET | `/import/{import_id}` | `src/lamella/routes/import_.py:145` | no | yes | no | no |
| GET | `/import/{import_id}.json` | `src/lamella/routes/import_.py:164` | no | no | no | no |
| POST | `/import/{import_id}/cancel` | `src/lamella/routes/import_.py:497` | yes | no | no | no |
| GET | `/import/{import_id}/classify` | `src/lamella/routes/import_.py:190` | no | yes | no | no |
| POST | `/import/{import_id}/classify` | `src/lamella/routes/import_.py:211` | yes | no | no | no |
| POST | `/import/{import_id}/commit` | `src/lamella/routes/import_.py:483` | yes | no | no | no |
| GET | `/import/{import_id}/ingest` | `src/lamella/routes/import_.py:348` | no | yes | no | no |
| POST | `/import/{import_id}/ingest` | `src/lamella/routes/import_.py:366` | yes | yes | no | no |
| GET | `/import/{import_id}/map` | `src/lamella/routes/import_.py:239` | no | yes | no | no |
| POST | `/import/{import_id}/map` | `src/lamella/routes/import_.py:306` | yes | no | no | no |
| GET | `/import/{import_id}/preview` | `src/lamella/routes/import_.py:421` | no | yes | no | no |
| POST | `/import/{import_id}/preview/recategorize` | `src/lamella/routes/import_.py:460` | yes | no | no | no |
| GET | `/inbox` | `src/lamella/routes/inbox.py:43` | no | yes | no | no |
| GET | `/intake` | `src/lamella/routes/intake.py:61` | no | yes | no | no |
| POST | `/intake/preview` | `src/lamella/routes/intake.py:79` | yes | yes | no | no |
| POST | `/intake/stage` | `src/lamella/routes/intake.py:122` | yes | yes | no | no |
| GET | `/jobs/active/dock` | `src/lamella/routes/jobs.py:102` | no | yes | no | no |
| GET | `/jobs/{job_id}` | `src/lamella/routes/jobs.py:70` | no | yes | no | no |
| POST | `/jobs/{job_id}/cancel` | `src/lamella/routes/jobs.py:85` | yes | yes | no | no |
| GET | `/jobs/{job_id}/partial` | `src/lamella/routes/jobs.py:46` | no | yes | no | no |
| GET | `/jobs/{job_id}/stream` | `src/lamella/routes/jobs.py:113` | no | no | no | no |
| GET | `/mileage` | `src/lamella/routes/mileage.py:127` | no | yes | no | no |
| POST | `/mileage` | `src/lamella/routes/mileage.py:616` | yes | no | no | no |
| GET | `/mileage/all` | `src/lamella/routes/mileage.py:150` | no | yes | no | no |
| GET | `/mileage/import` | `src/lamella/routes/mileage.py:930` | no | yes | no | no |
| POST | `/mileage/import/batches/{batch_id}/delete` | `src/lamella/routes/mileage.py:1130` | yes | no | no | no |
| POST | `/mileage/import/commit` | `src/lamella/routes/mileage.py:1055` | yes | yes | no | no |
| POST | `/mileage/import/preview` | `src/lamella/routes/mileage.py:960` | yes | yes | no | no |
| GET | `/mileage/last-odometer/{vehicle:path}` | `src/lamella/routes/mileage.py:578` | no | yes | no | no |
| GET | `/mileage/quick` | `src/lamella/routes/mileage.py:427` | no | yes | no | no |
| POST | `/mileage/quick` | `src/lamella/routes/mileage.py:449` | yes | no | no | no |
| GET | `/mileage/summary` | `src/lamella/routes/mileage.py:786` | no | yes | no | no |
| POST | `/mileage/summary/generate` | `src/lamella/routes/mileage.py:804` | yes | yes | no | no |
| POST | `/mileage/{entry_id:int}` | `src/lamella/routes/mileage.py:238` | yes | no | no | no |
| POST | `/mileage/{entry_id:int}/delete` | `src/lamella/routes/mileage.py:352` | yes | no | no | no |
| GET | `/mileage/{entry_id:int}/edit` | `src/lamella/routes/mileage.py:214` | no | yes | no | no |
| GET | `/note` | `src/lamella/routes/note.py:27` | no | yes | no | no |
| POST | `/note` | `src/lamella/routes/note.py:33` | yes | no | no | no |
| GET | `/notifications` | `src/lamella/routes/notifications.py:104` | no | yes | no | no |
| POST | `/notifications/test` | `src/lamella/routes/notifications.py:116` | yes | no | no | no |
| POST | `/notifications/{row_id}/resend` | `src/lamella/routes/notifications.py:152` | yes | no | no | no |
| GET | `/paperless/preview/{doc_id}` | `src/lamella/routes/paperless_proxy.py:101` | no | no | no | no |
| GET | `/paperless/thumb/{doc_id}` | `src/lamella/routes/paperless_proxy.py:75` | no | no | no | no |
| GET | `/paperless/writebacks` | `src/lamella/routes/paperless_writebacks.py:100` | no | yes | no | yes |
| POST | `/paperless/{doc_id}/enrich` | `src/lamella/routes/paperless_verify.py:286` | yes | yes | no | yes |
| POST | `/paperless/{doc_id}/verify` | `src/lamella/routes/paperless_verify.py:101` | yes | yes | yes | yes |
| POST | `/paperless/{doc_id}/verify/sync` | `src/lamella/routes/paperless_verify.py:251` | yes | yes | no | yes |
| GET | `/projects` | `src/lamella/routes/projects.py:111` | no | yes | no | no |
| POST | `/projects` | `src/lamella/routes/projects.py:140` | yes | no | no | no |
| GET | `/projects/{slug}` | `src/lamella/routes/projects.py:184` | no | yes | no | no |
| POST | `/projects/{slug}` | `src/lamella/routes/projects.py:234` | yes | no | no | no |
| POST | `/projects/{slug}/close` | `src/lamella/routes/projects.py:280` | yes | no | no | no |
| POST | `/projects/{slug}/delete` | `src/lamella/routes/projects.py:308` | yes | no | no | no |
| GET | `/readyz` | `src/lamella/routes/health.py:23` | no | no | no | no |
| GET | `/receipts` | `src/lamella/routes/receipts.py:89` | no | yes | no | no |
| GET | `/receipts/needed` | `src/lamella/routes/receipts_needed.py:192` | no | yes | no | no |
| GET | `/receipts/needed/partial` | `src/lamella/routes/receipts_needed.py:233` | no | yes | no | no |
| POST | `/receipts/needed/{txn_hash}/dismiss` | `src/lamella/routes/receipts_needed.py:479` | yes | no | no | no |
| POST | `/receipts/needed/{txn_hash}/link` | `src/lamella/routes/receipts_needed.py:281` | yes | no | no | no |
| POST | `/receipts/needed/{txn_hash}/undismiss` | `src/lamella/routes/receipts_needed.py:526` | yes | no | no | no |
| POST | `/receipts/{doc_id}/link` | `src/lamella/routes/receipts.py:196` | yes | no | no | no |
| GET | `/recurring` | `src/lamella/routes/recurring.py:72` | no | yes | no | no |
| POST | `/recurring/scan` | `src/lamella/routes/recurring.py:80` | yes | yes | no | no |
| POST | `/recurring/{recurring_id}/confirm` | `src/lamella/routes/recurring.py:129` | yes | no | no | no |
| POST | `/recurring/{recurring_id}/edit` | `src/lamella/routes/recurring.py:269` | yes | no | no | no |
| POST | `/recurring/{recurring_id}/ignore` | `src/lamella/routes/recurring.py:237` | yes | no | no | no |
| POST | `/recurring/{recurring_id}/stop` | `src/lamella/routes/recurring.py:215` | yes | no | no | no |
| GET | `/reports` | `src/lamella/routes/reports.py:92` | no | yes | no | no |
| GET | `/reports/audit-portfolio.pdf` | `src/lamella/routes/reports.py:296` | no | no | no | no |
| GET | `/reports/balance-audit` | `src/lamella/routes/balances.py:166` | no | yes | no | no |
| GET | `/reports/estimated-tax.csv` | `src/lamella/routes/reports.py:355` | no | no | no | no |
| GET | `/reports/estimated-tax.pdf` | `src/lamella/routes/reports.py:335` | no | no | no | no |
| GET | `/reports/intercompany` | `src/lamella/routes/intercompany.py:36` | no | yes | no | no |
| GET | `/reports/schedule-c-detail.csv` | `src/lamella/routes/reports.py:152` | no | no | no | no |
| GET | `/reports/schedule-c.csv` | `src/lamella/routes/reports.py:132` | no | no | no | no |
| GET | `/reports/schedule-c.pdf` | `src/lamella/routes/reports.py:172` | no | no | no | no |
| GET | `/reports/schedule-c.preview.html` | `src/lamella/routes/reports.py:197` | no | yes | no | no |
| GET | `/reports/schedule-f-detail.csv` | `src/lamella/routes/reports.py:237` | no | no | no | no |
| GET | `/reports/schedule-f.csv` | `src/lamella/routes/reports.py:217` | no | no | no | no |
| GET | `/reports/schedule-f.pdf` | `src/lamella/routes/reports.py:257` | no | no | no | no |
| GET | `/reports/schedule-f.preview.html` | `src/lamella/routes/reports.py:279` | no | yes | no | no |
| GET | `/reports/vehicles/form-4562-worksheet.pdf` | `src/lamella/routes/reports.py:561` | no | no | no | no |
| GET | `/reports/vehicles/mileage-log.pdf` | `src/lamella/routes/reports.py:481` | no | no | no | no |
| GET | `/reports/vehicles/schedule-c-part-iv.pdf` | `src/lamella/routes/reports.py:524` | no | no | no | no |
| GET | `/review` | `src/lamella/routes/review.py:252` | no | yes | no | no |
| POST | `/review/rescan` | `src/lamella/routes/review.py:384` | yes | no | no | no |
| GET | `/review/staged` | `src/lamella/routes/staging_review.py:372` | no | yes | no | no |
| POST | `/review/staged/ask-ai-modal` | `src/lamella/routes/staging_review.py:1018` | yes | yes | yes | no |
| POST | `/review/staged/classify` | `src/lamella/routes/staging_review.py:429` | yes | yes | no | no |
| POST | `/review/staged/classify-group` | `src/lamella/routes/staging_review.py:776` | yes | yes | no | no |
| POST | `/review/staged/dismiss` | `src/lamella/routes/staging_review.py:382` | yes | yes | no | no |
| POST | `/review/{item_id}/mark_transfer` | `src/lamella/routes/review.py:532` | yes | no | no | no |
| POST | `/review/{item_id}/mark_transfer_to` | `src/lamella/routes/review.py:650` | yes | no | no | no |
| POST | `/review/{item_id}/resolve` | `src/lamella/routes/review.py:395` | yes | no | yes | no |
| GET | `/rules` | `src/lamella/routes/rules.py:34` | no | yes | no | no |
| POST | `/rules` | `src/lamella/routes/rules.py:119` | yes | yes | no | no |
| POST | `/rules/promote-mined` | `src/lamella/routes/rules.py:70` | yes | yes | no | no |
| DELETE | `/rules/{rule_id}` | `src/lamella/routes/rules.py:225` | yes | no | no | no |
| POST | `/rules/{rule_id}/delete` | `src/lamella/routes/rules.py:241` | yes | no | no | no |
| GET | `/search` | `src/lamella/routes/search.py:154` | no | yes | no | no |
| POST | `/search/bulk-apply` | `src/lamella/routes/search.py:1764` | yes | yes | no | no |
| POST | `/search/mark-transfer-pair` | `src/lamella/routes/search.py:1396` | yes | yes | no | no |
| GET | `/search/palette.json` | `src/lamella/routes/search.py:2032` | no | no | no | no |
| POST | `/search/receipt-hunt` | `src/lamella/routes/search.py:1645` | yes | yes | no | no |
| GET | `/search/receipt-hunt/result` | `src/lamella/routes/search.py:1711` | no | yes | no | no |
| GET | `/settings` | `src/lamella/routes/settings.py:119` | no | yes | no | no |
| POST | `/settings` | `src/lamella/routes/settings.py:129` | yes | yes | no | no |
| GET | `/settings/account-descriptions` | `src/lamella/routes/account_descriptions.py:81` | no | yes | no | no |
| POST | `/settings/account-descriptions/generate` | `src/lamella/routes/account_descriptions.py:138` | yes | yes | yes | no |
| POST | `/settings/account-descriptions/mine` | `src/lamella/routes/account_descriptions.py:227` | yes | yes | yes | no |
| POST | `/settings/account-descriptions/save` | `src/lamella/routes/account_descriptions.py:348` | yes | yes | no | no |
| GET | `/settings/accounts` | `src/lamella/routes/accounts_admin.py:66` | no | yes | no | no |
| POST | `/settings/accounts-bulk-save` | `src/lamella/routes/accounts_admin.py:221` | yes | no | no | no |
| POST | `/settings/accounts-cleanup-system` | `src/lamella/routes/accounts_admin.py:271` | yes | no | no | no |
| POST | `/settings/accounts-new` | `src/lamella/routes/accounts_admin.py:416` | yes | no | no | no |
| POST | `/settings/accounts/add-subcategory` | `src/lamella/routes/accounts_admin.py:157` | yes | no | no | no |
| POST | `/settings/accounts/{account_path:path}` | `src/lamella/routes/accounts_admin.py:333` | yes | no | no | no |
| GET | `/settings/accounts/{account_path:path}/balances` | `src/lamella/routes/balances.py:42` | no | yes | no | no |
| POST | `/settings/accounts/{account_path:path}/balances` | `src/lamella/routes/balances.py:75` | yes | no | no | no |
| POST | `/settings/accounts/{account_path:path}/balances/{anchor_id:int}/delete` | `src/lamella/routes/balances.py:137` | yes | no | no | no |
| GET | `/settings/backups` | `src/lamella/routes/backups.py:36` | no | yes | no | no |
| POST | `/settings/backups/create` | `src/lamella/routes/backups.py:53` | yes | no | no | no |
| POST | `/settings/backups/delete` | `src/lamella/routes/backups.py:84` | yes | no | no | no |
| GET | `/settings/backups/download/{filename}` | `src/lamella/routes/backups.py:68` | no | no | no | no |
| POST | `/settings/backups/restore` | `src/lamella/routes/backups.py:97` | yes | no | no | no |
| GET | `/settings/data-integrity` | `src/lamella/routes/data_integrity.py:92` | no | yes | no | no |
| POST | `/settings/data-integrity/acknowledge-preflight` | `src/lamella/routes/data_integrity.py:128` | yes | yes | no | no |
| POST | `/settings/data-integrity/apply-reboot` | `src/lamella/routes/data_integrity.py:374` | yes | yes | no | no |
| POST | `/settings/data-integrity/auto-match-receipts` | `src/lamella/routes/data_integrity.py:805` | yes | yes | no | no |
| POST | `/settings/data-integrity/check` | `src/lamella/routes/data_integrity.py:301` | yes | yes | no | no |
| POST | `/settings/data-integrity/classify-fixmes` | `src/lamella/routes/data_integrity.py:728` | yes | yes | yes | no |
| GET | `/settings/data-integrity/duplicates` | `src/lamella/routes/data_integrity.py:520` | no | yes | no | no |
| POST | `/settings/data-integrity/duplicates/remove` | `src/lamella/routes/data_integrity.py:545` | yes | yes | no | no |
| GET | `/settings/data-integrity/legacy-fees-paths` | `src/lamella/routes/data_integrity.py:664` | no | yes | no | no |
| POST | `/settings/data-integrity/mine-rules` | `src/lamella/routes/data_integrity.py:455` | yes | yes | no | no |
| POST | `/settings/data-integrity/prepare-reboot` | `src/lamella/routes/data_integrity.py:343` | yes | yes | no | no |
| POST | `/settings/data-integrity/retrofit` | `src/lamella/routes/data_integrity.py:237` | yes | yes | no | no |
| POST | `/settings/data-integrity/rollback-reboot` | `src/lamella/routes/data_integrity.py:983` | yes | yes | no | no |
| POST | `/settings/data-integrity/scan` | `src/lamella/routes/data_integrity.py:163` | yes | yes | no | no |
| GET | `/settings/data-integrity/stacked-overrides` | `src/lamella/routes/data_integrity.py:857` | no | yes | no | no |
| POST | `/settings/data-integrity/stacked-overrides/cleanup` | `src/lamella/routes/data_integrity.py:908` | yes | yes | no | no |
| GET | `/settings/entities` | `src/lamella/routes/entities.py:70` | no | yes | no | no |
| POST | `/settings/entities` | `src/lamella/routes/entities.py:336` | yes | no | no | no |
| POST | `/settings/entities-cleanup` | `src/lamella/routes/entities.py:182` | yes | no | no | no |
| GET | `/settings/entities/suggest-slug` | `src/lamella/routes/entities.py:101` | no | yes | no | no |
| POST | `/settings/entities/{slug}/delete` | `src/lamella/routes/entities.py:131` | yes | no | no | no |
| POST | `/settings/entities/{slug}/generate-context` | `src/lamella/routes/entities.py:249` | yes | yes | yes | no |
| GET | `/settings/entities/{slug}/merge` | `src/lamella/routes/entities.py:499` | no | yes | no | no |
| POST | `/settings/entities/{slug}/merge` | `src/lamella/routes/entities.py:523` | yes | no | no | no |
| GET | `/settings/entities/{slug}/scaffold` | `src/lamella/routes/entities.py:430` | no | yes | no | no |
| POST | `/settings/entities/{slug}/scaffold` | `src/lamella/routes/entities.py:555` | yes | no | no | no |
| GET | `/settings/loans` | `src/lamella/routes/loans.py:145` | no | yes | no | no |
| POST | `/settings/loans` | `src/lamella/routes/loans.py:201` | yes | no | no | no |
| GET | `/settings/loans/wizard/{flow_name}` | `src/lamella/routes/loans_wizard.py:103` | no | yes | no | no |
| POST | `/settings/loans/wizard/{flow_name}/commit` | `src/lamella/routes/loans_wizard.py:254` | yes | no | no | no |
| POST | `/settings/loans/wizard/{flow_name}/preview` | `src/lamella/routes/loans_wizard.py:194` | yes | yes | no | no |
| POST | `/settings/loans/wizard/{flow_name}/step` | `src/lamella/routes/loans_wizard.py:129` | yes | yes | no | no |
| GET | `/settings/loans/{slug}` | `src/lamella/routes/loans.py:1846` | no | yes | no | no |
| POST | `/settings/loans/{slug}/anchors` | `src/lamella/routes/loans.py:1675` | yes | no | no | no |
| POST | `/settings/loans/{slug}/anchors/{anchor_id}/delete` | `src/lamella/routes/loans.py:1722` | yes | no | no | no |
| POST | `/settings/loans/{slug}/autofix` | `src/lamella/routes/loans.py:667` | yes | no | no | no |
| GET | `/settings/loans/{slug}/backfill` | `src/lamella/routes/loans_backfill.py:89` | no | yes | no | no |
| POST | `/settings/loans/{slug}/backfill/preview` | `src/lamella/routes/loans_backfill.py:111` | yes | yes | no | no |
| POST | `/settings/loans/{slug}/backfill/run` | `src/lamella/routes/loans_backfill.py:151` | yes | yes | no | no |
| GET | `/settings/loans/{slug}/backfill/sample.csv` | `src/lamella/routes/loans_backfill.py:228` | no | no | no | no |
| POST | `/settings/loans/{slug}/categorize-draw` | `src/lamella/routes/loans.py:720` | yes | no | no | no |
| GET | `/settings/loans/{slug}/edit` | `src/lamella/routes/loans.py:485` | no | yes | no | no |
| POST | `/settings/loans/{slug}/escrow/reconcile` | `src/lamella/routes/loans.py:836` | yes | no | no | no |
| POST | `/settings/loans/{slug}/fund-initial` | `src/lamella/routes/loans.py:518` | yes | no | no | no |
| POST | `/settings/loans/{slug}/groups/{group_id}/confirm` | `src/lamella/routes/loans.py:950` | yes | no | no | no |
| POST | `/settings/loans/{slug}/open-accounts` | `src/lamella/routes/loans.py:608` | yes | no | no | no |
| POST | `/settings/loans/{slug}/pauses` | `src/lamella/routes/loans.py:1738` | yes | no | no | no |
| POST | `/settings/loans/{slug}/pauses/{pause_id}/delete` | `src/lamella/routes/loans.py:1825` | yes | no | no | no |
| POST | `/settings/loans/{slug}/pauses/{pause_id}/end` | `src/lamella/routes/loans.py:1795` | yes | no | no | no |
| GET | `/settings/loans/{slug}/projection.json` | `src/lamella/routes/loans.py:1181` | no | no | no | no |
| POST | `/settings/loans/{slug}/record-missing-payment` | `src/lamella/routes/loans.py:1275` | yes | no | no | no |
| POST | `/settings/loans/{slug}/record-payment` | `src/lamella/routes/loans.py:1410` | yes | no | no | no |
| GET | `/settings/mileage-rates` | `src/lamella/routes/mileage.py:521` | no | yes | no | no |
| POST | `/settings/mileage-rates` | `src/lamella/routes/mileage.py:540` | yes | no | no | no |
| POST | `/settings/mileage-rates/{rate_id}/delete` | `src/lamella/routes/mileage.py:566` | yes | no | no | no |
| GET | `/settings/paperless-fields` | `src/lamella/routes/paperless_fields.py:97` | no | yes | no | yes |
| POST | `/settings/paperless-fields` | `src/lamella/routes/paperless_fields.py:415` | yes | no | no | yes |
| POST | `/settings/paperless-fields/classify` | `src/lamella/routes/paperless_fields.py:371` | yes | no | no | yes |
| POST | `/settings/paperless-fields/create` | `src/lamella/routes/paperless_fields.py:185` | yes | yes | no | yes |
| POST | `/settings/paperless-fields/refresh` | `src/lamella/routes/paperless_fields.py:123` | yes | yes | no | yes |
| POST | `/settings/payout-sources/dismiss` | `src/lamella/routes/payout_sources.py:236` | yes | no | no | no |
| POST | `/settings/payout-sources/scaffold` | `src/lamella/routes/payout_sources.py:68` | yes | no | no | no |
| GET | `/settings/properties` | `src/lamella/routes/properties.py:100` | no | yes | no | no |
| POST | `/settings/properties` | `src/lamella/routes/properties.py:122` | yes | no | no | no |
| GET | `/settings/properties/{slug}` | `src/lamella/routes/properties.py:307` | no | yes | no | no |
| GET | `/settings/properties/{slug}/change-ownership` | `src/lamella/routes/properties.py:1084` | no | yes | no | no |
| POST | `/settings/properties/{slug}/change-ownership/rename` | `src/lamella/routes/properties.py:815` | yes | no | no | no |
| POST | `/settings/properties/{slug}/change-ownership/transfer` | `src/lamella/routes/properties.py:1124` | yes | no | no | no |
| GET | `/settings/properties/{slug}/dispose` | `src/lamella/routes/properties.py:570` | no | yes | no | no |
| POST | `/settings/properties/{slug}/dispose` | `src/lamella/routes/properties.py:625` | yes | no | no | no |
| POST | `/settings/properties/{slug}/valuations` | `src/lamella/routes/properties.py:479` | yes | no | no | no |
| POST | `/settings/properties/{slug}/valuations/{valuation_id}/delete` | `src/lamella/routes/properties.py:524` | yes | no | no | no |
| GET | `/settings/rewrite` | `src/lamella/routes/rewrite.py:33` | no | yes | no | no |
| POST | `/settings/rewrite` | `src/lamella/routes/rewrite.py:60` | yes | yes | no | no |
| GET | `/settings/vehicles` | `src/lamella/routes/vehicles.py:3013` | no | no | no | no |
| POST | `/settings/vehicles` | `src/lamella/routes/vehicles.py:3018` | yes | no | no | no |
| GET | `/settings/vehicles/{slug}` | `src/lamella/routes/vehicles.py:3023` | no | no | no | no |
| POST | `/settings/vehicles/{slug}/mileage` | `src/lamella/routes/vehicles.py:3028` | yes | no | no | no |
| POST | `/settings/vehicles/{slug}/valuations` | `src/lamella/routes/vehicles.py:3033` | yes | no | no | no |
| POST | `/settings/vehicles/{slug}/valuations/{valuation_id}/delete` | `src/lamella/routes/vehicles.py:3040` | yes | no | no | no |
| GET | `/setup` | `src/lamella/routes/setup.py:5408` | no | yes | no | no |
| GET | `/setup/accounts` | `src/lamella/routes/setup.py:1855` | no | yes | no | no |
| POST | `/setup/accounts/add` | `src/lamella/routes/setup.py:2112` | yes | yes | no | no |
| POST | `/setup/accounts/close` | `src/lamella/routes/setup.py:2395` | yes | yes | no | no |
| POST | `/setup/accounts/save` | `src/lamella/routes/setup.py:2297` | yes | yes | no | no |
| GET | `/setup/charts` | `src/lamella/routes/setup.py:2534` | no | yes | no | no |
| POST | `/setup/charts/{slug}/scaffold` | `src/lamella/routes/setup.py:2597` | yes | no | no | no |
| GET | `/setup/check` | `src/lamella/routes/setup_check.py:605` | no | yes | no | no |
| GET | `/setup/entities` | `src/lamella/routes/setup.py:86` | no | yes | no | no |
| POST | `/setup/entities/add-business` | `src/lamella/routes/setup.py:379` | yes | yes | no | no |
| POST | `/setup/entities/add-person` | `src/lamella/routes/setup.py:306` | yes | yes | no | no |
| POST | `/setup/entities/{slug}/cleanup-stale-meta` | `src/lamella/routes/setup.py:1243` | yes | no | no | no |
| POST | `/setup/entities/{slug}/close-unused-opens` | `src/lamella/routes/setup.py:1274` | yes | no | no | no |
| POST | `/setup/entities/{slug}/deactivate` | `src/lamella/routes/setup.py:672` | yes | yes | no | no |
| POST | `/setup/entities/{slug}/delete` | `src/lamella/routes/setup.py:768` | yes | yes | no | no |
| GET | `/setup/entities/{slug}/manage` | `src/lamella/routes/setup.py:1061` | no | yes | no | no |
| POST | `/setup/entities/{slug}/migrate-account` | `src/lamella/routes/setup.py:1423` | yes | no | no | no |
| POST | `/setup/entities/{slug}/reactivate` | `src/lamella/routes/setup.py:721` | yes | yes | no | no |
| POST | `/setup/entities/{slug}/save` | `src/lamella/routes/setup.py:468` | yes | yes | no | no |
| POST | `/setup/entities/{slug}/skip` | `src/lamella/routes/setup.py:618` | yes | yes | no | no |
| POST | `/setup/fix-duplicate-closes` | `src/lamella/routes/setup.py:1778` | yes | no | no | no |
| POST | `/setup/fix-orphan-overrides` | `src/lamella/routes/setup.py:1655` | yes | no | no | no |
| GET | `/setup/import` | `src/lamella/routes/setup.py:5690` | no | yes | no | no |
| GET | `/setup/import-rewrite` | `src/lamella/routes/setup.py:4040` | no | yes | no | no |
| POST | `/setup/import/apply` | `src/lamella/routes/setup.py:5726` | yes | no | no | no |
| GET | `/setup/legacy-paths` | `src/lamella/routes/setup_legacy_paths.py:91` | no | yes | no | no |
| POST | `/setup/legacy-paths/heal` | `src/lamella/routes/setup_legacy_paths.py:120` | yes | no | no | no |
| GET | `/setup/loans` | `src/lamella/routes/setup.py:3094` | no | yes | no | no |
| POST | `/setup/loans/add` | `src/lamella/routes/setup.py:3322` | yes | yes | no | no |
| POST | `/setup/loans/{slug}/edit` | `src/lamella/routes/setup.py:3508` | yes | yes | no | no |
| GET | `/setup/progress` | `src/lamella/routes/setup.py:5392` | no | no | no | no |
| GET | `/setup/properties` | `src/lamella/routes/setup.py:2698` | no | yes | no | no |
| POST | `/setup/properties/add` | `src/lamella/routes/setup.py:2858` | yes | yes | no | no |
| POST | `/setup/properties/{slug}/scaffold` | `src/lamella/routes/setup.py:3016` | yes | no | no | no |
| GET | `/setup/reconstruct` | `src/lamella/routes/setup.py:6134` | no | yes | no | no |
| POST | `/setup/reconstruct` | `src/lamella/routes/setup.py:6163` | yes | yes | no | no |
| GET | `/setup/recovery` | `src/lamella/routes/setup_recovery.py:208` | no | yes | no | no |
| POST | `/setup/recovery/apply` | `src/lamella/routes/setup_recovery.py:659` | yes | no | no | no |
| POST | `/setup/recovery/draft/{finding_id}/dismiss` | `src/lamella/routes/setup_recovery.py:336` | yes | yes | no | no |
| POST | `/setup/recovery/draft/{finding_id}/edit` | `src/lamella/routes/setup_recovery.py:399` | yes | yes | no | no |
| GET | `/setup/recovery/finalizing` | `src/lamella/routes/setup_recovery.py:724` | no | yes | no | no |
| GET | `/setup/recovery/schema` | `src/lamella/routes/setup_schema.py:98` | no | yes | no | no |
| GET | `/setup/recovery/schema/confirm` | `src/lamella/routes/setup_schema.py:125` | no | yes | no | no |
| POST | `/setup/recovery/schema/heal` | `src/lamella/routes/setup_schema.py:186` | yes | no | no | no |
| POST | `/setup/refresh-progress` | `src/lamella/routes/setup.py:5370` | yes | no | no | no |
| POST | `/setup/scaffold` | `src/lamella/routes/setup.py:5605` | yes | no | no | no |
| GET | `/setup/simplefin` | `src/lamella/routes/setup.py:3730` | no | yes | no | no |
| POST | `/setup/simplefin/bind` | `src/lamella/routes/setup.py:3939` | yes | yes | no | no |
| POST | `/setup/simplefin/connect` | `src/lamella/routes/setup.py:3761` | yes | yes | no | no |
| POST | `/setup/simplefin/disconnect` | `src/lamella/routes/setup.py:3908` | yes | yes | no | no |
| POST | `/setup/simplefin/skip` | `src/lamella/routes/setup.py:3881` | yes | yes | no | no |
| POST | `/setup/stamp-version` | `src/lamella/routes/setup.py:1585` | yes | no | no | no |
| GET | `/setup/vector-progress-partial` | `src/lamella/routes/setup.py:5345` | no | yes | no | no |
| GET | `/setup/vehicles` | `src/lamella/routes/setup.py:4145` | no | yes | no | no |
| POST | `/setup/vehicles/add` | `src/lamella/routes/setup.py:4406` | yes | yes | no | no |
| POST | `/setup/vehicles/close-unused-orphans` | `src/lamella/routes/setup.py:4572` | yes | no | no | no |
| GET | `/setup/vehicles/{slug}/migrate` | `src/lamella/routes/setup.py:4704` | no | yes | no | no |
| POST | `/setup/vehicles/{slug}/migrate` | `src/lamella/routes/setup.py:4806` | yes | no | no | no |
| POST | `/setup/vehicles/{slug}/scaffold` | `src/lamella/routes/setup.py:5276` | yes | no | no | no |
| GET | `/setup/welcome` | `src/lamella/routes/setup.py:6347` | no | yes | no | no |
| POST | `/setup/welcome/continue` | `src/lamella/routes/setup.py:6379` | yes | no | no | no |
| GET | `/setup/wizard` | `src/lamella/routes/setup_wizard.py:439` | no | yes | no | no |
| GET | `/setup/wizard/accounts` | `src/lamella/routes/setup_wizard.py:1824` | no | yes | no | no |
| POST | `/setup/wizard/accounts` | `src/lamella/routes/setup_wizard.py:2077` | yes | yes | no | no |
| POST | `/setup/wizard/accounts/remove` | `src/lamella/routes/setup_wizard.py:2033` | yes | no | no | no |
| POST | `/setup/wizard/accounts/save` | `src/lamella/routes/setup_wizard.py:1911` | yes | no | no | no |
| GET | `/setup/wizard/bank` | `src/lamella/routes/setup_wizard.py:1217` | no | yes | no | no |
| POST | `/setup/wizard/bank/connect` | `src/lamella/routes/setup_wizard.py:1319` | yes | no | no | no |
| POST | `/setup/wizard/bank/connected` | `src/lamella/routes/setup_wizard.py:1262` | yes | no | no | no |
| POST | `/setup/wizard/bank/skip` | `src/lamella/routes/setup_wizard.py:1247` | yes | no | no | no |
| GET | `/setup/wizard/done` | `src/lamella/routes/setup_wizard.py:3434` | no | yes | no | no |
| POST | `/setup/wizard/done` | `src/lamella/routes/setup_wizard.py:3467` | yes | yes | no | no |
| GET | `/setup/wizard/entities` | `src/lamella/routes/setup_wizard.py:847` | no | yes | no | no |
| POST | `/setup/wizard/entities` | `src/lamella/routes/setup_wizard.py:1106` | yes | yes | no | no |
| POST | `/setup/wizard/entities/remove` | `src/lamella/routes/setup_wizard.py:1083` | yes | no | no | no |
| POST | `/setup/wizard/entities/save-business` | `src/lamella/routes/setup_wizard.py:1009` | yes | no | no | no |
| POST | `/setup/wizard/entities/save-person` | `src/lamella/routes/setup_wizard.py:940` | yes | no | no | no |
| GET | `/setup/wizard/finalizing` | `src/lamella/routes/setup_wizard.py:3549` | no | yes | no | no |
| GET | `/setup/wizard/property-vehicle` | `src/lamella/routes/setup_wizard.py:2818` | no | yes | no | no |
| POST | `/setup/wizard/property-vehicle/continue` | `src/lamella/routes/setup_wizard.py:3238` | yes | no | no | no |
| POST | `/setup/wizard/property-vehicle/remove` | `src/lamella/routes/setup_wizard.py:3024` | yes | no | no | no |
| POST | `/setup/wizard/property-vehicle/save-property` | `src/lamella/routes/setup_wizard.py:2873` | yes | no | no | no |
| POST | `/setup/wizard/property-vehicle/save-vehicle` | `src/lamella/routes/setup_wizard.py:2942` | yes | no | no | no |
| POST | `/setup/wizard/reset` | `src/lamella/routes/setup_wizard.py:3583` | yes | no | no | no |
| GET | `/setup/wizard/welcome` | `src/lamella/routes/setup_wizard.py:525` | no | yes | no | no |
| POST | `/setup/wizard/welcome` | `src/lamella/routes/setup_wizard.py:544` | yes | yes | no | no |
| GET | `/simplefin` | `src/lamella/routes/simplefin.py:374` | no | yes | no | no |
| POST | `/simplefin/account-map` | `src/lamella/routes/simplefin.py:451` | yes | yes | no | no |
| POST | `/simplefin/discover` | `src/lamella/routes/simplefin.py:718` | yes | yes | no | no |
| POST | `/simplefin/fetch` | `src/lamella/routes/simplefin.py:539` | yes | yes | no | no |
| POST | `/simplefin/map` | `src/lamella/routes/simplefin.py:755` | yes | yes | no | no |
| POST | `/simplefin/mode` | `src/lamella/routes/simplefin.py:383` | yes | yes | no | no |
| POST | `/simplefin/settings` | `src/lamella/routes/simplefin.py:408` | yes | yes | no | no |
| GET | `/status` | `src/lamella/routes/status.py:622` | no | yes | no | no |
| POST | `/status/paperless/full-sync` | `src/lamella/routes/status.py:671` | yes | yes | no | no |
| POST | `/status/vector-index/clear-stuck` | `src/lamella/routes/status.py:650` | yes | yes | no | no |
| POST | `/status/vector-index/rebuild` | `src/lamella/routes/status.py:718` | yes | yes | yes | no |
| GET | `/teach` | `src/lamella/routes/teach.py:27` | no | yes | no | no |
| POST | `/teach` | `src/lamella/routes/teach.py:38` | yes | no | no | no |
| GET | `/transactions` | `src/lamella/routes/transactions.py:93` | no | yes | no | no |
| GET | `/txn/{target_hash}` | `src/lamella/routes/search.py:423` | no | yes | no | no |
| POST | `/txn/{target_hash}/apply` | `src/lamella/routes/search.py:1173` | yes | yes | no | no |
| POST | `/txn/{target_hash}/ask-ai` | `src/lamella/routes/search.py:712` | yes | yes | yes | no |
| POST | `/txn/{target_hash}/categorize-inplace` | `src/lamella/routes/search.py:973` | yes | yes | no | no |
| GET | `/txn/{target_hash}/notes-partial` | `src/lamella/routes/search.py:835` | no | yes | no | no |
| GET | `/txn/{target_hash}/pair-candidates` | `src/lamella/routes/search.py:1315` | no | no | no | no |
| GET | `/txn/{target_hash}/panel` | `src/lamella/routes/search.py:866` | no | yes | no | no |
| POST | `/txn/{target_hash}/revert-override` | `src/lamella/routes/search.py:1125` | yes | yes | no | no |
| GET | `/vehicle-templates` | `src/lamella/routes/vehicles.py:2790` | no | yes | no | no |
| POST | `/vehicle-templates` | `src/lamella/routes/vehicles.py:2807` | yes | no | no | no |
| POST | `/vehicle-templates/{slug}/delete` | `src/lamella/routes/vehicles.py:2864` | yes | no | no | no |
| GET | `/vehicles` | `src/lamella/routes/vehicles.py:555` | no | yes | no | no |
| POST | `/vehicles` | `src/lamella/routes/vehicles.py:1790` | yes | no | no | no |
| GET | `/vehicles/backfill-audit` | `src/lamella/routes/vehicles.py:846` | no | yes | no | no |
| GET | `/vehicles/new` | `src/lamella/routes/vehicles.py:667` | no | yes | no | no |
| GET | `/vehicles/{slug}` | `src/lamella/routes/vehicles.py:683` | no | yes | no | no |
| POST | `/vehicles/{slug}/attribution` | `src/lamella/routes/vehicles.py:2872` | yes | no | no | no |
| POST | `/vehicles/{slug}/banner/{change_key}/dismiss` | `src/lamella/routes/vehicles.py:1024` | yes | no | no | no |
| GET | `/vehicles/{slug}/change-ownership` | `src/lamella/routes/vehicles.py:1047` | no | yes | no | no |
| POST | `/vehicles/{slug}/change-ownership/rename` | `src/lamella/routes/vehicles.py:1103` | yes | no | no | no |
| POST | `/vehicles/{slug}/change-ownership/transfer` | `src/lamella/routes/vehicles.py:1436` | yes | no | no | no |
| POST | `/vehicles/{slug}/credits` | `src/lamella/routes/vehicles.py:2687` | yes | no | no | no |
| POST | `/vehicles/{slug}/credits/{credit_id:int}/delete` | `src/lamella/routes/vehicles.py:2720` | yes | no | no | no |
| GET | `/vehicles/{slug}/dispose` | `src/lamella/routes/vehicles.py:2171` | no | yes | no | no |
| POST | `/vehicles/{slug}/dispose` | `src/lamella/routes/vehicles.py:2267` | yes | yes | no | no |
| POST | `/vehicles/{slug}/dispose/commit` | `src/lamella/routes/vehicles.py:2307` | yes | no | no | no |
| POST | `/vehicles/{slug}/dispose/{disposal_id}/revoke` | `src/lamella/routes/vehicles.py:2412` | yes | no | no | no |
| GET | `/vehicles/{slug}/edit` | `src/lamella/routes/vehicles.py:1766` | no | yes | no | no |
| POST | `/vehicles/{slug}/elections` | `src/lamella/routes/vehicles.py:2024` | yes | no | no | no |
| POST | `/vehicles/{slug}/elections/{tax_year}/delete` | `src/lamella/routes/vehicles.py:2111` | yes | no | no | no |
| POST | `/vehicles/{slug}/fuel` | `src/lamella/routes/vehicles.py:2608` | yes | no | no | no |
| POST | `/vehicles/{slug}/fuel/{event_id:int}/delete` | `src/lamella/routes/vehicles.py:2672` | yes | no | no | no |
| POST | `/vehicles/{slug}/mileage` | `src/lamella/routes/vehicles.py:1948` | yes | no | no | no |
| POST | `/vehicles/{slug}/promote-trips` | `src/lamella/routes/vehicles.py:2528` | yes | no | no | no |
| POST | `/vehicles/{slug}/renewals` | `src/lamella/routes/vehicles.py:2730` | yes | no | no | no |
| POST | `/vehicles/{slug}/renewals/{renewal_id:int}/complete` | `src/lamella/routes/vehicles.py:2763` | yes | no | no | no |
| POST | `/vehicles/{slug}/renewals/{renewal_id:int}/delete` | `src/lamella/routes/vehicles.py:2774` | yes | no | no | no |
| GET | `/vehicles/{slug}/trips` | `src/lamella/routes/vehicles.py:894` | no | yes | no | no |
| POST | `/vehicles/{slug}/valuations` | `src/lamella/routes/vehicles.py:2937` | yes | no | no | no |
| POST | `/vehicles/{slug}/valuations/{valuation_id}/delete` | `src/lamella/routes/vehicles.py:2993` | yes | no | no | no |
| POST | `/webhooks/paperless/new` | `src/lamella/routes/webhooks.py:58` | yes | no | no | no |


## 3. Background jobs

| Kind | Submitters | Title hint | Submitted at |
|---|---|---|---|
| audit-run | 1 | n/a | `src/lamella/routes/audit.py:196` |
| calendar-ai-audit | 1 | n/a | `src/lamella/routes/calendar.py:686` |
| calendar-ai-summary | 1 | n/a | `src/lamella/routes/calendar.py:494` |
| loan-backfill | 1 | n/a | `src/lamella/routes/loans_backfill.py:209` |
| reconstruct | 1 | Rebuilding SQLite state from the ledger | `src/lamella/routes/setup.py:6295` |
| vehicle-migrate | 1 | n/a | `src/lamella/routes/setup.py:4916` |


## 4. Scheduled tasks (APScheduler)

| Function | Trigger | Declared at |
|---|---|---|
| `_run_sqlite_backup` | `CronTrigger(hour=2, minute=0)` | `src/lamella/main.py:1076` |
| `_run_recurring_detection` | `CronTrigger(day_of_week='sun', hour=3, minute=0)` | `src/lamella/main.py:1083` |
| `_run_budget_evaluation` | `IntervalTrigger(hours=6)` | `src/lamella/main.py:1090` |
| `_run_business_cache_warmup` | `IntervalTrigger(minutes=10, jitter=60)` | `src/lamella/main.py:1098` |
| `_run_weekly_digest` | `CronTrigger(hour=9, minute=0)` | `src/lamella/main.py:1108` |
| `_run_receipt_auto_sweep` | `IntervalTrigger(hours=4, jitter=300)` | `src/lamella/main.py:1115` |
| `_run_context_ready_classify` | `CronTrigger(hour='4,16', minute=0, jitter=300)` | `src/lamella/main.py:1124` |
| `_paperless_sync_job` | `IntervalTrigger(hours=max(1, settings.paperless_sync_interval_hours))` | `src/lamella/main.py:1137` |
| `callback` | `IntervalTrigger(hours=max(1, interval_hours), jitter=300)` | `src/lamella/simplefin/schedule.py:41` |
| `callback` | `?` | `src/lamella/simplefin/schedule.py:54` |


## 5. Config files at runtime

| Path | Format | Read by | Mutated by app? |
|---|---|---|---|
| pyproject.toml | TOML | package metadata, console scripts | no |
| .env / environment | env | Settings via pydantic_settings | no |
| <ledger_dir>/main.bean | Beancount | ledger entry point | yes (in-place rewrites + connector-owned files) |
| <ledger_dir>/connector_*.bean | Beancount | connector-owned state writers | yes |
| <ledger_dir>/simplefin_transactions.bean | Beancount | SimpleFIN classified writes | yes |
| <data_dir>/lamella.sqlite | SQLite | cache + workflow state | yes |
| <data_dir>/lamella.sqlite.backups/ | SQLite | nightly dumps | yes |
| migrations/*.sql | SQL | schema migrations applied at startup | no |


## 6. Environment variables

_57 declared (Settings + raw os.environ.get reads)._


| Var | Type | Default | Secret? | Where |
|---|---|---|---|---|
| LAMELLA_ACCOUNT_DEFAULT_OPEN_DATE | str | '1900-01-01' | no | `src/lamella/config.py:101` |
| LAMELLA_AI_CACHE_TTL_HOURS | int | 24 | no | `src/lamella/config.py:49` |
| LAMELLA_AI_FALLBACK_CONFIDENCE_THRESHOLD | float | 0.6 | no | `src/lamella/config.py:71` |
| LAMELLA_AI_FALLBACK_ENABLED | bool | True | no | `src/lamella/config.py:72` |
| LAMELLA_AI_MAX_MONTHLY_SPEND_USD | float | 0.0 | no | `src/lamella/config.py:50` |
| LAMELLA_AI_VECTOR_CORRECTION_WEIGHT | float | 2.0 | no | `src/lamella/config.py:60` |
| LAMELLA_AI_VECTOR_MODEL_NAME | str | 'sentence-transformers/all-MiniLM-L6-v2' | no | `src/lamella/config.py:57` |
| LAMELLA_AI_VECTOR_SEARCH_ENABLED | bool | True | no | `src/lamella/config.py:56` |
| LAMELLA_APP_TZ | str | 'UTC' | no | `src/lamella/config.py:42` |
| LAMELLA_AUDIT_MAX_RECEIPT_BYTES | int | 10000000 | no | `src/lamella/config.py:116` |
| LAMELLA_BUDGET_ALERT_CHANNELS | str | '' | no | `src/lamella/config.py:117` |
| LAMELLA_DATA_DIR | Path | Path('/data') | no | `src/lamella/config.py:25` |
| LAMELLA_ESTIMATED_TAX_FLAT_RATE | float | 0.25 | no | `src/lamella/config.py:120` |
| LAMELLA_IMPORT_AI_COLUMN_MAP_MODEL | str \| None | None | no | `src/lamella/config.py:137` |
| LAMELLA_IMPORT_AI_CONFIDENCE_THRESHOLD | float | 0.7 | no | `src/lamella/config.py:138` |
| LAMELLA_IMPORT_LEDGER_OUTPUT_DIR | Path \| None | None | no | `src/lamella/config.py:134` |
| LAMELLA_IMPORT_MAX_UPLOAD_BYTES | int | 50000000 | no | `src/lamella/config.py:136` |
| LAMELLA_IMPORT_RETENTION_DAYS | int | 90 | no | `src/lamella/config.py:135` |
| LAMELLA_IMPORT_UPLOAD_DIR | Path \| None | None | no | `src/lamella/config.py:133` |
| LAMELLA_LEDGER_DIR | Path | Path('/ledger') | no | `src/lamella/config.py:26` |
| LAMELLA_LOG_LEVEL | str | 'INFO' | no | `src/lamella/config.py:38` |
| LAMELLA_MILEAGE_CSV_PATH | Path \| None | None | no | `src/lamella/config.py:110` |
| LAMELLA_MILEAGE_RATE | float | 0.67 | no | `src/lamella/config.py:109` |
| LAMELLA_NOTIFY_DIGEST_DAY | str | 'Monday' | no | `src/lamella/config.py:111` |
| LAMELLA_NOTIFY_MIN_FIXME_USD | float | 500.0 | no | `src/lamella/config.py:112` |
| LAMELLA_NTFY_BASE_URL | str | 'https://ntfy.sh' | no | `src/lamella/config.py:104` |
| LAMELLA_NTFY_TOKEN | SecretStr \| None | None | yes | `src/lamella/config.py:106` |
| LAMELLA_NTFY_TOPIC | str \| None | None | no | `src/lamella/config.py:105` |
| LAMELLA_OPENROUTER_API_KEY | SecretStr \| None | None | yes | `src/lamella/config.py:45` |
| LAMELLA_OPENROUTER_APP_TITLE | str \| None | 'lamella' | no | `src/lamella/config.py:48` |
| LAMELLA_OPENROUTER_APP_URL | str \| None | None | no | `src/lamella/config.py:47` |
| LAMELLA_OPENROUTER_MODEL | str | 'anthropic/claude-haiku-4.5' | no | `src/lamella/config.py:46` |
| LAMELLA_OPENROUTER_MODEL_FALLBACK | str \| None | 'anthropic/claude-opus-4.7' | no | `src/lamella/config.py:68` |
| LAMELLA_OPENROUTER_MODEL_RECEIPT_VERIFY | str | 'anthropic/claude-opus-4.7' | no | `src/lamella/config.py:80` |
| LAMELLA_PAPERLESS_API_TOKEN | SecretStr \| None | None | yes | `src/lamella/config.py:29` |
| LAMELLA_PAPERLESS_CF_ACCESS_CLIENT_ID | SecretStr \| None | None | yes | `src/lamella/config.py:34` |
| LAMELLA_PAPERLESS_CF_ACCESS_CLIENT_SECRET | SecretStr \| None | None | yes | `src/lamella/config.py:35` |
| LAMELLA_PAPERLESS_SYNC_INTERVAL_HOURS | int | 6 | no | `src/lamella/config.py:124` |
| LAMELLA_PAPERLESS_SYNC_LOOKBACK_DAYS | int | 3650 | no | `src/lamella/config.py:130` |
| LAMELLA_PAPERLESS_URL | str \| None | None | no | `src/lamella/config.py:28` |
| LAMELLA_PAPERLESS_WRITEBACK_ENABLED | bool | False | no | `src/lamella/config.py:79` |
| LAMELLA_PORT | int | 8080 | no | `src/lamella/config.py:37` |
| LAMELLA_PUSHOVER_API_TOKEN | SecretStr \| None | None | yes | `src/lamella/config.py:108` |
| LAMELLA_PUSHOVER_USER_KEY | SecretStr \| None | None | yes | `src/lamella/config.py:107` |
| LAMELLA_RECEIPT_REQUIRED_THRESHOLD_USD | float | 75.0 | no | `src/lamella/config.py:123` |
| LAMELLA_RECURRING_MIN_OCCURRENCES | int | 3 | no | `src/lamella/config.py:119` |
| LAMELLA_RECURRING_SCAN_WINDOW_DAYS | int | 540 | no | `src/lamella/config.py:118` |
| LAMELLA_REPORTS_OUTPUT_DIR | Path \| None | None | no | `src/lamella/config.py:115` |
| LAMELLA_SIMPLEFIN_ACCESS_URL | SecretStr \| None | None | yes | `src/lamella/config.py:85` |
| LAMELLA_SIMPLEFIN_ACCOUNT_MAP_PATH | Path \| None | None | no | `src/lamella/config.py:89` |
| LAMELLA_SIMPLEFIN_FETCH_INTERVAL_HOURS | int | 6 | no | `src/lamella/config.py:87` |
| LAMELLA_SIMPLEFIN_LOOKBACK_DAYS | int | 14 | no | `src/lamella/config.py:88` |
| LAMELLA_SIMPLEFIN_MODE | str | 'disabled' | no | `src/lamella/config.py:86` |
| CONNECTOR_CONFIG_DIR | str (raw) | n/a | no | `src/lamella/config.py:240` |
| HOME | str (raw) | n/a | no | `src/lamella/_uid_compat.py:53` |
| HOME | str (raw) | n/a | no | `src/lamella/_uid_compat.py:74` |
| HOME | str (raw) | n/a | no | `src/lamella/_uid_compat.py:44` |


## 7. External services

| Service | Purpose | Auth | Where |
|---|---|---|---|
| Beancount | Ledger parse (in-process, local) | n/a (file) | src/lamella/beancount_io.py |
| Paperless-ngx | Receipt index + writeback | API token (+ Cloudflare Access service token) | src/lamella/paperless/* |
| SimpleFIN Bridge | Bank transaction pull | Access URL (basic auth in URL) | src/lamella/simplefin/* |
| OpenRouter | AI completions (Haiku primary, Opus fallback) | API key (sk-or-v1-...) | src/lamella/ai/client.py |
| HuggingFace Hub | Sentence-transformers model download (one-time) | anonymous | src/lamella/ai/vector_index.py |
| Cloudflare Tunnel + Access | Network edge (terminates TLS, gates traffic) | service-token pair | deployment / docker-compose.yml |


## 8. SQLite migrations

- Latest: **054** (`054_setup_repair_state.sql`)

- Total: **54**

- Applied at startup by `lamella.db.migrate(conn)`.
