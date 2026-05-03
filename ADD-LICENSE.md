# ADD-LICENSE.md

Got it. Here's a complete implementation package you can drop into your repo or hand to your coding agent. I'll give you the file contents, the file header pattern, and clear instructions.

## Decisions baked in

Just so we're aligned on what these files reflect:

- **License**: Apache 2.0
- **Trademark**: Common-law (™), not registered yet
- **Owner**: I'll use `Lamella` as a placeholder. Replace before committing
- **Year**: 2026

## Agent instructions (paste this into a task)

```markdown
# Task: Set up Lamella's open-source licensing structure

Apply the Apache 2.0 license to the Lamella repository, add trademark
protection notices, and add SPDX copyright headers to all source files.

## Files to create at repo root

1. LICENSE - the full Apache License 2.0 text (verbatim from
   https://www.apache.org/licenses/LICENSE-2.0.txt)
2. NOTICE - attribution file (content provided below)
3. TRADEMARK.md - trademark policy (content provided below)
4. CONTRIBUTING.md - contribution guidelines including CLA reference
   (content provided below)
5. README.md - if it exists, append the License & Trademark section
   provided below; if not, create a minimal one with that section

## Files to add SPDX headers to

Add SPDX-formatted copyright headers to ALL source code files in the repo:
- Python (.py)
- JavaScript (.js, .mjs, .cjs)
- TypeScript (.ts, .tsx)
- PHP (.php)
- SQL (.sql) - only files we author, not migrations from frameworks
- Shell scripts (.sh)
- Dockerfile, docker-compose.yml
- HTML/CSS files we author (.html, .css) - skip vendored assets
- Config files we author (.yml, .yaml, .toml) - skip lock files

DO NOT add headers to:
- Files in node_modules/, vendor/, venv/, .venv/, dist/, build/
- Auto-generated files (migrations, lock files, compiled output)
- Third-party files we didn't author (check git log if unsure)
- JSON files (JSON has no comment syntax)
- Markdown files (use the README/NOTICE pattern instead)
- .env files or other config that shouldn't be committed anyway

## Header format by language

Use the appropriate comment syntax for each language. Header content is:

  Copyright 2026 Lamella
  SPDX-License-Identifier: Apache-2.0

  Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
  https://lamella.ai

Replace Lamella and AI-powered bookkeeping software that provides context-aware financial intelligence
before running. Examples for each language are below in this document.

## Process

1. Stop and ask the user to confirm the legal name and short description
   before modifying any files.
2. Create the four root-level files (LICENSE, NOTICE, TRADEMARK.md,
   CONTRIBUTING.md) using the exact content provided below.
3. Update or create README.md with the License & Trademark section.
4. Walk the source tree and add headers to qualifying files. Preserve
   any existing shebang lines (e.g., #!/usr/bin/env python3); the
   header goes AFTER the shebang.
5. If a file already has a copyright header, do NOT duplicate it.
   Update it only if the user explicitly requests.
6. Report back: list of files created, count of files headers added to,
   any files skipped and why.

## Do NOT

- Do not modify the contents of vendored or third-party code
- Do not add headers to test fixtures or sample data files
- Do not change existing copyright notices that name other authors
- Do not commit anything; leave changes staged for user review
```

## File: LICENSE

This should be the full Apache 2.0 license text. Have your agent fetch it verbatim from `https://www.apache.org/licenses/LICENSE-2.0.txt` rather than copy it from anywhere else (it's a long file and copying introduces typo risk). The file is plain text, no modifications.

## File: NOTICE

```
Lamella
Copyright 2026 Lamella

This product includes software developed by Lamella
(https://lamella.ai/).

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this software except in compliance with the License. You may
obtain a copy of the License in the LICENSE file in this repository, or
at: http://www.apache.org/licenses/LICENSE-2.0

----------------------------------------------------------------------

ATTRIBUTION REQUEST

If you use, host, or distribute Lamella as part of a product or service,
the following attribution is requested (but not legally required by the
Apache License) in your application's about page, footer, or
documentation:

    Powered by Lamella (https://lamella.ai)

----------------------------------------------------------------------

TRADEMARK NOTICE

"Lamella" and the Lamella logo are trademarks of
Lamella. The Apache License does not grant
permission to use these trademarks. See TRADEMARK.md for the trademark
usage policy.
```

## File: TRADEMARK.md

```markdown
# Lamella Trademark Policy

"Lamella" and the Lamella logo are trademarks of
Lamella ("we", "us"). This policy explains what you
may and may not do with these marks.

The Lamella software is licensed under the Apache License, Version 2.0
(see LICENSE). The Apache License grants you broad rights to use, copy,
modify, and distribute the software. **It does not, however, grant you
any rights to use the Lamella name or logo.** Trademark rights are
separate from copyright/license rights.

## What you MAY do without permission

- Use the name "Lamella" descriptively to refer to this project
  (e.g., "this is a fork of Lamella", "compatible with Lamella",
  "based on Lamella")
- State accurately that your product uses, integrates with, or is
  derived from Lamella
- Use unmodified Lamella branding when distributing unmodified copies
  of the official Lamella software
- Discuss, review, or write about Lamella

## What you MAY NOT do without our written permission

- Use "Lamella" (or any confusingly similar name) as the name of a
  fork, derivative work, hosted service, or competing product
- Use the Lamella logo for any product, service, or organization other
  than the official Lamella project
- Imply endorsement, affiliation, or sponsorship by Lamella or
  Lamella when none exists
- Register domain names, social media handles, or trademarks containing
  "Lamella" or confusingly similar terms
- Modify the Lamella logo

## Forks and derivative works

If you fork Lamella or create a derivative work, you must:

1. Choose a different name for your project. The name must not be
   confusingly similar to "Lamella".
2. Use a different logo.
3. Make clear that your project is a fork or derivative, not the
   official Lamella project.

You may, of course, state accurately that your project is "based on
Lamella" or "a fork of Lamella". That is descriptive use and is
permitted.

## Hosted services and SaaS

If you host Lamella as a service for others (commercial or otherwise),
you must not market or describe the service using the Lamella name or
logo as if it were the official offering. You may say your service "runs
Lamella" or is "powered by Lamella", but you may not call your service
"Lamella" or "Lamella Cloud" or similar.

We request (but do not legally require under the Apache License) that
hosted deployments include "Powered by Lamella" attribution with a link
to https://lamella.ai in the application's about page or footer.

## Questions and permissions

If you want to do something not covered above, or you're not sure
whether your intended use is allowed, please contact us at
legal@lamella.ai before proceeding. We are generally friendly to
reasonable requests.

## Notes

"Lamella" is currently used as an unregistered (common-law) trademark
in connection with the Lamella software and related services. We
reserve all rights in the mark.

This policy may be updated from time to time. The current version is
always available in the Lamella repository at TRADEMARK.md.
```

## File: CONTRIBUTING.md

```markdown
# Contributing to Lamella

Thank you for your interest in contributing to Lamella! This document
explains how to contribute and what we expect from contributors.

## License of contributions

Lamella is licensed under the Apache License, Version 2.0. By submitting
a contribution (pull request, patch, code, documentation, etc.), you
agree that:

1. Your contribution is licensed under the Apache License, Version 2.0,
   the same license as the rest of the project.
2. You have the right to submit the contribution under that license
   (i.e., you wrote it yourself, or you have permission from the
   copyright holder).
3. You understand that your contribution is public and may be
   redistributed under the project's license.

This is consistent with the Apache 2.0 license's contribution clause
(Section 5), which states that contributions are submitted under the
license terms unless explicitly stated otherwise.

## Developer Certificate of Origin

For now, contributions are accepted under the Apache 2.0 license terms
without a separate Contributor License Agreement (CLA). We may
introduce a CLA in the future if the project's governance needs change
(for example, if we want the option to dual-license or relicense). If
that happens, we will request that contributors sign the CLA going
forward; we will not retroactively require it for past contributions.

If you would prefer to assert authorship explicitly, you may add a
"Signed-off-by" line to your commits per the Developer Certificate of
Origin (https://developercertificate.org/):

    git commit -s -m "Your commit message"

This is appreciated but not required.

## How to contribute

[Add project-specific guidance here: how to set up the dev environment,
coding standards, how to run tests, branch naming, PR conventions, etc.]

## Code of conduct

Be respectful. Assume good faith. Don't be a jerk.

## Trademark

Please note that contributing code does not grant you any rights to the
Lamella trademark. See TRADEMARK.md for our trademark policy.
```

## README.md addition (License & Trademark section)

Append this near the bottom of your README:

```markdown
## License

Lamella is licensed under the Apache License, Version 2.0. See the
[LICENSE](LICENSE) and [NOTICE](NOTICE) files for details.

In short: you can use, modify, and distribute Lamella freely, including
for commercial purposes, as long as you preserve the copyright notice
and the NOTICE file in your distribution.

## Trademark

"Lamella"™ and the Lamella logo are trademarks of
Lamella. The Apache License does not grant rights
to use these trademarks. If you fork Lamella or build a derivative
product, please choose a different name. See [TRADEMARK.md](TRADEMARK.md)
for the full trademark policy.

## Attribution

If you host Lamella as a service or include it in a product, we
appreciate (but don't require) a "Powered by Lamella" credit linking
to https://lamella.ai in your application footer or about page.
```

## SPDX file headers by language

The SPDX-License-Identifier convention is the modern standard. It's machine-readable, recognized by automated license scanners (FOSSA, Snyk, GitHub's license detection), and much shorter than pasting the full Apache notice into every file.

### Python (`.py`)

```python
# Copyright 2026 Lamella
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai
```

For files with a shebang, header goes *after* the shebang:

```python
#!/usr/bin/env python3
# Copyright 2026 Lamella
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai
```

### JavaScript / TypeScript (`.js`, `.mjs`, `.cjs`, `.ts`, `.tsx`)

```javascript
// Copyright 2026 Lamella
// SPDX-License-Identifier: Apache-2.0
//
// Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
// https://lamella.ai
```

Or block comment style (also fine):

```javascript
/*
 * Copyright 2026 Lamella
 * SPDX-License-Identifier: Apache-2.0
 *
 * Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
 * https://lamella.ai
 */
```

Pick one style and use it consistently across all JS/TS files.

### PHP (`.php`)

```php
<?php
// Copyright 2026 Lamella
// SPDX-License-Identifier: Apache-2.0
//
// Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
// https://lamella.ai
```

### SQL (`.sql`)

```sql
-- Copyright 2026 Lamella
-- SPDX-License-Identifier: Apache-2.0
--
-- Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
-- https://lamella.ai
```

### Shell scripts (`.sh`, `.bash`)

```bash
#!/usr/bin/env bash
# Copyright 2026 Lamella
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai
```

### Dockerfile, YAML, TOML (`.yml`, `.yaml`, `.toml`, `Dockerfile`)

```dockerfile
# Copyright 2026 Lamella
# SPDX-License-Identifier: Apache-2.0
#
# Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
# https://lamella.ai
```

### HTML (`.html`)

```html
<!--
  Copyright 2026 Lamella
  SPDX-License-Identifier: Apache-2.0

  Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
  https://lamella.ai
-->
```

### CSS (`.css`)

```css
/*
 * Copyright 2026 Lamella
 * SPDX-License-Identifier: Apache-2.0
 *
 * Lamella - AI-powered bookkeeping software that provides context-aware financial intelligence
 * https://lamella.ai
 */
```

## A few practical notes

**Decided values (baked into this doc):**
- Legal entity: `Lamella` (placeholder until a formal entity is registered; personal name can transfer rights later)
- Project description: `AI-powered bookkeeping software that provides context-aware financial intelligence`
- Trademark contact: `legal@lamella.ai` (general project contact: `hello@lamella.ai`)

**On the year:** Apache convention is single-year for the original copyright (`2026`), updated to a range when substantial changes are made in subsequent years (`2026-2027`). Some projects use a single year of first publication forever; both are acceptable. Don't list every year.

**On AI-generated code:** since you've used Claude for a lot of the code, you don't need to mention it in the headers. The headers represent the *human* who selected, edited, integrated, and shipped the code, which is enough for copyright in current US Copyright Office guidance. Don't credit Anthropic or Claude in headers, since it would be both legally unusual and would create unnecessary questions.

**On contributions from others:** if you accept an outside PR before adding a CLA, the contributor automatically owns copyright in their changes (Apache's Section 5 grants you a license, not ownership). For a small project this is fine, since you have all the rights you need under Apache 2.0 to use, modify, sublicense, etc. The only thing you can't do without their permission is *relicense* the project entirely. If that ever matters to you (e.g., you decide to dual-license to AGPL + commercial later), you'd need to either get permission or rewrite their contributions. The CONTRIBUTING.md above leaves the door open to add a CLA later without breaking existing contributions.

**On running this with an agent:** I'd recommend doing a dry-run first. Have it list all files it *would* modify before actually writing anything. Source tree walks can find surprising files (vendored deps, generated code, test fixtures from other projects) and you want to catch those before they get headers they shouldn't have.

That's the whole package. Once these files are in place and headers are applied, you have a clean, defensible, modern open-source legal posture for Lamella with zero ongoing maintenance burden until you decide to register the trademark.
