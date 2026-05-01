/*
 * Copyright 2026 Lamella LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Shared slug-helper for add-modals (entities, vehicles, properties).
 *
 * Wires up:
 *   1. Live auto-populate: as the user types display_name, the slug
 *      field syncs (PascalCase, [A-Z][A-Za-z0-9_-]*). User can override
 *      the slug — once they edit it manually we stop auto-syncing.
 *   2. Format-on-blur: leaving the slug field with an invalid value
 *      shows an inline error AND tries to fix it (PascalCase the input).
 *   3. Availability-on-blur: leaving the slug field hits a server
 *      endpoint per kind to check whether the slug is already taken;
 *      shows the suggested next-free alternative when collision.
 *
 * Wire it up by calling LamellaSlug.attach({...}) on a freshly-rendered
 * modal — the entity/vehicle/property add-modal templates do this in
 * their tail script. The function is idempotent per-element (binds
 * once, returns silently on re-attach).
 */
(function () {
  "use strict";

  const SLUG_RE = /^[A-Z][A-Za-z0-9_-]*$/;

  /** Convert a free-form display name to a PascalCase slug.
   * Mirrors `suggest_slug` in src/lamella/core/registry/service.py. */
  function suggest(display) {
    if (!display) return "";
    // Strip everything that isn't letter/digit, split on whitespace.
    const cleaned = display.replace(/[^A-Za-z0-9]+/g, " ").trim();
    if (!cleaned) return "";
    const words = cleaned.split(/\s+/).filter(Boolean);
    let pascal = words.map(function (w) {
      return w.charAt(0).toUpperCase() + w.slice(1);
    }).join("");
    if (!pascal) return "";
    if (!/^[A-Za-z]/.test(pascal)) pascal = "X" + pascal;
    if (pascal[0] !== pascal[0].toUpperCase()) {
      pascal = pascal.charAt(0).toUpperCase() + pascal.slice(1);
    }
    return pascal;
  }

  /** Render an inline error / hint under the slug field.
   * Pass tone='err' for red, 'ok' for green tick, 'warn' for orange. */
  function setHint(field, message, tone) {
    let hint = field.parentElement.querySelector(".slug-hint");
    if (!hint) {
      hint = document.createElement("p");
      hint.className = "slug-hint";
      hint.style.fontSize = "0.78rem";
      hint.style.margin = "0.25rem 0 0";
      field.parentElement.appendChild(hint);
    }
    hint.textContent = message || "";
    if (tone === "err") {
      hint.style.color = "var(--err, #c0392b)";
    } else if (tone === "ok") {
      hint.style.color = "var(--ok, #2e8b57)";
    } else if (tone === "warn") {
      hint.style.color = "var(--warn, #c08423)";
    } else {
      hint.style.color = "var(--muted)";
    }
  }

  /** POST { kind, slug, exclude_slug } and read JSON
   * { available: bool, suggestion: str|null }. */
  function checkAvailability(kind, slug) {
    const url = "/api/slugs/check?kind="
              + encodeURIComponent(kind)
              + "&slug=" + encodeURIComponent(slug);
    return fetch(url, { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .catch(function () { return null; });
  }

  /** Attach behavior to a slug input + display_name source.
   *
   * opts:
   *   form: HTMLFormElement — the modal form (used to scope queries).
   *   kind: "entities" | "vehicles" | "properties" — for the
   *         availability endpoint and slug rules.
   */
  function attach(opts) {
    const form = opts.form;
    if (!form || form.dataset.slugHelperAttached === "1") return;
    form.dataset.slugHelperAttached = "1";

    const kind = opts.kind;
    const display = form.querySelector('input[name="display_name"]');
    const slug = form.querySelector('input[name="slug"]');
    if (!display || !slug) return;

    let userTouched = slug.value !== "";

    function syncFromDisplay() {
      if (userTouched) return;
      const proposal = suggest(display.value);
      slug.value = proposal;
      if (proposal) setHint(slug, "auto-suggested from display name", "");
    }
    display.addEventListener("input", syncFromDisplay);

    slug.addEventListener("input", function () {
      // User started typing in the slug field — stop auto-syncing.
      userTouched = true;
    });

    slug.addEventListener("blur", function () {
      const raw = (slug.value || "").trim();
      if (!raw) {
        // Empty + display has text → try one last auto-fill.
        if (display.value) {
          slug.value = suggest(display.value);
        }
        return;
      }
      if (!SLUG_RE.test(raw)) {
        const fixed = suggest(raw);
        if (fixed && fixed !== raw && SLUG_RE.test(fixed)) {
          slug.value = fixed;
          setHint(slug, "fixed format → " + fixed, "warn");
        } else {
          setHint(slug,
            "must start with a capital letter and contain only "
            + "letters, digits, hyphen, or underscore",
            "err");
          return;
        }
      }
      // Availability check.
      const candidate = slug.value;
      if (!kind) return;
      checkAvailability(kind, candidate).then(function (resp) {
        if (!resp) return;
        if (resp.available) {
          setHint(slug, "available ✓", "ok");
        } else {
          const sugg = resp.suggestion || "";
          if (sugg) {
            setHint(slug, "already taken — try " + sugg, "err");
          } else {
            setHint(slug, "already taken", "err");
          }
        }
      });
    });
  }

  window.LamellaSlug = { attach: attach, suggest: suggest };
})();
