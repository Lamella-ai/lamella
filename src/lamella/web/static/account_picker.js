/* B6 Step 0 — account_picker.js
 *
 * Vanilla controller for the new account-picker macro. Mounted via
 * event delegation on document.body so it survives HTMX swaps —
 * no per-page bind, no re-init after a partial swap.
 *
 * UI behaviors per design doc B4:
 *   - focus + empty value → fetch top suggestions (no behavior on
 *     focus + non-empty: input keeps its value, popup stays closed)
 *   - typing → 150ms debounce → fetch suggested popup partial
 *   - up/down arrow → highlight rows
 *   - Enter → commit highlighted row's data-value to the input
 *   - Escape → close popup, restore prior input
 *   - click outside → close popup
 *   - X-clear button → blank input, close popup, refocus
 *   - X-clear-filter button (decisions-pending §1.2) → strip kind
 *     and re-issue with kind="" so the user can pick across kinds
 *
 * Mobile: when the input would be hidden by the on-screen keyboard,
 * flip the popup above the input via window.visualViewport.
 */
(function () {
  "use strict";

  const DEBOUNCE_MS = 150;
  const SUGGEST_URL = "/api/accounts/suggest";
  const debounceTimers = new WeakMap();
  const stateByPicker = new WeakMap();

  function getState(picker) {
    let s = stateByPicker.get(picker);
    if (!s) {
      s = {
        priorValue: picker.querySelector(".acct-picker-input").value || "",
        kindOverride: null, // null = use macro's kind; "" = filter cleared
      };
      stateByPicker.set(picker, s);
    }
    return s;
  }

  function effectiveKind(picker) {
    const s = getState(picker);
    if (s.kindOverride !== null) return s.kindOverride;
    return picker.dataset.kind || "";
  }

  function buildSuggestUrl(picker, q) {
    const params = new URLSearchParams();
    if (q) params.set("q", q);
    const k = effectiveKind(picker);
    if (k) params.set("kind", k);
    if (picker.dataset.entityHint) params.set("entity", picker.dataset.entityHint);
    params.set("boost", "1");
    params.set("limit", q ? "5" : "3");
    params.set("mode", picker.dataset.mode || "picker");
    if (picker.dataset.allowCreate === "1") params.set("allow_create", "1");
    return SUGGEST_URL + "?" + params.toString();
  }

  function flipPlacement(picker) {
    const popup = picker.querySelector(".acct-picker-popup");
    const input = picker.querySelector(".acct-picker-input");
    if (!popup || !input || !window.visualViewport) return;
    const inputRect = input.getBoundingClientRect();
    const visibleBottom = window.visualViewport.offsetTop + window.visualViewport.height;
    const spaceBelow = visibleBottom - inputRect.bottom;
    if (spaceBelow < 200 && inputRect.top > 200) {
      popup.dataset.flip = "up";
    } else {
      delete popup.dataset.flip;
    }
  }

  function openPopup(picker) {
    const popup = picker.querySelector(".acct-picker-popup");
    const input = picker.querySelector(".acct-picker-input");
    if (!popup) return;
    popup.hidden = false;
    if (input) input.setAttribute("aria-expanded", "true");
    flipPlacement(picker);
  }

  function closePopup(picker) {
    const popup = picker.querySelector(".acct-picker-popup");
    const input = picker.querySelector(".acct-picker-input");
    if (popup) popup.hidden = true;
    if (input) input.setAttribute("aria-expanded", "false");
    clearHighlight(picker);
  }

  function clearHighlight(picker) {
    picker.querySelectorAll(".acct-picker-row.is-highlighted").forEach((row) => {
      row.classList.remove("is-highlighted");
    });
  }

  function highlightRow(picker, row) {
    clearHighlight(picker);
    if (row) row.classList.add("is-highlighted");
  }

  function rows(picker) {
    return Array.from(picker.querySelectorAll(".acct-picker-row"));
  }

  async function fetchSuggestions(picker, q) {
    const popup = picker.querySelector(".acct-picker-popup");
    if (!popup) return;
    try {
      const r = await fetch(buildSuggestUrl(picker, q), {
        headers: { "HX-Request": "true" },
      });
      if (!r.ok) return;
      popup.innerHTML = await r.text();
      const list = popup.querySelector(".acct-picker-list");
      if (list || popup.querySelector(".acct-picker-empty") || popup.querySelector(".acct-picker-filter-strip")) {
        openPopup(picker);
      } else {
        closePopup(picker);
      }
    } catch (e) {
      /* network blip — keep prior popup state */
    }
  }

  function commitValue(picker, value, displayLabel) {
    const input = picker.querySelector(".acct-picker-input");
    const clearBtn = picker.querySelector(".acct-picker-clear");
    if (!input) return;
    input.value = value;
    if (clearBtn) clearBtn.hidden = !value;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
    closePopup(picker);
    getState(picker).priorValue = value;
  }

  function findPicker(target) {
    return target && target.closest ? target.closest("[data-acct-picker]") : null;
  }

  document.addEventListener("focusin", function (e) {
    const picker = findPicker(e.target);
    if (!picker || !e.target.classList.contains("acct-picker-input")) return;
    getState(picker).priorValue = e.target.value || "";
    if (!e.target.value) fetchSuggestions(picker, "");
    if ("scrollIntoView" in e.target && window.matchMedia("(max-width: 768px)").matches) {
      try { e.target.scrollIntoView({ block: "center", behavior: "smooth" }); } catch (_) {}
    }
  });

  document.addEventListener("input", function (e) {
    const picker = findPicker(e.target);
    if (!picker || !e.target.classList.contains("acct-picker-input")) return;
    const val = e.target.value;
    const clearBtn = picker.querySelector(".acct-picker-clear");
    if (clearBtn) clearBtn.hidden = !val;
    const prev = debounceTimers.get(e.target);
    if (prev) clearTimeout(prev);
    debounceTimers.set(
      e.target,
      setTimeout(() => fetchSuggestions(picker, val), DEBOUNCE_MS),
    );
  });

  document.addEventListener("keydown", function (e) {
    const picker = findPicker(e.target);
    if (!picker || !e.target.classList.contains("acct-picker-input")) return;
    const popup = picker.querySelector(".acct-picker-popup");
    const all = rows(picker);
    const cur = picker.querySelector(".acct-picker-row.is-highlighted");
    const idx = cur ? all.indexOf(cur) : -1;

    if (e.key === "ArrowDown") {
      e.preventDefault();
      if (popup && popup.hidden) {
        fetchSuggestions(picker, e.target.value);
        return;
      }
      if (all.length) highlightRow(picker, all[Math.min(idx + 1, all.length - 1)]);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (all.length) highlightRow(picker, all[Math.max(idx - 1, 0)]);
    } else if (e.key === "Enter") {
      if (cur) {
        e.preventDefault();
        commitValue(picker, cur.dataset.value, cur.dataset.display);
      } else if (picker.dataset.allowCreate === "1" && e.target.value) {
        e.preventDefault();
        commitValue(picker, e.target.value, e.target.value);
      }
    } else if (e.key === "Tab") {
      if (cur) commitValue(picker, cur.dataset.value, cur.dataset.display);
    } else if (e.key === "Escape") {
      e.target.value = getState(picker).priorValue;
      closePopup(picker);
    }
  });

  document.addEventListener("click", function (e) {
    // Row click → commit
    const row = e.target.closest && e.target.closest(".acct-picker-row");
    if (row) {
      const picker = findPicker(row);
      if (picker) commitValue(picker, row.dataset.value, row.dataset.display);
      return;
    }
    // X-clear button → blank the input
    const clearBtn = e.target.closest && e.target.closest(".acct-picker-clear");
    if (clearBtn) {
      const picker = findPicker(clearBtn);
      if (picker) {
        const input = picker.querySelector(".acct-picker-input");
        if (input) input.value = "";
        clearBtn.hidden = true;
        closePopup(picker);
        if (input) input.focus();
        getState(picker).priorValue = "";
        if (input) {
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
        }
      }
      return;
    }
    // §1.2 escape-hatch — clear the kind filter
    const filterClear = e.target.closest && e.target.closest("[data-clear-filter]");
    if (filterClear) {
      const picker = findPicker(filterClear);
      if (picker) {
        getState(picker).kindOverride = "";
        const input = picker.querySelector(".acct-picker-input");
        fetchSuggestions(picker, input ? input.value : "");
        if (input) input.focus();
      }
      return;
    }
    // "More" button → re-issue without entity boost
    const more = e.target.closest && e.target.closest(".acct-picker-more");
    if (more) {
      const picker = findPicker(more);
      if (!picker) return;
      const params = new URLSearchParams();
      if (more.dataset.q) params.set("q", more.dataset.q);
      if (more.dataset.kind) params.set("kind", more.dataset.kind);
      if (more.dataset.entity) params.set("entity", more.dataset.entity);
      params.set("boost", "0");
      params.set("limit", "50");
      const popup = picker.querySelector(".acct-picker-popup");
      fetch(SUGGEST_URL + "?" + params.toString(), { headers: { "HX-Request": "true" } })
        .then((r) => (r.ok ? r.text() : ""))
        .then((html) => { if (popup && html) popup.innerHTML = html; });
      return;
    }
    // Click outside any picker → close all
    if (!findPicker(e.target)) {
      document.querySelectorAll("[data-acct-picker]").forEach((p) => closePopup(p));
    }
  });
})();
