/* Copyright 2026 Lamella LLC
 * SPDX-License-Identifier: Apache-2.0
 *
 * Position-aware classify / dismiss popover.
 * The popover lives DOM-adjacent to the trigger but is rendered with
 * `position: fixed` so it escapes overflow-hidden table cells / group
 * cards / row containers that would otherwise clip it. We compute the
 * viewport coordinates from the trigger's bounding rect at open time,
 * write them as inline `top` / `left`, then flip the `data-open`
 * attribute the existing CSS reads.
 *
 * Closes on click-outside, Escape, viewport resize / scroll, and after
 * the inner form's HTMX request completes (the existing handler in
 * _txn_actions.html removes data-open on success).
 */
(function () {
  var POPOVER_W = 22 * 16; // matches CSS width: min(22rem, ...)

  function position(trigger, pop) {
    var rect = trigger.getBoundingClientRect();
    var vw = document.documentElement.clientWidth;
    var vh = document.documentElement.clientHeight;
    // Right-anchor: align popover's right edge to trigger's right edge,
    // unless that pushes the left edge offscreen.
    var rightAlignedLeft = rect.right - POPOVER_W;
    var safeLeft = Math.max(8, Math.min(rightAlignedLeft, vw - POPOVER_W - 8));
    pop.style.left = safeLeft + "px";
    // Below the trigger by default; flip above if there's no room.
    var below = rect.bottom + 6;
    var popH = pop.offsetHeight || 200;
    if (below + popH > vh - 8 && rect.top - popH - 6 > 8) {
      pop.style.top = (rect.top - popH - 6) + "px";
    } else {
      pop.style.top = below + "px";
    }
  }

  function closeAll() {
    document.querySelectorAll(".txnact__popover[data-open]").forEach(function (p) {
      p.removeAttribute("data-open");
    });
  }

  window.lamellaTxnactPopover = function (trigger) {
    var pop = trigger.nextElementSibling;
    if (!pop || !pop.classList.contains("txnact__popover")) return;
    var alreadyOpen = pop.hasAttribute("data-open");
    closeAll();
    if (alreadyOpen) return; // toggle: clicking the trigger again closes
    pop.setAttribute("data-open", "");
    // Position AFTER setting data-open so offsetHeight measures correctly.
    position(trigger, pop);
    // Focus the first input so the user can type immediately.
    var input = pop.querySelector("input, button, select, textarea");
    if (input && typeof input.focus === "function") {
      try { input.focus(); } catch (e) { /* element may be hidden */ }
    }
    // Track the trigger for reposition on scroll/resize.
    pop.__trigger = trigger;
  };

  // Click outside to close.
  document.addEventListener("click", function (ev) {
    var target = ev.target;
    if (!(target instanceof Element)) return;
    // Click on a trigger handles itself via the onclick.
    if (target.closest(".txnact__btn--classify, .txnact__btn--dismiss")) return;
    // Click inside an open popover stays open.
    if (target.closest(".txnact__popover")) return;
    closeAll();
  });

  // Escape closes any open popover.
  document.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") closeAll();
  });

  // Reposition open popovers on viewport changes.
  function reposition() {
    document.querySelectorAll(".txnact__popover[data-open]").forEach(function (p) {
      if (p.__trigger && document.body.contains(p.__trigger)) {
        position(p.__trigger, p);
      }
    });
  }
  window.addEventListener("scroll", reposition, { passive: true });
  window.addEventListener("resize", reposition, { passive: true });
})();
