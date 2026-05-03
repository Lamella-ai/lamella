// Lamella — minimal interactions: copy-to-clipboard for code blocks.
// Diagram connector lines are drawn by the inline IIFE in index.html so they
// can use the curated stagger logic against the pill edge.
(function () {
  'use strict';

  function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function (resolve, reject) {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'absolute';
      ta.style.left = '-9999px';
      document.body.appendChild(ta);
      ta.select();
      try {
        var ok = document.execCommand('copy');
        document.body.removeChild(ta);
        ok ? resolve() : reject(new Error('copy failed'));
      } catch (e) {
        document.body.removeChild(ta);
        reject(e);
      }
    });
  }

  function attachCopy(el) {
    var btn = el.querySelector('.copy-btn');
    if (!btn) return;
    btn.addEventListener('click', function () {
      var src = el.querySelector('[data-copy-src]') || el.querySelector('pre') || el.querySelector('code');
      var text = src ? (src.textContent || '').trim() : '';
      if (!text) return;
      copyText(text).then(function () {
        btn.classList.add('copied');
        var prev = btn.getAttribute('data-label') || 'Copy';
        btn.textContent = 'Copied';
        setTimeout(function () {
          btn.classList.remove('copied');
          btn.textContent = prev;
        }, 1400);
      }).catch(function () {});
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    document.querySelectorAll('[data-copy]').forEach(attachCopy);
  });
})();
