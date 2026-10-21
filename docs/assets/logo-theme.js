// Keeps the <picture>-based brand logos (docs/index.md, docs/poppydb.md) in sync with
// Material for MkDocs' manual light/dark toggle. <source media="(prefers-color-scheme:
// dark)"> only reacts to the OS-level preference; it has no way to react to the theme's
// own toggle, which sets [data-md-color-scheme] on <body> independently of the OS setting.
// This mirrors that attribute onto the <img> so both switches (OS and in-page toggle) work.
// Harmless outside mkdocs (e.g. GitHub/wiki rendering): no [data-md-color-scheme] ever
// appears there, so <picture>'s native prefers-color-scheme behavior is all that applies.
(function () {
  function applyScheme(scheme) {
    document.querySelectorAll("picture > source[srcset]").forEach(function (source) {
      var img = source.parentElement.querySelector("img");
      if (!img) return;
      if (!img.dataset.lightSrc) {
        img.dataset.lightSrc = img.getAttribute("src");
        img.dataset.darkSrc = source.getAttribute("srcset");
      }
      img.setAttribute("src", scheme === "slate" ? img.dataset.darkSrc : img.dataset.lightSrc);
    });
  }

  function currentScheme() {
    return document.body.getAttribute("data-md-color-scheme");
  }

  function init() {
    applyScheme(currentScheme());
    new MutationObserver(function () {
      applyScheme(currentScheme());
    }).observe(document.body, { attributes: true, attributeFilter: ["data-md-color-scheme"] });
  }

  if (document.body) {
    init();
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
