/* Theme bootstrap — must run before first paint.

   Kept as a separate file (rather than inline in <head>) so the dashboard can
   ship a strict Content-Security-Policy with `script-src 'self'` and no
   'unsafe-inline'. A blocking <script src> in <head> still executes before the
   first paint, so a light-mode visitor never sees a flash of the dark palette.
   That is why this is NOT deferred.
*/
(function () {
  var t;
  try {
    var saved = localStorage.getItem("aeroflow-theme");
    t = (saved === "light" || saved === "dark") ? saved
      : (window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark");
  } catch (e) {
    t = "dark";
  }
  document.documentElement.setAttribute("data-theme", t);
})();
