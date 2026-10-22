var _LIBS = {
  xlsx:  { src:'https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js', sri:'sha384-vtjasyidUo0kW94K5MXDXntzOJpQgBKXmE7e2Ga4LG0skTTLeBi97eFAXsqewJjw', ready:function(){ return typeof XLSX !== 'undefined'; } },
  zxing: { src:'https://cdn.jsdelivr.net/npm/@zxing/library@0.19.1/umd/index.min.js', sri:'sha384-NyKHkzm0aj4yWFC3Hh4cp1VflBgCLfStVlAK6WJOdXAht/pj6RHbMcZgUj48rcAs', ready:function(){ return !!window.ZXing; } }
};
var _libPromises = {};
function _loadLib(name) {
  var lib = _LIBS[name];
  if (lib.ready()) return Promise.resolve();
  if (_libPromises[name]) return _libPromises[name];
  _libPromises[name] = new Promise(function(resolve) {
    var el = document.createElement('script');
    el.src = lib.src; el.integrity = lib.sri; el.crossOrigin = 'anonymous'; el.async = true;
    el.onload = function(){ resolve(); };
    el.onerror = function(){ delete _libPromises[name]; el.remove(); resolve(); }; // callers check lib.ready() and show their own message
    document.head.appendChild(el);
  });
  return _libPromises[name];
}
