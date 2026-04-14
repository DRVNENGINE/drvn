(function () {
  const DATA_URL   = './data.json';
  const REFRESH_MS = 5 * 60 * 1000;
  const ATR_THRESH = 0.67;

  function fmt(val, decimals) {
    if (val == null) return '-';
    return Number(val).toFixed(decimals || 2);
  }
  function fmtChange(val) {
    if (val == null) return '-';
    return (val >= 0 ? '+' : '') + Number(val).toFixed(2);
  }
  function fmtPct(val) {
    if (val == null) return '-';
    return (val >= 0 ? '+' : '') + Number(val).toFixed(2) + '%';
  }
  function fmtVol(val) {
    if (val == null) return '-';
    if (val >= 1000000) return (val / 1000000).toFixed(1) + 'M';
    if (val >= 1000)    return (val / 1000).toFixed(0) + 'K';
    return val.toString();
  }

  function injectData(payload) {
    const tickers = payload.tickers || {};

    document.querySelectorAll('[data-drvn-field="updated_at"]').forEach(el => {
      el.textContent = payload.updated_at_et || '';
    });

    Object.entries(tickers).forEach(function(entry) {
      const symbol = entry[0];
      const d = entry[1];
      document.querySelectorAll('[data-drvn-ticker="' + symbol + '"]').forEach(function(el) {
        const field = el.getAttribute('data-drvn-field');
        if (!field) return;
        switch (field) {
          case 'price':      el.textContent = '$' + fmt(d.price);       break;
          case 'change':     el.textContent = fmtChange(d.change);      break;
          case 'change_pct': el.textContent = fmtPct(d.change_pct);     break;
          case 'open':       el.textContent = '$' + fmt(d.open);        break;
          case 'high':       el.textContent = '$' + fmt(d.high);        break;
          case 'low':        el.textContent = '$' + fmt(d.low);         break;
          case 'prev_close': el.textContent = '$' + fmt(d.prev_close);  break;
          case 'atr_30m':    el.textContent = fmt(d.atr_30m, 4);        break;
          case 'volume':     el.textContent = fmtVol(d.volume);         break;
          case 'vwap':       el.textContent = '$' + fmt(d.vwap);        break;
          case 'bias':       el.textContent = d.bias || '-';            break;
        }
        if (field === 'change' || field === 'change_pct') {
          el.classList.remove('drvn-up', 'drvn-down', 'drvn-flat');
          if (d.change > 0)      el.classList.add('drvn-up');
          else if (d.change < 0) el.classList.add('drvn-down');
          else                   el.classList.add('drvn-flat');
        }
        if (field === 'bias') {
          el.classList.remove('drvn-bullish', 'drvn-bearish', 'drvn-neutral');
          const b = (d.bias || '').toLowerCase();
          if (b.includes('bull'))      el.classList.add('drvn-bullish');
          else if (b.includes('bear')) el.classList.add('drvn-bearish');
          else                         el.classList.add('drvn-neutral');
        }
        if (field === 'atr_30m') {
          el.classList.remove('drvn-compressed', 'drvn-expanding');
          el.classList.add(d.atr_compressed ? 'drvn-compressed' : 'drvn-expanding');
        }
      });
    });

    var spy = tickers['SPY'];
    if (spy) {
      var banner = document.getElementById('drvn-bias-banner');
      if (banner) {
        banner.textContent = spy.bias;
        banner.className = spy.bias.toLowerCase().includes('bull') ? 'bias-bullish'
          : spy.bias.toLowerCase().includes('bear') ? 'bias-bearish' : 'bias-neutral';
      }
      var atrBadge = document.getElementById('drvn-atr-status');
      if (atrBadge) {
        atrBadge.textContent = spy.atr_compressed ? 'COMPRESSED' : 'EXPANDING';
        atrBadge.className   = spy.atr_compressed ? 'atr-compressed' : 'atr-expanding';
      }
    }
    console.log('[DRVN] Data injected - ' + payload.updated_at_et);
  }

  function loadData() {
    fetch(DATA_URL + '?_=' + Date.now())
      .then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function(payload) { injectData(payload); })
      .catch(function(err) { console.warn('[DRVN] Data load failed:', err); });
  }

  document.addEventListener('DOMContentLoaded', function() {
    loadData();
    setInterval(loadData, REFRESH_MS);
  });
})();
