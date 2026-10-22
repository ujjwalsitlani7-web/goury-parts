// ============================================================
// LANGUAGE TOGGLE — English ↔ Hindi (UI Labels)
// ============================================================
const I18N = {
  en: {
    // Header
    app_subtitle: 'Spare Parts Automation',
    // Dashboard stats
    today_sale: "Today's Sale",
    this_month: 'This Month',
    month_profit: 'Month Profit',
    credit_due: 'Credit Due',
    low_stock: 'Low Stock',
    out_of_stock: 'Out of Stock',
    // Action plan
    action_plan: "☀️ Today's Action Plan",
    // Bottom nav
    nav_home: 'Home',
    nav_owner: 'Owner',
    nav_stock: 'Stock',
    nav_sale: 'Sale',
    nav_wholesale: 'Wholesale',
    nav_staff: 'Staff',
    nav_reports: 'Reports',
    nav_wa: 'WA',
    nav_import: 'Import',
    nav_collect: 'Collect',
    nav_demand: 'Demand',
    nav_ai: 'AI',
    nav_purchase: 'Purchase',
    // Sale tab
    sale_heading: '🧾 New Counter Sale',
    sale_customer_label: 'Customer',
    sale_walkin: 'Walk-in Customer (Instant Price)',
    sale_search_label: 'Search & Add Parts',
    sale_cart: '🛒 Cart',
    sale_cart_empty: 'Add parts above to start a sale',
    sale_subtotal: 'Subtotal',
    sale_discount: 'Discount ₹',
    sale_total: 'TOTAL',
    sale_payment_mode: 'Payment Mode',
    sale_cash: 'Cash',
    sale_credit: 'Credit',
    sale_details: '👤 Sale Details',
    sale_soldby: '🧾 Sold By',
    sale_select_staff: '— select staff —',
    sale_deliveredby: '🛵 Delivered By',
    sale_walkin_counter: 'Walk-in / Counter',
    sale_mech_section: '🔧 Mechanic Referral Commission',
    sale_mech_name: 'Mechanic Name',
    sale_no_mech: 'No mechanic — direct walk-in',
    sale_mech_comm: 'Commission % on this sale',
    sale_walkin_section: '👤 Walk-in Customer',
    sale_anonymous: 'Anonymous walk-in',
    sale_add_walkin: '+ Add New Walk-in Customer to Database',
    sale_complete: '✓ Complete Sale & Print Bill',
    sale_clear: '🗑 Clear Cart',
    // Part search results (used in JS)
    sale_in_stock: 'in stock',
    sale_tap_add: 'Tap to add',
    sale_out_of_stock_toast: 'Out of stock!',
    sale_stock_limit: 'Stock limit reached',
    // Toggle button label
    toggle_label: 'हिं',
  },
  hi: {
    // Header
    app_subtitle: 'स्पेयर पार्ट्स ऑटोमेशन',
    // Dashboard stats
    today_sale: 'आज की बिक्री',
    this_month: 'इस महीने',
    month_profit: 'महीने का मुनाफा',
    credit_due: 'उधार बाकी',
    low_stock: 'कम स्टॉक',
    out_of_stock: 'स्टॉक खत्म',
    // Action plan
    action_plan: '☀️ आज का काम',
    // Bottom nav
    nav_home: 'होम',
    nav_owner: 'मालिक',
    nav_stock: 'स्टॉक',
    nav_sale: 'बिक्री',
    nav_wholesale: 'थोक',
    nav_staff: 'स्टाफ',
    nav_reports: 'रिपोर्ट',
    nav_wa: 'WA',
    nav_import: 'इम्पोर्ट',
    nav_collect: 'वसूली',
    nav_demand: 'माँग',
    nav_ai: 'AI',
    nav_purchase: 'खरीद',
    // Sale tab
    sale_heading: '🧾 नई बिक्री',
    sale_customer_label: 'ग्राहक',
    sale_walkin: 'वॉक-इन ग्राहक (तुरंत मूल्य)',
    sale_search_label: 'पार्ट खोजें और जोड़ें',
    sale_cart: '🛒 कार्ट',
    sale_cart_empty: 'बिक्री शुरू करने के लिए ऊपर पार्ट जोड़ें',
    sale_subtotal: 'उप-कुल',
    sale_discount: 'छूट ₹',
    sale_total: 'कुल',
    sale_payment_mode: 'भुगतान तरीका',
    sale_cash: 'नकद',
    sale_credit: 'उधार',
    sale_details: '👤 बिक्री विवरण',
    sale_soldby: '🧾 किसने बेचा',
    sale_select_staff: '— स्टाफ चुनें —',
    sale_deliveredby: '🛵 डिलीवरी किसने की',
    sale_walkin_counter: 'काउंटर / वॉक-इन',
    sale_mech_section: '🔧 मैकेनिक कमीशन',
    sale_mech_name: 'मैकेनिक का नाम',
    sale_no_mech: 'कोई मैकेनिक नहीं — सीधी बिक्री',
    sale_mech_comm: 'इस बिक्री पर कमीशन %',
    sale_walkin_section: '👤 वॉक-इन ग्राहक',
    sale_anonymous: 'अज्ञात ग्राहक',
    sale_add_walkin: '+ नया वॉक-इन ग्राहक जोड़ें',
    sale_complete: '✓ बिक्री पूरी करें और बिल प्रिंट करें',
    sale_clear: '🗑 कार्ट साफ करें',
    // Part search results (used in JS)
    sale_in_stock: 'स्टॉक में',
    sale_tap_add: 'जोड़ने के लिए टैप करें',
    sale_out_of_stock_toast: 'स्टॉक खत्म!',
    sale_stock_limit: 'स्टॉक सीमा पहुंच गई',
    // Toggle button label
    toggle_label: 'EN',
  }
};

let currentLang = localStorage.getItem('shopai_lang') || 'en';

function applyLanguage(lang) {
  currentLang = lang;
  localStorage.setItem('shopai_lang', lang);
  const dict = I18N[lang];

  // Translate all data-i18n elements
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.getAttribute('data-i18n');
    if (dict[key] !== undefined) el.textContent = dict[key];
  });

  // Header subtitle (dynamic, set via renderHome too — handle separately)
  const subEl = document.getElementById('hdr-sub');
  if (subEl) subEl.textContent = dict.app_subtitle;

  // Toggle button label flips to opposite
  const btn = document.getElementById('lang-toggle-btn');
  if (btn) btn.textContent = dict.toggle_label;

  // Visual cue — button glows when Hindi is active
  if (btn) {
    btn.style.background = lang === 'hi'
      ? 'rgba(255,220,80,.35)'
      : 'rgba(255,255,255,.15)';
    btn.style.fontWeight = '700';
  }

  // Clear sale search results so they re-render with correct language
  const sr = document.getElementById('sale-search-results');
  if (sr) sr.innerHTML = '';

  // Toast confirmation
  if (typeof toast === 'function') {
    toast(lang === 'hi' ? '🇮🇳 हिंदी में बदला गया' : '🔤 Switched to English');
  }
}

function t(key) {
  return (I18N[currentLang] && I18N[currentLang][key]) ? I18N[currentLang][key] : (I18N['en'][key] || key);
}

function toggleLanguage() {
  applyLanguage(currentLang === 'en' ? 'hi' : 'en');
}

// Apply on page load (after DOM ready)
document.addEventListener('DOMContentLoaded', function() {
  applyLanguage(currentLang);
});
// Also apply after a short delay to catch dynamically rendered labels
setTimeout(() => applyLanguage(currentLang), 800);
