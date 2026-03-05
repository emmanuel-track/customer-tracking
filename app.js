'use strict';

// ═══════════════════════════════════════════
//  FIREBASE SETUP
// ═══════════════════════════════════════════
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getFirestore, doc, getDoc, setDoc, onSnapshot }
  from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import {
  getAuth, createUserWithEmailAndPassword, signInWithEmailAndPassword,
  signOut, onAuthStateChanged, updatePassword, EmailAuthProvider,
  reauthenticateWithCredential
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";

const firebaseConfig = {
  apiKey: "AIzaSyDmh1RYZmY3ACZTSkRW0YScSLDylqFwtvg",
  authDomain: "emmanuel-s-tracking.firebaseapp.com",
  projectId: "emmanuel-s-tracking",
  storageBucket: "emmanuel-s-tracking.firebasestorage.app",
  messagingSenderId: "679834142883",
  appId: "1:679834142883:web:e06e2a1709cb15d075d716",
  measurementId: "G-WNSY3QEWT6"
};

const fbApp  = initializeApp(firebaseConfig);
const db_fs  = getFirestore(fbApp);
const fb_auth = getAuth(fbApp);

// ── Firestore document reference for a key ──
const docRef = (k) => doc(db_fs, 'fuelapp', k);

// ── In-memory cache (keeps UI snappy) ──
const _cache = {};

// ═══════════════════════════════════════════
//  DB — drop-in replacement for localStorage
//  All reads are instant (from cache).
//  All writes go to Firestore + cache.
//  Real-time listeners keep cache fresh.
// ═══════════════════════════════════════════
const DB = {
  get(k, d=null){
    return (_cache[k] !== undefined) ? _cache[k] : d;
  },
  set(k, v){
    _cache[k] = v;
    // Fire-and-forget write to Firestore
    setDoc(docRef(k), { value: JSON.stringify(v) }).catch(e => console.warn('DB.set error', k, e));
  },
};

// ── Load ALL keys from Firestore into cache before booting the app ──
async function loadAllFromFirestore(){
  const keys = ['_users','_purchases','_customers','_deliveries','_inv',
                 '_audit','_prefs','_avatars','_sms','_theme','_notifications','_thresholds',
                 '_backup_schedule','_last_backup_ts'];
  const timeout = (ms) => new Promise((_,rej) => setTimeout(()=>rej(new Error('timeout')), ms));
  await Promise.all(keys.map(async k => {
    try {
      const snap = await Promise.race([getDoc(docRef(k)), timeout(5000)]);
      if(snap.exists()){
        _cache[k] = JSON.parse(snap.data().value);
      }
    } catch(e){ console.warn('load error', k, e); }
  }));
}

// ── Real-time listeners — any change from another device updates cache & re-renders ──
function attachListeners(appInstance){
  const keys = ['_purchases','_customers','_deliveries','_inv','_audit','_users','_prefs','_sms'];
  // Also listen to ledger keys for all existing customers
  const custLedgerKeys = DB.get('_customers',[]).map(c=>'_ledger_'+c.id);
  [...keys,...custLedgerKeys].forEach(k => {
    onSnapshot(docRef(k), snap => {
      if(!snap.exists()) return;
      try {
        const newVal = JSON.parse(snap.data().value);
        // Only re-render if value actually changed
        if(JSON.stringify(_cache[k]) !== JSON.stringify(newVal)){
          _cache[k] = newVal;
          // Re-render current page to reflect remote changes
          if(appInstance && appInstance.page){
            const p = appInstance.page;
            const renders = {
              dashboard: ()=>appInstance.renderDash(),
              inventory:  ()=>appInstance.renderInv(),
              customers:  ()=>appInstance.renderCusts(),
              record:     ()=>appInstance.renderRecord(),
              monthly:    ()=>appInstance.renderMonthly(),
              yearly:     ()=>appInstance.renderYearly(),
              history:    ()=>appInstance.renderHist(),
              audit:      ()=>appInstance.renderAudit(),
              settings:   ()=>appInstance.renderSettings(),
            };
            renders[p]?.();
            // Re-check low stock whenever purchases or inventory changes
            if(k==='_purchases'||k==='_inv') appInstance._checkLowStock();
          }
        }
      } catch(e){ console.warn('listener parse error', k, e); }
    });
  });
}

const MF = ['January','February','March','April','May','June','July','August','September','October','November','December'];
const MS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const CUR_SYM = { GHS:'GHS\u00a0', USD:'$', EUR:'€', GBP:'£', NGN:'₦', KES:'KSh\u00a0', ZAR:'R\u00a0' };
const AV_COLS  = ['#F97316','#3B82F6','#10B981','#8B5CF6','#EC4899','#F59E0B','#06B6D4','#EF4444'];

const F = {
  money(n){ const s=(DB.get('_prefs',{currency:'GHS'}).currency)||'GHS'; return (CUR_SYM[s]||s+'\u00a0')+(+n).toLocaleString('en',{minimumFractionDigits:2,maximumFractionDigits:2}); },
  n2(n){ return (+n).toLocaleString('en',{minimumFractionDigits:2,maximumFractionDigits:2}); },
  date(s){ if(!s) return '—'; return new Date(s+'T00:00:00').toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'}); },
  init(s){ return String(s||'').split(' ').map(w=>w[0]||'').join('').slice(0,2).toUpperCase()||'??'; },
  esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); },
  today(){ return new Date().toISOString().slice(0,10); },
  pct(v,t){ return t?(v/t*100).toFixed(2)+'%':'0.00%'; },
  pctN(v,t){ return t?+(v/t*100).toFixed(2):0; },
  id(){ return Date.now().toString(36)+Math.random().toString(36).slice(2,6); },
  ts(){ return new Date().toLocaleString('en-GB'); },
};
const av = n => AV_COLS[String(n||'').charCodeAt(0)%AV_COLS.length];

/* ════ INVENTORY HELPERS ════ */
const INV = {
  totals(){ return DB.get('_inv',{petrolTotal:0,dieselTotal:0}); },
  sold(fuel){ return DB.get('_purchases',[]).filter(p=>p.fuel===fuel).reduce((s,p)=>s+p.litres,0); },
  remain(fuel){ const t=INV.totals(); const tot=(fuel==='Petrol'?t.petrolTotal:t.dieselTotal)||0; return Math.max(0,tot-INV.sold(fuel)); },
  pct(litres,fuel){ const t=INV.totals(); const tot=(fuel==='Petrol'?t.petrolTotal:t.dieselTotal)||0; return tot?F.pct(litres,tot):'N/A'; },
  pctN(litres,fuel){ const t=INV.totals(); const tot=(fuel==='Petrol'?t.petrolTotal:t.dieselTotal)||0; return tot?F.pctN(litres,tot):0; },
};

class App {
  constructor(){ this.user=null; this.page='dashboard'; this.cPage=1; this.hPage=1; this.aPage=1; this.PS=15; this._delCb=null; this._rcpt=null; this.dFrom=null; this.dTo=null; }

  async init(){
    this._showLoadingOverlay();
    let _initDone = false;
    const safetyTimer = setTimeout(()=>{
      if(!_initDone){ _initDone=true; this._hideLoadingOverlay(); if(!this.user) this._showLogin(); }
    }, 8000);
    onAuthStateChanged(fb_auth, async (fbUser) => {
      clearTimeout(safetyTimer);
      const ADMIN_EMAIL = 'eb810111@gmail.com';
      if(fbUser){
        // Already booted with this user — do nothing (avoids re-boot on minor token refresh)
        if(this.user && this.user.uid === fbUser.uid) return;
        // Wait for Firestore data BEFORE booting so dashboard has real data
        try { await loadAllFromFirestore(); } catch(e){ console.warn('Firestore load failed', e); }
        let profile = DB.get('_users',[]).find(u=>u.uid===fbUser.uid);
        if(!profile){
          const role = fbUser.email.toLowerCase()===ADMIN_EMAIL ? 'admin' : 'staff';
          profile = { uid:fbUser.uid, email:fbUser.email, display:fbUser.email.split('@')[0], role, createdAt:new Date().toLocaleDateString('en-GB') };
          const users = DB.get('_users',[]); users.push(profile); DB.set('_users', users);
        } else if(fbUser.email.toLowerCase()===ADMIN_EMAIL && profile.role!=='admin'){
          profile.role = 'admin';
          const users = DB.get('_users',[]).map(u=>u.uid===profile.uid?{...u,role:'admin'}:u);
          DB.set('_users', users);
        }
        this.user = profile;
        this._hideLoadingOverlay();
        this._boot();
      } else {
        // Signed out — reset state and show login immediately
        this.user = null;
        this._bound = false;
        this._hideLoadingOverlay();
        this._showLogin();
      }
    });
  }

  _showLogin(){
    document.getElementById('login-screen').style.display='';
    document.getElementById('app').style.display='none';

    // Clear any auto-filled credentials from password manager
    const lUser = document.getElementById('l-user');
    const lPass = document.getElementById('l-pass');
    const lBtn  = document.getElementById('login-form')?.querySelector('[type=submit]');
    if(lUser) lUser.value='';
    if(lPass) lPass.value='';
    if(lBtn){ lBtn.textContent='Sign In'; lBtn.disabled=false; }
    document.getElementById('l-err')?.classList.remove('show');
    // Small delay before re-enabling submit to block password manager auto-submit
    this._loginReady = false;
    setTimeout(()=>{ this._loginReady = true; }, 800);
    // Also mark ready immediately on any user keypress
    const _markReady = ()=>{ this._loginReady = true; };
    lUser?.addEventListener('keydown', _markReady, {once:true});
    lPass?.addEventListener('keydown', _markReady, {once:true});

    // Toggle between sign-in and register
    document.getElementById('go-register').onclick=()=>{
      document.getElementById('auth-signin').style.display='none';
      document.getElementById('auth-register').style.display='flex';
      ['r-display','r-uname','r-pwd','r-cpwd'].forEach(id=>document.getElementById(id).value='');
      document.getElementById('reg-err').classList.remove('show');
      document.getElementById('r-role').value='staff';
      document.getElementById('role-staff').classList.add('selected');
      document.getElementById('role-admin').classList.remove('selected');
    };
    document.getElementById('go-signin').onclick=()=>{
      document.getElementById('auth-register').style.display='none';
      document.getElementById('auth-signin').style.display='flex';
    };

    // Password visibility toggles
    const _pwToggle=(inputId, btnId)=>{
      const inp=document.getElementById(inputId);
      const btn=document.getElementById(btnId);
      if(!inp||!btn) return;
      btn.addEventListener('click',()=>{
        const visible=inp.type==='text';
        inp.type=visible?'password':'text';
        btn.classList.toggle('visible',!visible);
      });
    };
    _pwToggle('l-pass','l-pass-toggle');
    _pwToggle('r-pwd','r-pwd-toggle');
    _pwToggle('r-cpwd','r-cpwd-toggle');
    document.querySelectorAll('.role-card[data-role]').forEach(c=>{
      c.onclick=()=>{
        document.querySelectorAll('.role-card[data-role]').forEach(x=>x.classList.remove('selected'));
        c.classList.add('selected');
        document.getElementById('r-role').value=c.dataset.role;
      };
    });

    // Sign-in submit
    document.getElementById('login-form').onsubmit=async e=>{
      e.preventDefault();
      // Block auto-submit from password manager on page load
      if(!this._loginReady) return;
      const email=document.getElementById('l-user').value.trim().toLowerCase();
      const pwd=document.getElementById('l-pass').value;
      const errEl=document.getElementById('l-err');
      const btn=e.target.querySelector('[type=submit]');
      errEl.classList.remove('show');
      btn.textContent='Signing in…'; btn.disabled=true;
      try {
        const cred = await signInWithEmailAndPassword(fb_auth, email, pwd);
        // Directly boot — don't wait for onAuthStateChanged
        const ADMIN_EMAIL = 'eb810111@gmail.com';
        try { await loadAllFromFirestore(); } catch(e){ console.warn('Firestore load failed', e); }
        let profile = DB.get('_users',[]).find(u=>u.uid===cred.user.uid);
        if(!profile){
          const role = email===ADMIN_EMAIL ? 'admin' : 'staff';
          profile = { uid:cred.user.uid, email, display:email.split('@')[0], role, createdAt:new Date().toLocaleDateString('en-GB') };
          const users = DB.get('_users',[]); users.push(profile); DB.set('_users', users);
        }
        this.user = profile;
        this._boot();
      } catch(err){
        btn.textContent='Sign In'; btn.disabled=false;
        const msgs={
          'auth/user-not-found':'No account found with this email.',
          'auth/wrong-password':'Incorrect password.',
          'auth/invalid-email':'Please enter a valid email address.',
          'auth/invalid-credential':'Incorrect email or password.',
          'auth/too-many-requests':'Too many attempts. Please try again later.',
        };
        errEl.textContent=msgs[err.code]||'Sign in failed. Please try again.';
        errEl.classList.add('show');
      }
    };

    // Register submit
    document.getElementById('reg-form').onsubmit=async e=>{
      e.preventDefault();
      const display=document.getElementById('r-display').value.trim();
      const email=document.getElementById('r-uname').value.trim().toLowerCase();
      const pwd=document.getElementById('r-pwd').value;
      const cpwd=document.getElementById('r-cpwd').value;
      const role=document.getElementById('r-role').value;
      const err=document.getElementById('reg-err');
      const btn=e.target.querySelector('[type=submit]');
      if(!display||!email||!pwd||!cpwd){ err.textContent='All fields are required.'; err.classList.add('show'); return; }
      if(pwd.length<6){ err.textContent='Password must be at least 6 characters.'; err.classList.add('show'); return; }
      if(pwd!==cpwd){ err.textContent='Passwords do not match.'; err.classList.add('show'); return; }
      err.classList.remove('show');
      btn.textContent='Creating…'; btn.disabled=true;
      try {
        const cred = await createUserWithEmailAndPassword(fb_auth, email, pwd);
        const ADMIN_EMAIL = 'eb810111@gmail.com';
        const role = email.toLowerCase()===ADMIN_EMAIL ? 'admin' : 'staff';
        const newUser = { uid:cred.user.uid, email, display, role, createdAt:new Date().toLocaleDateString('en-GB') };
        const users = DB.get('_users',[]);
        users.push(newUser); DB.set('_users', users);
        this._audit('create',`New account registered: ${display} (${email})`,`Role: ${role}`);
        this._notifySMS('user',`Name: ${display}\nEmail: ${email}\nRole: ${role}`);
        // onAuthStateChanged will boot the app
      } catch(err2){
        btn.textContent='Create Account'; btn.disabled=false;
        const msgs={
          'auth/email-already-in-use':'An account with this email already exists.',
          'auth/invalid-email':'Please enter a valid email address.',
          'auth/weak-password':'Password must be at least 6 characters.',
        };
        err.textContent=msgs[err2.code]||'Registration failed. Please try again.';
        err.classList.add('show');
      }
    };
  }

  _boot(){
    document.getElementById('login-screen').style.display='none';
    document.getElementById('app').style.display='';
    const d=this.user.display||this.user.email||'User';
    document.getElementById('user-chip-name').textContent=d;
    document.getElementById('user-av').textContent=F.init(d);
    const sbName=document.getElementById('sb-user-name');
    const sbRole=document.getElementById('sb-user-role');
    const sbAv=document.getElementById('sb-user-av');
    if(sbName) sbName.textContent=d;
    if(sbRole) sbRole.textContent=(this.user.role||'staff').charAt(0).toUpperCase()+(this.user.role||'staff').slice(1);
    if(sbAv){ sbAv.textContent=F.init(d); sbAv.style.background=av(d); }
    this._refreshTopbar();
    document.documentElement.setAttribute('data-theme',DB.get('_theme','light'));
    // Apply role-based permissions
    const isAdmin = this.user.role === 'admin';
    document.querySelectorAll('.admin-only').forEach(el => el.style.display = isAdmin ? '' : 'none');
    if(!this._bound){ this._bind(); this._fillSelects(); this._initRange(); }
    this.nav('dashboard');
    attachListeners(this);
    this._checkLowStock();
    this._renderNotifPanel();
    this._checkAutoBackup();
  }

  _audit(type,action,detail=''){
    const logs=DB.get('_audit',[]);
    logs.unshift({id:F.id(),type,action,detail,user:this.user?.display||this.user?.username||'System',ts:F.ts()});
    if(logs.length>3000) logs.splice(3000);
    DB.set('_audit',logs);
  }

  _initRange(){
    const purchases = DB.get('_purchases', []);
    const now = new Date(), y = now.getFullYear(), m = now.getMonth();
    const ld = new Date(y, m+1, 0);
    const t = `${y}-${String(m+1).padStart(2,'0')}-${String(ld.getDate()).padStart(2,'0')}`;
    // Start from first purchase date, or start of current month if no purchases
    let f;
    if(purchases.length){
      const sorted = purchases.slice().sort((a,b)=>a.date.localeCompare(b.date));
      f = sorted[0].date;
    } else {
      f = `${y}-${String(m+1).padStart(2,'0')}-01`;
    }
    this.dFrom=f; this.dTo=t;
    document.getElementById('d-from').value=f;
    document.getElementById('d-to').value=t;
  }

  _bind(){
    if(this._bound) return; this._bound=true;

    // Wipe old listeners on key elements by cloning them
    const _rebind = (id) => {
      const el = document.getElementById(id);
      if(!el) return null;
      const clone = el.cloneNode(true);
      el.parentNode.replaceChild(clone, el);
      return clone;
    };

    // Re-bind nav items fresh (removes stale listeners from previous login)
    document.querySelectorAll('.nav-item[data-page]').forEach(el => {
      const clone = el.cloneNode(true);
      el.parentNode.replaceChild(clone, el);
      clone.addEventListener('click', () => {
        this.nav(clone.dataset.page);
        document.getElementById('app').classList.remove('nav-open');
      });
    });

    // Hamburger menu button — mobile
    const menuBtn = _rebind('menu-btn');
    if(menuBtn) menuBtn.addEventListener('click', () => document.getElementById('app').classList.toggle('nav-open'));

    // Close nav on outside tap — named handler, never duplicated
    if(this._appClickHandler) document.getElementById('app')?.removeEventListener('click', this._appClickHandler);
    this._appClickHandler = (e) => {
      if(!e.target.closest('.sidebar') && !e.target.closest('#menu-btn'))
        document.getElementById('app').classList.remove('nav-open');
    };
    document.getElementById('app')?.addEventListener('click', this._appClickHandler);

    // Sidebar X close button
    const sbClose = _rebind('sb-close-btn');
    if(sbClose) sbClose.addEventListener('click', () => document.getElementById('app').classList.remove('nav-open'));

    // Logout — show login immediately, sign out Firebase in background
    const _doLogout = () => {
      this._audit('login', `"${this.user?.display}" signed out`);
      this.user = null; this._bound = false; this._tblHandlers = {};
      this._showLogin(); signOut(fb_auth);
    };
    const sbLogout  = _rebind('sb-logout-btn');
    const logoutBtn = _rebind('logout-btn');
    if(sbLogout)  sbLogout.addEventListener('click',  _doLogout);
    if(logoutBtn) logoutBtn.addEventListener('click', _doLogout);

    // Theme toggle
    const themeBtn = _rebind('theme-btn');
    if(themeBtn) themeBtn.addEventListener('click', () => {
      const t = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', t); DB.set('_theme', t); this._redraw();
    });

    // Notification bell
    const notifBtn   = _rebind('notif-btn');
    const notifClear = _rebind('notif-clear-btn');
    if(notifBtn)   notifBtn.addEventListener('click',   (e) => { e.stopPropagation(); this._toggleNotifPanel(); });
    if(notifClear) notifClear.addEventListener('click', () => { DB.set('_notifications', []); this._renderNotifPanel(); });

    // Close notif panel on outside click — named handler, never duplicated
    if(this._docClickHandler) document.removeEventListener('click', this._docClickHandler);
    this._docClickHandler = (e) => { if(!e.target.closest('#notif-wrap')) document.getElementById('notif-panel').classList.remove('open'); };
    document.addEventListener('click', this._docClickHandler);

    // Dashboard range
    document.getElementById('d-from').addEventListener('change',e=>{ this.dFrom=e.target.value; this.renderDash(); });
    document.getElementById('d-to').addEventListener('change',e=>{ this.dTo=e.target.value; this.renderDash(); });
    document.getElementById('d-reset').addEventListener('click',()=>{ this._initRange(); this.renderDash(); });

    // Customers
    document.getElementById('add-cust-btn').addEventListener('click',()=>this.openCust(null));
    document.getElementById('exp-cust-btn').addEventListener('click',()=>this.expCusts());
    document.getElementById('c-search').addEventListener('input',()=>{ this.cPage=1; this.renderCusts(); });
    document.getElementById('c-fuel-f').addEventListener('change',()=>{ this.cPage=1; this.renderCusts(); });
    document.getElementById('c-balance-f').addEventListener('change',()=>{ this.cPage=1; this.renderCusts(); });
    document.getElementById('c-reset-btn').addEventListener('click',()=>{ document.getElementById('c-search').value=''; document.getElementById('c-fuel-f').value=''; document.getElementById('c-balance-f').value=''; this.cPage=1; this.renderCusts(); });
    document.getElementById('save-payment-btn').addEventListener('click',()=>this._savePayment());
    document.getElementById('save-cust-btn').addEventListener('click',()=>this.saveCust());

    // Record
    document.getElementById('rec-form').addEventListener('submit',e=>{ e.preventDefault(); this.saveRecord(); });
    document.getElementById('rec-clear').addEventListener('click',()=>this.clearForm());
    document.getElementById('ft-p').addEventListener('click',()=>this.setFuel('Petrol'));
    document.getElementById('ft-d').addEventListener('click',()=>this.setFuel('Diesel'));
    ['r-litres','r-ppl'].forEach(id=>document.getElementById(id).addEventListener('input',()=>{ this._calcTotal(); this._updatePreview(); this._chkInvWarn(); }));
    document.getElementById('r-cust').addEventListener('change',()=>this._updatePreview());
    document.getElementById('r-date').addEventListener('change',()=>this._updatePreview());

    // History
    document.getElementById('h-q').addEventListener('input',()=>{ this.hPage=1; this.renderHist(); });
    document.getElementById('h-fuel').addEventListener('change',()=>{ this.hPage=1; this.renderHist(); });
    document.getElementById('h-month').addEventListener('change',()=>{ this.hPage=1; this.renderHist(); });
    document.getElementById('h-from').addEventListener('change',()=>{ this.hPage=1; this.renderHist(); });
    document.getElementById('h-to').addEventListener('change',()=>{ this.hPage=1; this.renderHist(); });
    document.getElementById('h-reset').addEventListener('click',()=>{ ['h-q','h-fuel','h-month','h-from','h-to'].forEach(id=>{ const e=document.getElementById(id); if(e) e.value=''; }); this.hPage=1; this.renderHist(); });
    document.getElementById('exp-hist-btn').addEventListener('click',()=>this.expHist());
    document.getElementById('exp-hist-pdf-btn').addEventListener('click',()=>this.expHistPDF());

    // Monthly/Yearly
    ['m-month','m-year'].forEach(id=>document.getElementById(id).addEventListener('change',()=>this.renderMonthly()));
    document.getElementById('y-year').addEventListener('change',()=>this.renderYearly());
    document.getElementById('y-compare-year').addEventListener('change',()=>this._renderYoY());
    document.getElementById('exp-monthly-btn').addEventListener('click',()=>this.expMonthly());
    document.getElementById('exp-monthly-pdf-btn').addEventListener('click',()=>this.expMonthlyPDF());
    document.getElementById('exp-yearly-btn').addEventListener('click',()=>this.expYearly());

    // Inventory
    document.getElementById('add-inv-btn').addEventListener('click',()=>this.openInv());
    document.getElementById('save-inv-btn').addEventListener('click',()=>this.saveInv());

    // Edit purchase
    ['ep-litres','ep-ppl'].forEach(id=>document.getElementById(id).addEventListener('input',()=>{
      const l=+document.getElementById('ep-litres').value||0, p=+document.getElementById('ep-ppl').value||0;
      document.getElementById('ep-total').value=l&&p?F.money(l*p):'';
    }));
    document.getElementById('save-ep-btn').addEventListener('click',()=>this.saveEP());

    // Settings
    document.getElementById('stabs').addEventListener('click',e=>{ const t=e.target.closest('.stab'); if(!t) return; document.querySelectorAll('.stab').forEach(x=>x.classList.remove('active')); document.querySelectorAll('.spane').forEach(x=>x.classList.remove('active')); t.classList.add('active'); const pane=document.getElementById('sp-'+t.dataset.stab); if(pane) pane.classList.add('active'); if(t.dataset.stab==='users') this.renderUsersTable(); });
    document.getElementById('save-profile-btn').addEventListener('click',()=>this.saveProfile());

    // Profile picture upload
    document.getElementById('upload-av-btn').addEventListener('click',()=>document.getElementById('av-file-input').click());
    document.getElementById('av-file-input').addEventListener('change',e=>{ const file=e.target.files[0]; if(file) this._loadAvatarFile(file); e.target.value=''; });
    document.getElementById('camera-av-btn').addEventListener('click',()=>this.openCamera());
    document.getElementById('remove-av-btn').addEventListener('click',()=>this._removeAvatar());

    // Camera
    document.getElementById('cam-snap-btn').addEventListener('click',()=>this._snapPhoto());
    document.getElementById('cam-retake-btn').addEventListener('click',()=>this._retakePhoto());
    document.getElementById('cam-use-btn').addEventListener('click',()=>this._usePhoto());
    document.getElementById('cam-flip-btn').addEventListener('click',()=>this._flipCamera());

    // SMS settings
    document.getElementById('save-sms-btn').addEventListener('click',()=>this.saveSMSSettings());
    document.getElementById('test-sms-btn').addEventListener('click',()=>this.sendTestSMS());
    document.getElementById('sms-master').addEventListener('change',e=>{ document.getElementById('sms-status-badge').textContent=e.target.checked?'Enabled':'Disabled'; document.getElementById('sms-status-badge').style.cssText=e.target.checked?'background:var(--diesel-bg);color:var(--diesel);border:1px solid var(--diesel-bdr)':'background:var(--surface-3);color:var(--t3);border:1px solid var(--border)'; });
    document.getElementById('save-pass-btn').addEventListener('click',()=>this.savePass());
    document.getElementById('save-prefs-btn').addEventListener('click',()=>this.savePrefs());

    // Audit
    document.getElementById('al-q').addEventListener('input',()=>{ this.aPage=1; this.renderAudit(); });
    document.getElementById('al-type').addEventListener('change',()=>{ this.aPage=1; this.renderAudit(); });
    document.getElementById('al-reset').addEventListener('click',()=>{ document.getElementById('al-q').value=''; document.getElementById('al-type').value=''; this.aPage=1; this.renderAudit(); });
    document.getElementById('exp-audit-btn').addEventListener('click',()=>this.expAudit());
    document.getElementById('clear-audit-btn').addEventListener('click',()=>this._confirm('Clear the entire audit log? This cannot be undone.',()=>{ DB.set('_audit',[]); this.renderAudit(); this.toast('Audit log cleared'); }));
    document.getElementById('del-ok').addEventListener('click',()=>{ if(this._delCb) this._delCb(); this.closeModal('m-del'); });

    // User management
    document.getElementById('add-user-btn').addEventListener('click',()=>this.openAddUser());
    document.getElementById('save-eu-btn').addEventListener('click',()=>this.saveEditUser());
    document.querySelectorAll('[data-eurole]').forEach(c=>{
      c.addEventListener('click',()=>{
        document.querySelectorAll('[data-eurole]').forEach(x=>x.classList.remove('selected'));
        c.classList.add('selected');
        document.getElementById('eu-role').value=c.dataset.eurole;
      });
    });

    // Delegated events
    if(!this._tblHandlers) this._tblHandlers = {};
    const _tbl = (id, fn) => {
      const el = document.getElementById(id); if(!el) return;
      if(this._tblHandlers[id]) el.removeEventListener('click', this._tblHandlers[id]);
      this._tblHandlers[id] = e => fn(e);
      el.addEventListener('click', this._tblHandlers[id]);
    };
    _tbl('rec-recent', e=>this._recAc(e));
    _tbl('h-tbody',    e=>this._recAc(e));
    _tbl('m-tbody',    e=>this._recAc(e));
    _tbl('c-tbody',    e=>this._custAc(e));
    _tbl('inv-tbody',  e=>this._invAc(e));
  }

  _recAc(e){ const b=e.target.closest('[data-ac]'); if(!b) return; const {ac,id}=b.dataset; if(ac==='ep') this.openEP(id); if(ac==='dp') this._confirm('Delete this purchase?',()=>this.delPurchase(id)); if(ac==='rcp') this.showReceipt(id); }
  _custAc(e){ const b=e.target.closest('button[data-ac]'); if(!b) return; const {ac,id}=b.dataset; if(ac==='ec') this.openCust(id); if(ac==='dc') this._confirm('Delete this customer and all their data?',()=>this.delCust(id)); if(ac==='vc') this.viewProfile(id); if(ac==='pay') this.openPayment(id); }
  _invAc(e){ const b=e.target.closest('[data-ac]'); if(!b) return; if(b.dataset.ac==='di') this._confirm('Delete this delivery entry?',()=>this.delInv(b.dataset.id)); }

  nav(page){
    this.page=page;
    document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
    document.querySelectorAll('.nav-item[data-page]').forEach(l=>l.classList.toggle('active',l.dataset.page===page));
    const pageEl=document.getElementById('page-'+page);
    if(!pageEl) return;
    pageEl.classList.add('active');
    document.getElementById('topbar-title').textContent=({dashboard:'Dashboard',inventory:'Fuel Inventory',customers:'Customers',record:'Record Purchase',monthly:'Monthly Report',yearly:'Yearly Report',history:'Purchase History',audit:'Audit Log',settings:'Settings'})[page]||page;
    document.getElementById('app').classList.remove('nav-open');
    ({dashboard:()=>this.renderDash(),inventory:()=>this.renderInv(),customers:()=>this.renderCusts(),record:()=>this.renderRecord(),monthly:()=>this.renderMonthly(),yearly:()=>this.renderYearly(),history:()=>this.renderHist(),audit:()=>this.renderAudit(),settings:()=>this.renderSettings()})[page]?.();
  }
  _redraw(){ ({dashboard:()=>this.renderDash(),inventory:()=>this.renderInv(),monthly:()=>this.renderMonthly(),yearly:()=>this.renderYearly()})[this.page]?.(); }

  _fillSelects(){
    const now=new Date(), cm=now.getMonth(), cy=now.getFullYear();
    ['m-month'].forEach(id=>{ const e=document.getElementById(id); if(!e) return; e.innerHTML=MF.map((m,i)=>`<option value="${i}" ${i===cm?'selected':''}>${m}</option>`).join(''); });
    document.getElementById('h-month').innerHTML='<option value="">All Months</option>'+MF.map((m,i)=>`<option value="${i}">${m}</option>`).join('');
    ['m-year','y-year'].forEach(id=>{ const e=document.getElementById(id); if(!e) return; let h=''; for(let y=cy+1;y>=cy-5;y--) h+=`<option value="${y}" ${y===cy?'selected':''}>${y}</option>`; e.innerHTML=h; });
    const ycEl=document.getElementById('y-compare-year'); if(ycEl){ let h='<option value="">— select year —</option>'; for(let y=cy+1;y>=cy-5;y--) h+=`<option value="${y}">${y}</option>`; ycEl.innerHTML=h; }
  }

  /* ── DASHBOARD ── */
  renderDash(){
    // Show KPI skeletons on first render if no data yet
    const kpiGrid = document.getElementById('d-kpis');
    if(kpiGrid && !kpiGrid.querySelector('.kpi-card') && !DB.get('_purchases',[]).length){
      kpiGrid.innerHTML = Array(4).fill('<div class="skeleton sk-kpi"></div>').join('');
    }
    const ps=DB.get('_purchases',[]);
    const f=this.dFrom, t=this.dTo;
    const arr=ps.filter(p=>{ if(f&&p.date<f) return false; if(t&&p.date>t) return false; return true; });
    document.getElementById('d-range-label').textContent=f&&t?`${F.date(f)} — ${F.date(t)}`:'All time';
    const pL=arr.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const dL=arr.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);
    const tL=pL+dL, rev=arr.reduce((s,p)=>s+p.total,0), tx=arr.length, custs=[...new Set(arr.map(p=>p.cid))].length;
    document.getElementById('d-kpis').innerHTML=
      this._kpi('Total Revenue',F.money(rev),'kpi-icon','background:var(--brand-lt);color:var(--brand)',`${tx} transactions`,'var(--brand)',100,'<path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>')+
      this._kpi('Total Fuel Sold',F.n2(tL)+' L','kpi-icon','background:var(--petrol-bg);color:var(--petrol)','Petrol + Diesel combined','var(--petrol)',F.pctN(pL,tL),'<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>')+
      this._kpi('Petrol',F.n2(pL)+' L','kpi-icon','background:var(--petrol-bg);color:var(--petrol)',F.pct(pL,tL)+' of fuel sold','var(--petrol)',F.pctN(pL,tL),'<circle cx="12" cy="12" r="10"/>')+
      this._kpi('Diesel',F.n2(dL)+' L','kpi-icon','background:var(--diesel-bg);color:var(--diesel)',F.pct(dL,tL)+' of fuel sold','var(--diesel)',F.pctN(dL,tL),'<rect x="2" y="3" width="20" height="14" rx="2"/>')+
      this._kpi('Customers',custs,'kpi-icon','background:var(--info-bg);color:var(--info)',`${tx} total transactions`,'var(--info)',0,'<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/>')+
      this._kpi('Avg Sale',tx?F.money(rev/tx):'—','kpi-icon','background:var(--brand-lt);color:var(--brand)','Per transaction average','var(--brand)',0,'<line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>');
    this._donut('d-donut',pL,dL); document.getElementById('d-dval').textContent=F.n2(tL)+'L'; this._dlegend('d-dleg',pL,dL,tL);
    const tD=f?new Date(f+'T00:00:00'):new Date();
    // Defer to next frame so the page layout is fully computed before drawing canvas
    requestAnimationFrame(()=>{ this._trend('d-trend',arr,tD.getMonth(),tD.getFullYear()); });
    const cm=this._cmap();
    const ct={}; arr.forEach(p=>{ if(!ct[p.cid]) ct[p.cid]={p:0,d:0,rev:0}; if(p.fuel==='Petrol') ct[p.cid].p+=p.litres; else ct[p.cid].d+=p.litres; ct[p.cid].rev+=p.total; });
    const top=Object.entries(ct).sort((a,b)=>b[1].rev-a[1].rev).slice(0,8);
    const tb=document.getElementById('d-top');
    if(!top.length){ tb.innerHTML=`<tr><td colspan="9" class="empty-row">No data for this period.</td></tr>`; return; }
    tb.innerHTML=top.map(([cid,d],i)=>{ const c=cm[cid]||{name:'Unknown',car:'—'}; const tot=d.p+d.d; return `<tr>
      <td><strong style="color:var(--t3)">${i+1}</strong></td>
      <td><div class="row-gap"><div class="cust-av" style="background:${av(c.name)}">${F.init(c.name)}</div><span class="fw-600" style="cursor:pointer;color:var(--brand)" onclick="A.viewProfile('${cid}')">${F.esc(c.name)}</span></div></td>
      <td><span style="font-family:monospace;font-size:.8rem">${F.esc(c.car)}</span></td>
      <td>${F.n2(d.p)} L</td>
      <td><span class="badge badge-petrol">${F.pct(d.p,tL)}</span></td>
      <td>${F.n2(d.d)} L</td>
      <td><span class="badge badge-diesel">${F.pct(d.d,tL)}</span></td>
      <td class="fw-700">${F.n2(tot)} L</td>
      <td class="fw-700 text-brand">${F.money(d.rev)}</td>
    </tr>`; }).join('');
  }

  /* ── INVENTORY ── */
  renderInv(){
    const inv=INV.totals(), ps=DB.get('_purchases',[]);
    const pSold=INV.sold('Petrol'), dSold=INV.sold('Diesel');
    const pRem=Math.max(0,(inv.petrolTotal||0)-pSold), dRem=Math.max(0,(inv.dieselTotal||0)-dSold);
    const pPct=inv.petrolTotal?F.pctN(pRem,inv.petrolTotal):0, dPct=inv.dieselTotal?F.pctN(dRem,inv.dieselTotal):0;
    document.getElementById('inv-cards').innerHTML=this._invCard('Petrol','petrol',inv.petrolTotal||0,pSold,pRem,pPct)+this._invCard('Diesel','diesel',inv.dieselTotal||0,dSold,dRem,dPct);
    ['petrol','diesel'].forEach(f=>{ const c=document.getElementById(`mr-${f}`); if(!c) return; const pct=f==='petrol'?pPct:dPct; this._miniRing(c,pct,f==='petrol'?'#0EA5E9':'#10B981'); });
    const del=DB.get('_deliveries',[]), tb=document.getElementById('inv-tbody');
    if(!del.length){ tb.innerHTML=`<tr><td colspan="6" class="empty-row">No deliveries recorded yet.</td></tr>`; return; }
    tb.innerHTML=del.slice().reverse().map(d=>`<tr>
      <td>${F.date(d.date)}</td>
      <td><span class="badge badge-${d.fuel.toLowerCase()}">${d.fuel}</span></td>
      <td class="fw-700">${F.n2(d.litres)} L</td>
      <td>${F.esc(d.supplier||'—')}</td>
      <td class="text-muted" style="font-size:.75rem;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${F.esc(d.notes||'—')}</td>
      <td>${this.user?.role==='admin'?`<button class="btn btn-danger btn-xs" data-ac="di" data-id="${d.id}">Remove</button>`:''}</td>
    </tr>`).join('');
  }

  _invCard(label,cls,total,sold,remain,pct){
    const clr=cls==='petrol'?'#0EA5E9':'#10B981';
    return `<div class="inv-card ${cls}-card">
      <div class="inv-card-header">
        <div>
          <span class="badge badge-${cls}" style="margin-bottom:.5rem">${label}</span>
          <div class="inv-total-val">${F.n2(remain)}<span class="inv-total-unit">L</span></div>
          <div class="inv-sub">Remaining of ${F.n2(total)} L total</div>
        </div>
        <div class="mini-ring"><canvas id="mr-${cls}" width="54" height="54"></canvas><div class="mini-ring-val">${pct.toFixed(0)}%</div></div>
      </div>
      <div class="inv-bar-bg"><div class="inv-bar-fill ${cls}" style="width:${pct}%"></div></div>
      <div class="inv-stats">
        <div class="inv-stat-item"><div class="inv-stat-lbl">Total Added</div><div class="inv-stat-val">${F.n2(total)} L</div></div>
        <div class="inv-stat-item"><div class="inv-stat-lbl">Sold</div><div class="inv-stat-val" style="color:${clr}">${F.n2(sold)} L</div></div>
        <div class="inv-stat-item"><div class="inv-stat-lbl">Remaining %</div><div class="inv-stat-val">${pct.toFixed(1)}%</div></div>
      </div>
    </div>`;
  }

  _miniRing(canvas,pct,clr){
    const ctx=canvas.getContext('2d'),dpr=devicePixelRatio||1;
    const sz=58;
    canvas.style.width=sz+'px'; canvas.style.height=sz+'px';
    canvas.width=sz*dpr; canvas.height=sz*dpr; ctx.scale(dpr,dpr);
    const cx=sz/2,cy=sz/2,r=24,th=4.5;
    const theme=document.documentElement.getAttribute('data-theme');
    ctx.beginPath(); ctx.arc(cx,cy,r,0,Math.PI*2);
    ctx.strokeStyle=theme==='dark'?'rgba(255,255,255,.08)':'rgba(0,0,0,.07)';
    ctx.lineWidth=th; ctx.stroke();
    if(pct>0){
      ctx.beginPath(); ctx.arc(cx,cy,r,-Math.PI/2,-Math.PI/2+(pct/100)*Math.PI*2);
      ctx.strokeStyle=clr; ctx.lineWidth=th; ctx.lineCap='round'; ctx.stroke();
    }
  }

  openInv(){ document.getElementById('inv-fuel').value='Petrol'; document.getElementById('inv-litres').value=''; document.getElementById('inv-date').value=F.today(); document.getElementById('inv-supplier').value=''; document.getElementById('inv-notes').value=''; document.getElementById('inv-err').classList.remove('show'); document.getElementById('m-inv').style.display='flex'; }

  saveInv(){
    const fuel=document.getElementById('inv-fuel').value, litres=+document.getElementById('inv-litres').value, date=document.getElementById('inv-date').value, supplier=document.getElementById('inv-supplier').value.trim(), notes=document.getElementById('inv-notes').value.trim();
    if(!litres||litres<=0||!date){ document.getElementById('inv-err').classList.add('show'); return; }
    const d={id:F.id(),fuel,litres,date,supplier,notes,addedAt:F.ts()};
    const dels=DB.get('_deliveries',[]); dels.push(d); DB.set('_deliveries',dels);
    const inv=INV.totals();
    if(fuel==='Petrol') inv.petrolTotal=(inv.petrolTotal||0)+litres; else inv.dieselTotal=(inv.dieselTotal||0)+litres;
    DB.set('_inv',inv);
    this._audit('create',`Delivery: ${litres} L ${fuel}`,supplier||'No supplier');
    this.closeModal('m-inv'); this.renderInv(); this.toast(`${F.n2(litres)} L of ${fuel} added`);
  }

  delInv(id){
    const dels=DB.get('_deliveries',[]); const d=dels.find(x=>x.id===id); if(!d) return;
    const inv=INV.totals();
    if(d.fuel==='Petrol') inv.petrolTotal=Math.max(0,(inv.petrolTotal||0)-d.litres); else inv.dieselTotal=Math.max(0,(inv.dieselTotal||0)-d.litres);
    DB.set('_inv',inv); DB.set('_deliveries',dels.filter(x=>x.id!==id));
    this._audit('delete',`Removed delivery: ${d.litres} L ${d.fuel}`);
    this.renderInv(); this.toast('Delivery removed','error');
  }

  _chkInvWarn(){
    const fuel=document.getElementById('r-fuel').value, litres=+document.getElementById('r-litres').value||0;
    const remain=INV.remain(fuel), total=INV.totals(), tot=(fuel==='Petrol'?total.petrolTotal:total.dieselTotal)||0;
    const w=document.getElementById('inv-warn'), m=document.getElementById('inv-warn-msg');
    if(tot>0&&litres>remain){ w.style.display='flex'; m.textContent=`Only ${F.n2(remain)} L of ${fuel} left — you're entering ${F.n2(litres)} L.`; }
    else if(tot>0&&remain<300){ w.style.display='flex'; m.textContent=`Low stock: ${F.n2(remain)} L of ${fuel} remaining.`; }
    else w.style.display='none';
  }

  /* ── CUSTOMERS ── */
  renderCusts(){
    const custs=DB.get('_customers',[]), ps=DB.get('_purchases',[]);
    let arr=custs.slice();
    const q=document.getElementById('c-search').value.trim().toLowerCase();
    const ff=document.getElementById('c-fuel-f').value;
    const bf=document.getElementById('c-balance-f').value;
    if(q) arr=arr.filter(c=>(c.name||'').toLowerCase().includes(q)||(c.car||'').toLowerCase().includes(q)||(c.phone||'').toLowerCase().includes(q));
    if(ff){ const cids=new Set(ps.filter(p=>p.fuel===ff).map(p=>p.cid)); arr=arr.filter(c=>cids.has(c.id)); }
    // Balance filter
    if(bf==='debt') arr=arr.filter(c=>this._custBalance(c.id)<0);
    if(bf==='credit') arr=arr.filter(c=>this._custBalance(c.id)>0);
    // Debtors summary
    const allBalances=custs.map(c=>this._custBalance(c.id));
    const totalDebt=allBalances.filter(b=>b<0).reduce((s,b)=>s+Math.abs(b),0);
    const totalCredit=allBalances.filter(b=>b>0).reduce((s,b)=>s+b,0);
    const debtorCount=allBalances.filter(b=>b<0).length;
    const dsEl=document.getElementById('debtors-summary');
    if(dsEl){ dsEl.style.display=debtorCount>0?'':'none'; document.getElementById('ds-count').textContent=debtorCount; document.getElementById('ds-total').textContent=F.money(totalDebt); document.getElementById('ds-credit').textContent=F.money(totalCredit); }
    const pages=Math.ceil(arr.length/this.PS)||1; this.cPage=Math.min(this.cPage,pages);
    const slice=arr.slice((this.cPage-1)*this.PS,this.cPage*this.PS);
    const inv=INV.totals();
    const tb=document.getElementById('c-tbody');
    if(!slice.length){ tb.innerHTML=`<tr><td colspan="12" class="empty-row">No customers found.</td></tr>`; }
    else tb.innerHTML=slice.map((c,i)=>{
      const cps=ps.filter(p=>p.cid===c.id);
      const pet=cps.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
      const die=cps.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);
      const rev=cps.reduce((s,p)=>s+p.total,0);
      const last=cps.length?cps.slice().sort((a,b)=>b.date.localeCompare(a.date))[0].date:'—';
      const totFuel=pet+die;
      const pTot=(inv.petrolTotal||0)+(inv.dieselTotal||0);
      const stockPct=pTot?F.pct(totFuel,pTot):'N/A';
      const bal=this._custBalance(c.id);
      const balHtml=bal===0?`<span style="color:var(--t4);font-size:.75rem">Settled</span>`:bal>0?`<span style="color:var(--success);font-weight:700">+${F.money(bal)}</span>`:`<span style="color:var(--danger);font-weight:700">-${F.money(Math.abs(bal))}</span>`;
      return `<tr>
        <td class="text-muted">${(this.cPage-1)*this.PS+i+1}</td>
        <td><div class="row-gap"><div class="cust-av" style="background:${av(c.name)}">${F.init(c.name)}</div><div><div class="fw-600">${F.esc(c.name)}</div><div style="font-size:.68rem;color:var(--t4)">${F.esc(c.email||'')}</div></div></div></td>
        <td><span style="font-family:monospace;font-size:.78rem">${F.esc(c.car)}</span></td>
        <td class="text-muted">${F.esc(c.phone||'—')}</td>
        <td>${F.n2(pet)} L</td>
        <td>${F.n2(die)} L</td>
        <td class="fw-700">${F.n2(totFuel)} L</td>
        <td><span class="badge badge-brand">${stockPct}</span></td>
        <td class="fw-700 text-brand">${F.money(rev)}</td>
        <td>${balHtml}</td>
        <td>${last==='—'?'—':F.date(last)}</td>
        <td><div class="row-gap">
          <button class="btn btn-ghost btn-xs" data-ac="vc" data-id="${c.id}" title="View Profile"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:3px;vertical-align:-1px;pointer-events:none"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>View</button>
          ${this.user?.role==='admin'?'<button class="btn btn-info btn-xs" data-ac="pay" data-id="'+c.id+'" title="Record Balance / Payment"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:3px;vertical-align:-1px;pointer-events:none"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg>Balance</button>':''}
          ${(()=>{ const isA=this.user?.role==='admin'; return (isA?'<button class="btn btn-secondary btn-xs" data-ac="ec" data-id="'+c.id+'" title="Edit Customer"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:3px;vertical-align:-1px;pointer-events:none"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>Edit</button><button class="btn btn-danger btn-xs" data-ac="dc" data-id="'+c.id+'" title="Delete Customer"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:3px;vertical-align:-1px;pointer-events:none"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/></svg>Delete</button>':''); })()}
        </div></td>
      </tr>`;
    }).join('');
    this._pager('c-pager',this.cPage,pages,p=>{ this.cPage=p; this.renderCusts(); });
  }

  openCust(id){
    if(!id && this.user?.role !== 'admin'){ this.toast('Only the Admin can add customers','error'); return; }
    const c=id?(DB.get('_customers',[]).find(x=>x.id===id)||null):null;
    document.getElementById('mc-title').textContent=c?'Edit Customer':'Add Customer';
    document.getElementById('cf-id').value=c?.id||'';
    document.getElementById('cf-name').value=c?.name||'';
    document.getElementById('cf-car').value=c?.car||'';
    document.getElementById('cf-phone').value=c?.phone||'';
    document.getElementById('cf-email').value=c?.email||'';
    document.getElementById('cf-addr').value=c?.addr||'';
    document.getElementById('cf-err').classList.remove('show');
    document.getElementById('m-cust').style.display='flex';
  }

  saveCust(){
    const id=document.getElementById('cf-id').value, name=document.getElementById('cf-name').value.trim(), car=document.getElementById('cf-car').value.trim(), phone=document.getElementById('cf-phone').value.trim();
    if(!name||!car||!phone){ document.getElementById('cf-err').classList.add('show'); return; }
    let custs=DB.get('_customers',[]);
    const data={id:id||F.id(),name,car,phone,email:document.getElementById('cf-email').value.trim(),addr:document.getElementById('cf-addr').value.trim()};
    if(id){ custs=custs.map(c=>c.id===id?{...c,...data}:c); this._audit('edit',`Edited customer: ${name}`); this.toast('Customer updated'); }
    else { custs.push({...data,createdAt:F.ts()}); this._audit('create',`Added customer: ${name}`); this.toast('Customer added'); }
    DB.set('_customers',custs); this.closeModal('m-cust'); this.renderCusts();
  }

  delCust(id){
    const custs=DB.get('_customers',[]); const c=custs.find(x=>x.id===id);
    DB.set('_customers',custs.filter(x=>x.id!==id)); DB.set('_purchases',DB.get('_purchases',[]).filter(p=>p.cid!==id));
    this._audit('delete',`Deleted customer: ${c?.name||id}`); this.renderCusts(); this.toast('Customer deleted','error');
  }

  viewProfile(cid){
    const cm=this._cmap(); const c=cm[cid]; if(!c){ this.toast('Not found','error'); return; }
    const ps=DB.get('_purchases',[]).filter(p=>p.cid===cid);
    const pet=ps.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const die=ps.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);
    const rev=ps.reduce((s,p)=>s+p.total,0), tL=pet+die;
    const last=ps.length?ps.slice().sort((a,b)=>b.date.localeCompare(a.date))[0].date:'—';
    const inv=INV.totals(), pTot=(inv.petrolTotal||0)+(inv.dieselTotal||0);
    const custStockPct=pTot?F.pct(tL,pTot):'N/A';
    const bal=this._custBalance(cid);
    const balBanner=bal===0
      ?`<div class="balance-banner zero" style="margin-bottom:.85rem"><span class="balance-label">Balance</span><span class="balance-amount">Settled ✓</span></div>`
      :bal>0
      ?`<div class="balance-banner credit" style="margin-bottom:.85rem"><div><span class="balance-label">Credit Balance</span><div class="balance-amount">+${F.money(bal)}</div></div><div class="balance-actions"><button class="btn btn-secondary btn-sm" onclick="A.closeModal('m-profile');A.openPayment('${cid}')">Manage</button></div></div>`
      :`<div class="balance-banner debt" style="margin-bottom:.85rem"><div><span class="balance-label">Outstanding Debt</span><div class="balance-amount">-${F.money(Math.abs(bal))}</div></div><div class="balance-actions"><button class="btn btn-primary btn-sm" onclick="A.closeModal('m-profile');A.openPayment('${cid}')"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;vertical-align:-2px"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg>Record Payment</button></div></div>`;
    const ledger=this._custLedger(cid);
    const ledgerHtml=ledger.length?ledger.slice(0,8).map(e=>`
      <div class="ledger-row ${e.type}">
        <div class="ledger-dot"></div>
        <div class="ledger-desc">${F.esc(e.note)}</div>
        <div class="ledger-amt">${e.amount>0?'+':''}${F.money(e.amount)}</div>
        <div class="ledger-ts">${e.ts}</div>
      </div>`).join(''):`<div style="font-size:.75rem;color:var(--t4);padding:.5rem 0">No transactions yet</div>`;
    document.getElementById('mp-title').textContent=c.name+' — Profile';
    document.getElementById('mp-body').innerHTML=`
      <div class="profile-hd">
        <div class="profile-big-av" style="background:${av(c.name)}">${F.init(c.name)}</div>
        <div>
          <div class="profile-name">${F.esc(c.name)}</div>
          <div class="profile-meta">${F.esc(c.phone||'')}${c.email?' · '+F.esc(c.email):''}</div>
          <div class="profile-meta">Vehicle: <strong>${F.esc(c.car)}</strong> · ${ps.length} visits${c.addr?' · '+F.esc(c.addr):''}</div>
        </div>
      </div>
      ${balBanner}
      <div class="profile-stats">
        <div class="pstat"><div class="pstat-lbl">Total Spent</div><div class="pstat-val orange">${F.money(rev)}</div></div>
        <div class="pstat"><div class="pstat-lbl">Total Fuel</div><div class="pstat-val">${F.n2(tL)} L</div></div>
        <div class="pstat"><div class="pstat-lbl">Petrol</div><div class="pstat-val petrol">${F.n2(pet)} L</div></div>
        <div class="pstat"><div class="pstat-lbl">Diesel</div><div class="pstat-val diesel">${F.n2(die)} L</div></div>
        <div class="pstat"><div class="pstat-lbl">Petrol %</div><div class="pstat-val">${F.pct(pet,tL)}</div></div>
        <div class="pstat"><div class="pstat-lbl">Diesel %</div><div class="pstat-val">${F.pct(die,tL)}</div></div>
        <div class="pstat"><div class="pstat-lbl">% of Total Stock</div><div class="pstat-val orange">${custStockPct}</div></div>
        <div class="pstat"><div class="pstat-lbl">Last Visit</div><div class="pstat-val" style="font-size:.82rem">${last==='—'?'—':F.date(last)}</div></div>
      </div>
      <div style="font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--t4);margin-bottom:.5rem;margin-top:.85rem">Balance Ledger</div>
      <div style="margin-bottom:.85rem">${ledgerHtml}</div>
      <div style="font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--t4);margin-bottom:.6rem">Purchase History</div>
      <div class="tbl-wrap" style="max-height:260px;overflow-y:auto">
        <table class="tbl">
          <thead><tr><th>Date</th><th>Fuel</th><th>Litres</th><th>% of Stock</th><th>Price/L</th><th>Total</th><th></th></tr></thead>
          <tbody>${ps.length?ps.slice().sort((a,b)=>b.date.localeCompare(a.date)).map(p=>`<tr>
            <td>${F.date(p.date)}</td>
            <td><span class="badge badge-${p.fuel.toLowerCase()}">${p.fuel}</span></td>
            <td>${F.n2(p.litres)} L</td>
            <td><span class="badge badge-brand">${INV.pct(p.litres,p.fuel)}</span></td>
            <td>${F.money(p.ppl)}</td>
            <td class="fw-700 text-brand">${F.money(p.total)}</td>
            <td><button class="btn btn-info btn-xs" onclick="A.showReceipt('${p.id}');A.closeModal('m-profile')">Receipt</button></td>
          </tr>`).join(''):`<tr><td colspan="7" class="empty-row">No purchases yet.</td></tr>`}</tbody>
        </table>
      </div>`;
    document.getElementById('m-profile').style.display='flex';
  }

  _custBalance(cid){
    const ledger=DB.get('_ledger_'+cid,[]);
    return ledger.reduce((s,e)=>s+e.amount,0);
  }

  _custLedger(cid){
    return DB.get('_ledger_'+cid,[]).slice().sort((a,b)=>b.ts.localeCompare(a.ts));
  }

  openPayment(cid){
    const cm=this._cmap(); const c=cm[cid]; if(!c) return;
    document.getElementById('pay-cid').value=cid;
    document.getElementById('mp-pay-title').textContent=`${c.name} — Payment`;
    document.getElementById('pay-amount').value='';
    document.getElementById('pay-note').value='';
    document.getElementById('pay-type').value='payment';
    document.getElementById('pay-err').classList.remove('show');
    const bal=this._custBalance(cid);
    const info=document.getElementById('pay-balance-info');
    info.innerHTML=bal===0?`<div class="balance-banner zero"><span class="balance-label">Balance</span><span class="balance-amount">Settled</span></div>`
      :bal>0?`<div class="balance-banner credit"><span class="balance-label">Credit Balance</span><span class="balance-amount">+${F.money(bal)}</span></div>`
      :`<div class="balance-banner debt"><span class="balance-label">Outstanding Debt</span><span class="balance-amount">-${F.money(Math.abs(bal))}</span></div>`;
    document.getElementById('m-payment').style.display='flex';
  }

  _savePayment(){
    const cid=document.getElementById('pay-cid').value;
    const type=document.getElementById('pay-type').value;
    const amount=+document.getElementById('pay-amount').value;
    const note=document.getElementById('pay-note').value.trim();
    if(!amount||amount<=0){ document.getElementById('pay-err').classList.add('show'); return; }
    const ledger=DB.get('_ledger_'+cid,[]);
    const entry={
      id: F.id(),
      type,
      amount: type==='payment'? amount : type==='credit'? amount : -amount,
      note: note||( type==='payment'?'Payment received': type==='credit'?'Credit added':'Debt recorded'),
      ts: F.ts(),
      user: this.user?.display||'Admin'
    };
    ledger.push(entry);
    DB.set('_ledger_'+cid, ledger);
    const cm=this._cmap(); const c=cm[cid]||{};
    this._audit('edit',`${type==='payment'?'Payment received from':'Balance updated for'} ${c.name||cid}`,`${type}: ${F.money(amount)}${note?' — '+note:''}`);
    this.closeModal('m-payment');
    this.renderCusts();
    this.toast(type==='payment'?'Payment recorded':'Balance updated');
  }

  expCusts(){
    const custs=DB.get('_customers',[]), ps=DB.get('_purchases',[]);
    const rows=[['Name','Vehicle','Phone','Email','Address','Petrol (L)','Diesel (L)','% of Total Stock','Revenue','Visits']];
    custs.forEach(c=>{ const cps=ps.filter(p=>p.cid===c.id); const pet=cps.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0); const die=cps.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0); const inv=INV.totals(),pTot=(inv.petrolTotal||0)+(inv.dieselTotal||0); rows.push([c.name,c.car,c.phone||'',c.email||'',c.addr||'',pet.toFixed(2),die.toFixed(2),pTot?F.pct(pet+die,pTot):'N/A',cps.reduce((s,p)=>s+p.total,0).toFixed(2),cps.length]); });
    this._csv(rows,'customers'); this._audit('create','Exported customer list');
  }

  /* ── RECORD PURCHASE ── */
  renderRecord(){
    const custs=DB.get('_customers',[]), sel=document.getElementById('r-cust'), prev=sel.value;
    sel.innerHTML='<option value="">— Select customer —</option>'+custs.map(c=>`<option value="${c.id}" ${c.id===prev?'selected':''}>${F.esc(c.name)} (${F.esc(c.car)})</option>`).join('');
    if(!document.getElementById('r-date').value) document.getElementById('r-date').value=F.today();
    this._renderRecent();
  }

  _renderRecent(){
    const ps=DB.get('_purchases',[]), cm=this._cmap();
    const arr=ps.slice().sort((a,b)=>b.date.localeCompare(a.date)).slice(0,10);
    const tb=document.getElementById('rec-recent');
    if(!arr.length){ tb.innerHTML=`<tr><td colspan="9" class="empty-row">No purchases yet.</td></tr>`; return; }
    tb.innerHTML=arr.map(p=>{ const c=cm[p.cid]||{name:'?',car:'—'}; return `<tr>
      <td>${F.date(p.date)}</td>
      <td class="fw-600">${F.esc(c.name)}</td>
      <td><span style="font-family:monospace;font-size:.78rem">${F.esc(c.car)}</span></td>
      <td><span class="badge badge-${p.fuel.toLowerCase()}">${p.fuel}</span></td>
      <td>${F.n2(p.litres)} L</td>
      <td><span class="badge badge-brand">${INV.pct(p.litres,p.fuel)}</span></td>
      <td>${F.money(p.ppl)}</td>
      <td class="fw-700 text-brand">${F.money(p.total)}</td>
      <td><div class="row-gap">
        <button class="btn btn-info btn-xs" data-ac="rcp" data-id="${p.id}">Receipt</button>
        <button class="btn btn-ghost btn-xs" data-ac="ep" data-id="${p.id}">Edit</button>
        <button class="btn btn-danger btn-xs" data-ac="dp" data-id="${p.id}">Del</button>
      </div></td>
    </tr>`; }).join('');
  }

  setFuel(fuel){
    document.getElementById('r-fuel').value=fuel;
    document.getElementById('ft-p').className='ft-btn'+(fuel==='Petrol'?' p-active':'');
    document.getElementById('ft-d').className='ft-btn'+(fuel==='Diesel'?' d-active':'');
    document.getElementById('pv-fuel').textContent=fuel;
    document.getElementById('pv-fuel').className='preview-item-val '+(fuel==='Petrol'?'petrol-color':'diesel-color');
    this._calcTotal(); this._updatePreview(); this._chkInvWarn();
  }

  _calcTotal(){
    const l=+document.getElementById('r-litres').value||0, p=+document.getElementById('r-ppl').value||0, t=l&&p?l*p:0;
    document.getElementById('r-total').value=t?F.money(t):'';
  }

  _updatePreview(){
    const fuel=document.getElementById('r-fuel').value;
    const l=+document.getElementById('r-litres').value||0;
    const p=+document.getElementById('r-ppl').value||0;
    const cid=document.getElementById('r-cust').value, cm=this._cmap(), c=cm[cid];
    document.getElementById('pv-cust').textContent=c?c.name:'—';
    document.getElementById('pv-date').textContent=F.date(document.getElementById('r-date').value)||'—';
    document.getElementById('pv-litres').textContent=l?F.n2(l)+' L':'—';
    document.getElementById('pv-ppl').textContent=p?F.money(p):'—';
    document.getElementById('pv-total').textContent=l&&p?F.money(l*p):'—';
    // % of total inventory
    const inv=INV.totals(), total=(fuel==='Petrol'?inv.petrolTotal:inv.dieselTotal)||0;
    if(total>0&&l>0){
      const pct=(l/total*100);
      document.getElementById('pv-pct').textContent=pct.toFixed(2)+'%';
      document.getElementById('pv-pct').className='preview-item-val '+(fuel==='Petrol'?'petrol-color':'diesel-color');
      const remain=INV.remain(fuel);
      document.getElementById('pv-remain').textContent=F.n2(remain)+' L';
    } else {
      document.getElementById('pv-pct').textContent='—';
      document.getElementById('pv-remain').textContent='—';
    }
  }

  clearForm(){
    document.getElementById('rec-form').reset(); this.setFuel('Petrol');
    document.getElementById('r-date').value=F.today();
    ['pv-cust','pv-date','pv-litres','pv-ppl','pv-total','pv-pct','pv-remain'].forEach(id=>document.getElementById(id).textContent='—');
    document.getElementById('rec-err').classList.remove('show'); document.getElementById('inv-warn').style.display='none';
  }

  saveRecord(){
    const cid=document.getElementById('r-cust').value, date=document.getElementById('r-date').value;
    const fuel=document.getElementById('r-fuel').value, litres=+document.getElementById('r-litres').value, ppl=+document.getElementById('r-ppl').value;
    const notes=document.getElementById('r-notes').value.trim();
    if(!cid||!date||!litres||!ppl||litres<=0||ppl<=0){ document.getElementById('rec-err').classList.add('show'); return; }
    document.getElementById('rec-err').classList.remove('show');
    const ps=DB.get('_purchases',[]);
    const inv=INV.totals(), total=(fuel==='Petrol'?inv.petrolTotal:inv.dieselTotal)||0;
    const stockPct=total?F.pct(litres,total):'N/A';
    const rec={id:F.id(),cid,date,fuel,litres,ppl,total:litres*ppl,notes,stockPct,savedAt:F.ts()};
    ps.push(rec); DB.set('_purchases',ps);
    const cm=this._cmap();
    this._audit('create',`Sale: ${F.n2(litres)} L ${fuel} for ${cm[cid]?.name||cid}`,`${F.money(litres*ppl)} · ${stockPct} of stock`);
    // SMS: sale notification
    this._notifySMS('sale',`Customer: ${cm[cid]?.name||cid}\nFuel: ${fuel} · ${F.n2(litres)} L\nAmount: ${F.money(litres*ppl)}\nStock used: ${stockPct}`);
    // SMS: low stock check
    const remain=INV.remain(fuel);
    if(remain>0&&remain<300) this._notifySMS('low',`${fuel} stock is low: only ${F.n2(remain)} L remaining!`);
    this.clearForm(); this._renderRecent(); this.renderRecord();
    this.toast(`Saved — ${F.n2(litres)} L ${fuel} (${stockPct} of stock)`);
    setTimeout(()=>this.showReceipt(rec.id),400);
  }

  /* ── RECEIPT ── */
  showReceipt(id){
    const ps=DB.get('_purchases',[]); const p=ps.find(x=>x.id===id); if(!p) return;
    const cm=this._cmap(); const c=cm[p.cid]||{name:'Unknown',car:'—',phone:'—'};
    const inv=INV.totals(), total=(p.fuel==='Petrol'?inv.petrolTotal:inv.dieselTotal)||0;
    const stockPct=total?F.pct(p.litres,total):(p.stockPct||'N/A');
    this._rcpt={p,c};
    document.getElementById('receipt-content').innerHTML=`
      <div class="receipt">
        <div class="receipt-logo">
          <div class="receipt-logo-name">Emmanuel's Fuel Tracking System</div>
          <div class="receipt-logo-sub">Official Transaction Receipt</div>
        </div>
        <hr class="receipt-hr"/>
        <div class="rrow"><span class="rkey">Receipt #</span><span class="rval">${p.id.toUpperCase().slice(-8)}</span></div>
        <div class="rrow"><span class="rkey">Date</span><span class="rval">${F.date(p.date)}</span></div>
        <div class="rrow"><span class="rkey">Printed</span><span class="rval" style="font-size:.72rem">${F.ts()}</span></div>
        <hr class="receipt-hr"/>
        <div class="rrow"><span class="rkey">Customer</span><span class="rval">${F.esc(c.name)}</span></div>
        <div class="rrow"><span class="rkey">Vehicle</span><span class="rval">${F.esc(c.car)}</span></div>
        <div class="rrow"><span class="rkey">Phone</span><span class="rval">${F.esc(c.phone||'—')}</span></div>
        <hr class="receipt-hr"/>
        <div class="rrow"><span class="rkey">Fuel Type</span><span class="rval"><strong>${p.fuel}</strong></span></div>
        <div class="rrow"><span class="rkey">Quantity</span><span class="rval">${F.n2(p.litres)} Litres</span></div>
        <div class="rrow"><span class="rkey">Unit Price</span><span class="rval">${F.money(p.ppl)}/L</span></div>
        <hr class="receipt-hr"/>
        <div class="rtotal"><span class="rkey">TOTAL AMOUNT</span><span class="rval">${F.money(p.total)}</span></div>
        <div class="receipt-pct-row">
          <span class="rkey">% of Total ${p.fuel} Stock</span>
          <span class="receipt-pct-badge" style="${p.fuel==='Diesel'?'color:var(--diesel);background:var(--diesel-bg);border-color:var(--diesel-bdr)':''}">${stockPct}</span>
        </div>
        ${p.notes?`<hr class="receipt-hr"/><div style="font-size:.72rem;color:var(--t3);padding:.1rem 0">Note: ${F.esc(p.notes)}</div>`:''}
        <div class="receipt-footer"><div>Thank you for your business!</div><div style="margin-top:.2rem;font-weight:600">Emmanuel's Tracking System</div></div>
      </div>`;
    document.getElementById('m-receipt').style.display='flex';
  }

  dlReceipt(){ if(!this._rcpt) return; const {p,c}=this._rcpt; const inv=INV.totals(),total=(p.fuel==='Petrol'?inv.petrolTotal:inv.dieselTotal)||0; this._csv([['Receipt','Date','Customer','Vehicle','Fuel','Litres','% of Stock','Price/L','Total'],[p.id.slice(-8),p.date,c.name,c.car,p.fuel,p.litres,total?F.pct(p.litres,total):'N/A',p.ppl,p.total.toFixed(2)]],`receipt_${p.id.slice(-8)}`); }

  _receiptHTML(){
    if(!this._rcpt) return '';
    const {p, c} = this._rcpt;
    const inv = INV.totals();
    const total = (p.fuel==='Petrol' ? inv.petrolTotal : inv.dieselTotal) || 0;
    const stockPct = total ? F.pct(p.litres, total) : (p.stockPct||'N/A');
    const prefs = DB.get('_prefs', {currency:'GHS'});
    const curLabel = prefs.currency || 'GHS';
    const amtWords = (n=>{
      const a=['','One','Two','Three','Four','Five','Six','Seven','Eight','Nine','Ten','Eleven','Twelve','Thirteen','Fourteen','Fifteen','Sixteen','Seventeen','Eighteen','Nineteen'];
      const b=['','','Twenty','Thirty','Forty','Fifty','Sixty','Seventy','Eighty','Ninety'];
      const t=Math.floor(n); const dec=Math.round((n-t)*100);
      const w=t<20?a[t]:t<100?b[Math.floor(t/10)]+(t%10?' '+a[t%10]:''):t<1000?a[Math.floor(t/100)]+' Hundred'+(t%100?' '+(t%100<20?a[t%100]:b[Math.floor(t%100/10)]+(t%100%10?' '+a[t%100%10]:'')):''):a[Math.floor(t/1000)]+' Thousand'+(t%1000?' '+(t%1000<20?a[t%1000]:b[Math.floor(t%1000/10)]+(t%1000%10?' '+a[t%1000%10]:'')):'');
      return (w||'Zero')+(dec>0?` and ${dec}/100`:' Only');
    })(p.total);
    const fuelColor = p.fuel==='Diesel' ? '#1d4ed8' : '#ea580c';
    const fuelBg    = p.fuel==='Diesel' ? '#eff6ff' : '#fff7ed';
    const receiptNo = p.id.toUpperCase().slice(-8);
    const dateStr   = new Date(p.date+'T00:00:00').toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'long',year:'numeric'});
    const timeStr   = new Date().toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
    return `<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>Receipt ${receiptNo}</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,'Segoe UI',sans-serif;background:#f4f4f5;color:#111;padding:32px 16px;display:flex;justify-content:center}
.page{background:#fff;width:100%;max-width:560px;border-radius:12px;overflow:hidden;box-shadow:0 4px 32px rgba(0,0,0,.12)}
.hd{background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);padding:28px 32px;position:relative;overflow:hidden}
.hd::before{content:'';position:absolute;top:-60px;right:-60px;width:220px;height:220px;background:radial-gradient(circle,rgba(249,115,22,.3) 0%,transparent 65%)}
.hd-inner{position:relative;z-index:1;display:flex;justify-content:space-between;align-items:flex-start}
.hd-logo{display:flex;align-items:center;gap:10px;margin-bottom:4px}
.hd-logo-icon{width:36px;height:36px;background:#f97316;border-radius:9px;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.hd-logo-icon svg{width:17px;height:17px;stroke:#fff;fill:none;stroke-width:2.2;stroke-linecap:round}
.hd-name{font-size:1rem;font-weight:800;color:#fff;letter-spacing:-.03em}
.hd-sub{font-size:.67rem;color:rgba(255,255,255,.45);margin-top:2px}
.hd-badge{background:rgba(249,115,22,.15);border:1px solid rgba(249,115,22,.35);border-radius:8px;padding:10px 14px;text-align:right;flex-shrink:0}
.hd-badge-label{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:rgba(255,255,255,.45);margin-bottom:3px}
.hd-badge-no{font-size:1rem;font-weight:900;color:#f97316;letter-spacing:.04em}
.hd-badge-type{font-size:.62rem;color:rgba(255,255,255,.45);margin-top:3px}
.body{padding:24px 28px}
.fuel-banner{display:flex;align-items:center;justify-content:space-between;background:${fuelBg};border:1.5px solid ${fuelColor}33;border-radius:10px;padding:12px 16px;margin-bottom:20px}
.fuel-left{display:flex;align-items:center;gap:10px}
.fuel-dot{width:10px;height:10px;border-radius:50%;background:${fuelColor};flex-shrink:0}
.fuel-name{font-size:.88rem;font-weight:700;color:${fuelColor}}
.fuel-desc{font-size:.67rem;color:#888;margin-top:1px}
.fuel-qty{font-size:1.3rem;font-weight:900;color:${fuelColor}}
.fuel-qty-unit{font-size:.68rem;color:#888;margin-left:2px}
.sec-label{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#aaa;margin:18px 0 8px}
.rows{border:1px solid #e5e7eb;border-radius:10px;overflow:hidden}
.row{display:flex;justify-content:space-between;align-items:center;padding:9px 14px;border-bottom:1px solid #f3f4f6;font-size:.75rem}
.row:last-child{border-bottom:none}
.row-alt{background:#fafafa}
.rk{color:#6b7280;font-weight:500}
.rv{font-weight:600;color:#111;text-align:right;max-width:58%}
.total-block{background:linear-gradient(135deg,#1a1a2e 0%,#111827 100%);border-radius:10px;padding:16px 20px;margin-top:14px;display:flex;align-items:center;justify-content:space-between}
.total-label{font-size:.67rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:rgba(255,255,255,.5)}
.total-amount{font-size:1.5rem;font-weight:900;color:#fff;letter-spacing:-.04em;margin-top:2px}
.total-cur{font-size:1rem;font-weight:800;color:#f97316}
.total-sub{font-size:.63rem;color:rgba(255,255,255,.35);margin-top:3px;text-align:right}
.words-block{background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;padding:9px 14px;margin-top:8px;display:flex;gap:8px}
.words-label{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#9ca3af;white-space:nowrap;margin-top:1px}
.words-val{font-size:.71rem;font-style:italic;color:#374151;font-weight:500;line-height:1.5}
.pct-block{display:flex;align-items:center;justify-content:space-between;padding:9px 14px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;margin-top:8px}
.pct-label{font-size:.72rem;color:#6b7280;font-weight:500}
.pct-badge{background:${fuelBg};color:${fuelColor};border:1px solid ${fuelColor}44;border-radius:99px;padding:3px 10px;font-size:.68rem;font-weight:700}
.notes-block{margin-top:8px;padding:9px 14px;background:#fffbeb;border:1px solid #fde68a;border-radius:8px}
.notes-label{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#92400e;margin-bottom:4px}
.notes-val{font-size:.72rem;color:#78350f;line-height:1.5}
.sig-row{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:24px}
.sig-box{border-top:1.5px solid #e5e7eb;padding-top:8px}
.sig-label{font-size:.62rem;color:#9ca3af;font-weight:500;text-align:center}
.ft{background:#f9fafb;border-top:1px solid #e5e7eb;padding:14px 28px;display:flex;align-items:center;justify-content:space-between}
.ft-brand{font-size:.7rem;font-weight:700;color:#374151}
.ft-msg{font-size:.66rem;color:#9ca3af;margin-top:1px}
.ft-dots{display:flex;gap:4px}
.ft-dot{width:5px;height:5px;border-radius:50%;background:#e5e7eb}
.ft-dot.on{background:#f97316}
@media print{
  *{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important}
  body{background:#fff!important;padding:0!important}
  .page{box-shadow:none!important;border-radius:0!important;max-width:100%!important}
  @page{margin:0;size:A4}
}
</style></head><body>
<div class="page">
  <div class="hd">
    <div class="hd-inner">
      <div>
        <div class="hd-logo">
          <div class="hd-logo-icon"><svg viewBox="0 0 24 24"><path d="M12 2C12 2 5 9 5 14a7 7 0 0 0 14 0c0-5-7-12-7-12z"/></svg></div>
          <div>
            <div class="hd-name">Emmanuel's Fuel Tracking System</div>
            <div class="hd-sub">Official Transaction Receipt</div>
          </div>
        </div>
      </div>
      <div class="hd-badge">
        <div class="hd-badge-label">Receipt No.</div>
        <div class="hd-badge-no">#${receiptNo}</div>
        <div class="hd-badge-type">FUEL SALE</div>
      </div>
    </div>
  </div>
  <div class="body">
    <div class="fuel-banner">
      <div class="fuel-left">
        <div class="fuel-dot"></div>
        <div><div class="fuel-name">${p.fuel}</div><div class="fuel-desc">Fuel Type</div></div>
      </div>
      <div><span class="fuel-qty">${parseFloat(p.litres).toFixed(2)}</span><span class="fuel-qty-unit">Litres</span></div>
    </div>
    <div class="sec-label">Transaction Details</div>
    <div class="rows">
      <div class="row row-alt"><span class="rk">Date of Sale</span><span class="rv">${dateStr}</span></div>
      <div class="row"><span class="rk">Issued At</span><span class="rv">${timeStr}</span></div>
      <div class="row row-alt"><span class="rk">Unit Price</span><span class="rv">${curLabel} ${parseFloat(p.ppl).toFixed(2)} / Litre</span></div>
      <div class="row"><span class="rk">Quantity</span><span class="rv">${parseFloat(p.litres).toFixed(2)} L</span></div>
    </div>
    <div class="sec-label">Customer Information</div>
    <div class="rows">
      <div class="row row-alt"><span class="rk">Full Name</span><span class="rv">${c.name||'—'}</span></div>
      <div class="row"><span class="rk">Vehicle / Plate</span><span class="rv">${c.car||'—'}</span></div>
      <div class="row row-alt"><span class="rk">Phone</span><span class="rv">${c.phone||'—'}</span></div>
    </div>
    <div class="total-block">
      <div><div class="total-label">Total Amount</div><div class="total-amount">${parseFloat(p.total).toFixed(2)}</div></div>
      <div style="text-align:right"><div class="total-cur">${curLabel}</div><div class="total-sub">${p.litres} L × ${p.ppl}</div></div>
    </div>
    <div class="words-block">
      <span class="words-label">In Words</span>
      <span class="words-val">${curLabel} ${amtWords}</span>
    </div>
    <div class="pct-block">
      <span class="pct-label">% of Total ${p.fuel} Stock Used</span>
      <span class="pct-badge">${stockPct}</span>
    </div>
    ${p.notes?`<div class="notes-block"><div class="notes-label">Note</div><div class="notes-val">${p.notes}</div></div>`:''}
    <div class="sig-row">
      <div class="sig-box"><div class="sig-label">Authorised By</div></div>
      <div class="sig-box"><div class="sig-label">Customer Signature</div></div>
    </div>
  </div>
  <div class="ft">
    <div><div class="ft-brand">Emmanuel's Fuel Tracking System</div><div class="ft-msg">Thank you for your business</div></div>
    <div class="ft-dots"><div class="ft-dot on"></div><div class="ft-dot"></div><div class="ft-dot"></div></div>
  </div>
</div>
</body></html>`;
  }

  printReceipt(){
    if(!this._rcpt) return;
    const html = this._receiptHTML();
    // Use hidden iframe to avoid popup blockers
    let ifr = document.getElementById('_print_frame');
    if(!ifr){ ifr=document.createElement('iframe'); ifr.id='_print_frame'; ifr.style.cssText='position:fixed;top:-9999px;left:-9999px;width:800px;height:600px;border:none'; document.body.appendChild(ifr); }
    const doc = ifr.contentDocument || ifr.contentWindow.document;
    doc.open(); doc.write(html); doc.close();
    setTimeout(()=>{ try{ ifr.contentWindow.focus(); ifr.contentWindow.print(); }catch(e){ this.toast('Print failed — try Download PDF instead','error'); } }, 600);
  }

  dlReceiptPDF(){
    if(!this._rcpt) return;
    const html = this._receiptHTML();
    const receiptNo = this._rcpt.p.id.toUpperCase().slice(-8);
    const blob = new Blob([html], {type:'text/html'});
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = `Receipt_${receiptNo}.html`;
    a.click(); URL.revokeObjectURL(url);
    this.toast('Receipt downloaded — open in browser and Save as PDF');
  }
  /* ── EDIT PURCHASE ── */


  /* ── EDIT PURCHASE ── */
  openEP(id){
    const p=DB.get('_purchases',[]).find(x=>x.id===id); if(!p) return;
    document.getElementById('ep-id').value=id; document.getElementById('ep-date').value=p.date; document.getElementById('ep-litres').value=p.litres; document.getElementById('ep-ppl').value=p.ppl; document.getElementById('ep-total').value=F.money(p.total); document.getElementById('ep-notes').value=p.notes||''; document.getElementById('m-ep').style.display='flex';
  }
  saveEP(){
    const id=document.getElementById('ep-id').value, litres=+document.getElementById('ep-litres').value, ppl=+document.getElementById('ep-ppl').value, date=document.getElementById('ep-date').value, notes=document.getElementById('ep-notes').value.trim();
    if(!litres||!ppl||!date) return;
    let ps=DB.get('_purchases',[]); ps=ps.map(p=>p.id===id?{...p,date,litres,ppl,total:litres*ppl,notes}:p);
    DB.set('_purchases',ps); this._audit('edit',`Edited purchase`,`${litres} L @ ${F.money(ppl)}`);
    this.closeModal('m-ep'); if(this.page==='record') this._renderRecent(); if(this.page==='monthly') this.renderMonthly(); if(this.page==='history') this.renderHist(); this.toast('Purchase updated');
  }
  delPurchase(id){
    let ps=DB.get('_purchases',[]); const p=ps.find(x=>x.id===id); ps=ps.filter(x=>x.id!==id); DB.set('_purchases',ps);
    this._audit('delete',`Deleted purchase`,p?`${p.litres} L ${p.fuel}`:'');
    if(this.page==='record') this._renderRecent(); if(this.page==='monthly') this.renderMonthly(); if(this.page==='history') this.renderHist(); this.toast('Deleted','error');
  }

  /* ── MONTHLY ── */
  renderMonthly(){
    const mo=+document.getElementById('m-month').value, yr=+document.getElementById('m-year').value;
    const ps=DB.get('_purchases',[]).filter(p=>{ const d=new Date(p.date+'T00:00:00'); return d.getMonth()===mo&&d.getFullYear()===yr; });
    const cm=this._cmap(), pL=ps.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0), dL=ps.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0), tL=pL+dL, rev=ps.reduce((s,p)=>s+p.total,0);
    document.getElementById('m-kpis').innerHTML=this._kpi('Revenue',F.money(rev),'kpi-icon','background:var(--brand-lt);color:var(--brand)',`${ps.length} sales`,'var(--brand)',100,'<path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>')+this._kpi('Total Fuel',F.n2(tL)+' L','kpi-icon','background:var(--petrol-bg);color:var(--petrol)','Combined','var(--petrol)',F.pctN(pL,tL),'<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>')+this._kpi('Petrol',F.n2(pL)+' L','kpi-icon','background:var(--petrol-bg);color:var(--petrol)',F.pct(pL,tL),'var(--petrol)',F.pctN(pL,tL),'<circle cx="12" cy="12" r="10"/>')+this._kpi('Diesel',F.n2(dL)+' L','kpi-icon','background:var(--diesel-bg);color:var(--diesel)',F.pct(dL,tL),'var(--diesel)',F.pctN(dL,tL),'<rect x="2" y="3" width="20" height="14" rx="2"/>');
    this._donut('m-donut',pL,dL); document.getElementById('m-dval').textContent=F.n2(tL)+'L'; this._dlegend('m-dleg',pL,dL,tL); this._trend('m-trend',ps,mo,yr);
    const tb=document.getElementById('m-tbody');
    if(!ps.length){ tb.innerHTML=`<tr><td colspan="9" class="empty-row">No purchases this month.</td></tr>`; }
    else tb.innerHTML=ps.slice().sort((a,b)=>a.date.localeCompare(b.date)).map(p=>{ const c=cm[p.cid]||{name:'?',car:'—'}; return `<tr>
      <td>${F.date(p.date)}</td><td class="fw-600">${F.esc(c.name)}</td>
      <td><span style="font-family:monospace;font-size:.78rem">${F.esc(c.car)}</span></td>
      <td><span class="badge badge-${p.fuel.toLowerCase()}">${p.fuel}</span></td>
      <td>${F.n2(p.litres)} L</td>
      <td><span class="badge badge-brand">${INV.pct(p.litres,p.fuel)}</span></td>
      <td>${F.money(p.ppl)}</td>
      <td class="fw-700 text-brand">${F.money(p.total)}</td>
      <td><button class="btn btn-info btn-xs" data-ac="rcp" data-id="${p.id}">Receipt</button></td>
    </tr>`; }).join('');
    const csumm={};
    ps.forEach(p=>{ if(!csumm[p.cid]) csumm[p.cid]={p:0,d:0,rev:0,visits:0}; if(p.fuel==='Petrol') csumm[p.cid].p+=p.litres; else csumm[p.cid].d+=p.litres; csumm[p.cid].rev+=p.total; csumm[p.cid].visits++; });
    const cs=document.getElementById('m-csumm'), entries=Object.entries(csumm).sort((a,b)=>b[1].rev-a[1].rev);
    if(!entries.length) cs.innerHTML=`<tr><td colspan="9" class="empty-row">No data.</td></tr>`;
    else cs.innerHTML=entries.map(([cid,d])=>{ const c=cm[cid]||{name:'?',car:'—'}; const tot=d.p+d.d; return `<tr>
      <td class="fw-600">${F.esc(c.name)}</td><td><span style="font-family:monospace;font-size:.78rem">${F.esc(c.car)}</span></td>
      <td>${F.n2(d.p)} L</td><td><span class="badge badge-petrol">${F.pct(d.p,tL)}</span></td>
      <td>${F.n2(d.d)} L</td><td><span class="badge badge-diesel">${F.pct(d.d,tL)}</span></td>
      <td class="fw-700">${F.n2(tot)} L</td><td class="fw-700 text-brand">${F.money(d.rev)}</td><td>${d.visits}</td>
    </tr>`; }).join('');
  }

  // ── SHARED EXCEL HELPER ──
  _xlsxDownload(rows, summaryRows, sheetName, filename){
    const WB = XLSX.utils.book_new();

    // ── Main data sheet ──
    const ws = XLSX.utils.aoa_to_sheet(rows);
    const hdrStyle = { font:{bold:true,color:{rgb:'FFFFFF'},name:'Arial',sz:10}, fill:{fgColor:{rgb:'1A1A2E'}}, alignment:{horizontal:'center',vertical:'center',wrapText:true}, border:{bottom:{style:'medium',color:{rgb:'F97316'}}} };
    const altStyle = { fill:{fgColor:{rgb:'F9FAFB'}}, font:{name:'Arial',sz:9}, alignment:{horizontal:'left',vertical:'center'} };
    const normStyle = { fill:{fgColor:{rgb:'FFFFFF'}}, font:{name:'Arial',sz:9}, alignment:{horizontal:'left',vertical:'center'} };
    const numStyle  = { fill:{fgColor:{rgb:'FFFFFF'}}, font:{name:'Arial',sz:9}, alignment:{horizontal:'right',vertical:'center'}, numFmt:'#,##0.00' };
    const totalStyle= { font:{bold:true,name:'Arial',sz:10,color:{rgb:'1A1A2E'}}, fill:{fgColor:{rgb:'FFF7ED'}}, border:{top:{style:'medium',color:{rgb:'F97316'}}}, alignment:{horizontal:'right',vertical:'center'} };

    const range = XLSX.utils.decode_range(ws['!ref']||'A1');
    for(let R=range.s.r; R<=range.e.r; R++){
      for(let C=range.s.c; C<=range.e.c; C++){
        const addr = XLSX.utils.encode_cell({r:R,c:C});
        if(!ws[addr]) ws[addr]={t:'z',v:''};
        if(R===0){ ws[addr].s = hdrStyle; }
        else { ws[addr].s = (C>=4 && C<=7) ? numStyle : (R%2===0 ? altStyle : normStyle); }
      }
    }

    // Summary sheet
    if(summaryRows && summaryRows.length){
      const ws2 = XLSX.utils.aoa_to_sheet(summaryRows);
      const sr = XLSX.utils.decode_range(ws2['!ref']||'A1');
      for(let R=sr.s.r; R<=sr.e.r; R++){
        for(let C=sr.s.c; C<=sr.e.c; C++){
          const addr=XLSX.utils.encode_cell({r:R,c:C});
          if(!ws2[addr]) ws2[addr]={t:'z',v:''};
          ws2[addr].s = R===0 ? hdrStyle : (R===summaryRows.length-1 ? totalStyle : (R%2===0?altStyle:normStyle));
        }
      }
      ws2['!cols']=[{wch:28},{wch:16},{wch:16},{wch:16},{wch:16}];
      XLSX.utils.book_append_sheet(WB, ws2, 'Summary');
    }

    // Auto column widths
    const cols = rows[0]?.map((_,ci)=>({ wch: Math.min(36, Math.max(12, ...rows.map(r=>String(r[ci]||'').length))) })) || [];
    ws['!cols'] = cols;
    ws['!rows'] = [{hpt:22}]; // header row height

    XLSX.utils.book_append_sheet(WB, ws, sheetName);
    XLSX.writeFile(WB, filename+'.xlsx');
  }

  // ── SHARED PDF REPORT HELPER ──
  _reportPDF(title, subtitle, columns, rows, summaryItems, filename){
    const prefs = DB.get('_prefs',{currency:'GHS'});
    const cur = prefs.currency||'GHS';
    const now = new Date().toLocaleString('en-GB',{day:'2-digit',month:'long',year:'numeric',hour:'2-digit',minute:'2-digit'});

    const tableRows = rows.map((r,i)=>`
      <tr style="background:${i%2===0?'#ffffff':'#f9fafb'}">
        ${r.map((v,ci)=>`<td style="padding:7px 10px;font-size:.72rem;color:#374151;border-bottom:1px solid #f3f4f6;text-align:${ci>=4&&ci<=7?'right':'left'}">${v}</td>`).join('')}
      </tr>`).join('');

    const summaryHTML = summaryItems.map(s=>`
      <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px 18px;flex:1;min-width:120px">
        <div style="font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#9ca3af;margin-bottom:4px">${s.label}</div>
        <div style="font-size:1.1rem;font-weight:900;color:#111;letter-spacing:-.03em">${s.value}</div>
        ${s.sub?`<div style="font-size:.68rem;color:#9ca3af;margin-top:2px">${s.sub}</div>`:''}
      </div>`).join('');

    const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>${title}</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:system-ui,-apple-system,'Segoe UI',sans-serif;background:#f4f4f5;padding:32px 20px;color:#111}
  .page{background:#fff;max-width:900px;margin:0 auto;border-radius:12px;overflow:hidden;box-shadow:0 4px 32px rgba(0,0,0,.1)}
  .hd{background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);padding:28px 36px;position:relative;overflow:hidden}
  .hd::before{content:'';position:absolute;top:-60px;right:-60px;width:220px;height:220px;background:radial-gradient(circle,rgba(249,115,22,.3) 0%,transparent 65%)}
  .hd-inner{position:relative;z-index:1;display:flex;justify-content:space-between;align-items:flex-start}
  .hd-logo{display:flex;align-items:center;gap:10px}
  .hd-icon{width:36px;height:36px;background:#f97316;border-radius:9px;display:flex;align-items:center;justify-content:center;flex-shrink:0}
  .hd-icon svg{width:17px;height:17px;stroke:#fff;fill:none;stroke-width:2.2;stroke-linecap:round}
  .hd-name{font-size:1rem;font-weight:800;color:#fff;letter-spacing:-.03em}
  .hd-sub{font-size:.67rem;color:rgba(255,255,255,.45);margin-top:2px}
  .hd-meta{text-align:right}
  .hd-title{font-size:.75rem;font-weight:700;color:#f97316;text-transform:uppercase;letter-spacing:.06em}
  .hd-date{font-size:.65rem;color:rgba(255,255,255,.45);margin-top:3px}
  .summary{display:flex;gap:12px;flex-wrap:wrap;padding:20px 36px;background:#f9fafb;border-bottom:1px solid #e5e7eb}
  .body{padding:0 36px 28px}
  table{width:100%;border-collapse:collapse;margin-top:20px}
  thead tr{background:#1a1a2e}
  thead th{padding:10px 10px;font-size:.68rem;font-weight:700;color:#fff;text-align:left;text-transform:uppercase;letter-spacing:.06em;border-bottom:3px solid #f97316;white-space:nowrap}
  thead th:nth-child(n+5){text-align:right}
  .ft{background:#f9fafb;border-top:1px solid #e5e7eb;padding:14px 36px;display:flex;justify-content:space-between;align-items:center}
  .ft-brand{font-size:.7rem;font-weight:700;color:#374151}
  .ft-msg{font-size:.65rem;color:#9ca3af}
  @media print{
    *{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important}
    body{background:#fff!important;padding:0!important}
    .page{box-shadow:none!important;border-radius:0!important;max-width:100%!important}
    @page{margin:0;size:A4 landscape}
  }
</style></head><body>
<div class="page">
  <div class="hd">
    <div class="hd-inner">
      <div class="hd-logo">
        <div class="hd-icon"><svg viewBox="0 0 24 24"><path d="M12 2C12 2 5 9 5 14a7 7 0 0 0 14 0c0-5-7-12-7-12z"/></svg></div>
        <div><div class="hd-name">Emmanuel's Fuel Tracking System</div><div class="hd-sub">${subtitle}</div></div>
      </div>
      <div class="hd-meta">
        <div class="hd-title">${title}</div>
        <div class="hd-date">Generated: ${now}</div>
      </div>
    </div>
  </div>
  <div class="summary">${summaryHTML}</div>
  <div class="body">
    <table>
      <thead><tr>${columns.map(c=>`<th>${c}</th>`).join('')}</tr></thead>
      <tbody>${tableRows}</tbody>
    </table>
  </div>
  <div class="ft">
    <div><div class="ft-brand">Emmanuel's Fuel Tracking System</div><div class="ft-msg">Confidential — For internal use only</div></div>
    <div style="font-size:.65rem;color:#9ca3af">Total records: ${rows.length}</div>
  </div>
</div>
<${'script'}>
  window.onload=function(){setTimeout(function(){window.print();},400);window.onafterprint=function(){window.close();};};
<${'/'+'script'}>
</body></html>`;

    // Use hidden iframe — no popup blocker
    let ifr=document.getElementById('_print_frame');
    if(!ifr){ifr=document.createElement('iframe');ifr.id='_print_frame';ifr.style.cssText='position:fixed;top:-9999px;left:-9999px;width:1100px;height:800px;border:none';document.body.appendChild(ifr);}
    const doc=ifr.contentDocument||ifr.contentWindow.document;
    doc.open();doc.write(html);doc.close();
    setTimeout(()=>{try{ifr.contentWindow.focus();ifr.contentWindow.print();}catch(e){
      // Fallback: download as HTML for manual print-to-PDF
      const blob=new Blob([html],{type:'text/html'});
      const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=filename+'.html';a.click();
      this.toast('Saved as HTML — open and print to PDF');
    }},700);
  }

  expMonthly(){
    const mo=+document.getElementById('m-month').value, yr=+document.getElementById('m-year').value;
    const ps=DB.get('_purchases',[]).filter(p=>{ const d=new Date(p.date+'T00:00:00'); return d.getMonth()===mo&&d.getFullYear()===yr; });
    const cm=this._cmap();
    const prefs=DB.get('_prefs',{currency:'GHS'}); const cur=prefs.currency||'GHS';
    const sorted=ps.slice().sort((a,b)=>a.date.localeCompare(b.date));

    // Summary
    const totLitres=sorted.reduce((s,p)=>s+p.litres,0);
    const totRev=sorted.reduce((s,p)=>s+p.total,0);
    const petrolL=sorted.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const dieselL=sorted.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);

    const dataRows=[['Date','Customer','Vehicle','Fuel Type','Litres','Unit Price ('+cur+')','Total ('+cur+')','Notes']];
    sorted.forEach((p,i)=>{ const c=cm[p.cid]||{name:'?',car:'?'}; dataRows.push([p.date,c.name,c.car,p.fuel,p.litres,+p.ppl,+p.total.toFixed(2),p.notes||'']); });
    // Totals row
    dataRows.push(['','','','TOTAL',totLitres,'',+totRev.toFixed(2),'']);

    const summaryRows=[
      ['Metric','Value'],
      ['Month',`${MF[mo]} ${yr}`],
      ['Total Transactions',sorted.length],
      ['Total Litres Sold',totLitres.toFixed(2)],
      ['Petrol Litres',petrolL.toFixed(2)],
      ['Diesel Litres',dieselL.toFixed(2)],
      ['Total Revenue ('+cur+')',totRev.toFixed(2)],
    ];

    this._xlsxDownload(dataRows, summaryRows, `${MF[mo]} ${yr}`, `Monthly_Report_${MF[mo]}_${yr}`);
    this._audit('create',`Exported monthly Excel: ${MF[mo]} ${yr}`);
  }

  expMonthlyPDF(){
    const mo=+document.getElementById('m-month').value, yr=+document.getElementById('m-year').value;
    const ps=DB.get('_purchases',[]).filter(p=>{ const d=new Date(p.date+'T00:00:00'); return d.getMonth()===mo&&d.getFullYear()===yr; });
    const cm=this._cmap();
    const prefs=DB.get('_prefs',{currency:'GHS'}); const cur=prefs.currency||'GHS';
    const sorted=ps.slice().sort((a,b)=>a.date.localeCompare(b.date));
    const totL=sorted.reduce((s,p)=>s+p.litres,0), totR=sorted.reduce((s,p)=>s+p.total,0);
    const pL=sorted.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const dL=sorted.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);

    const cols=['Date','Customer','Vehicle','Fuel','Litres','Unit Price','Total ('+cur+')','Notes'];
    const rows=sorted.map(p=>{ const c=cm[p.cid]||{name:'?',car:'?'}; return [p.date,c.name,c.car,p.fuel,p.litres.toFixed(2),cur+' '+p.ppl,cur+' '+p.total.toFixed(2),p.notes||'—']; });
    const summary=[
      {label:'Period',value:`${MF[mo]} ${yr}`},
      {label:'Transactions',value:sorted.length},
      {label:'Total Litres',value:totL.toFixed(2)+' L'},
      {label:'Petrol',value:pL.toFixed(2)+' L'},
      {label:'Diesel',value:dL.toFixed(2)+' L'},
      {label:'Total Revenue',value:cur+' '+totR.toFixed(2)},
    ];
    this._reportPDF(`${MF[mo]} ${yr} Monthly Report`,'Monthly Sales Report',cols,rows,summary,`Monthly_${MF[mo]}_${yr}`);
    this._audit('create',`Exported monthly PDF: ${MF[mo]} ${yr}`);
  }

  expHist(){
    const ps=DB.get('_purchases',[]); const cm=this._cmap();
    const prefs=DB.get('_prefs',{currency:'GHS'}); const cur=prefs.currency||'GHS';
    const sorted=ps.slice().sort((a,b)=>b.date.localeCompare(a.date));
    const totL=sorted.reduce((s,p)=>s+p.litres,0), totR=sorted.reduce((s,p)=>s+p.total,0);
    const pL=sorted.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const dL=sorted.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);

    const dataRows=[['Date','Customer','Vehicle','Phone','Fuel','Litres','Unit Price ('+cur+')','Total ('+cur+')','Notes']];
    sorted.forEach(p=>{ const c=cm[p.cid]||{name:'?',car:'?',phone:''}; dataRows.push([p.date,c.name,c.car,c.phone||'',p.fuel,p.litres,+p.ppl,+p.total.toFixed(2),p.notes||'']); });
    dataRows.push(['','','','','TOTAL',totL,'',+totR.toFixed(2),'']);

    const summaryRows=[
      ['Metric','Value'],
      ['Total Transactions',sorted.length],
      ['Total Litres Sold',totL.toFixed(2)],
      ['Petrol Litres',pL.toFixed(2)],
      ['Diesel Litres',dL.toFixed(2)],
      ['Total Revenue ('+cur+')',totR.toFixed(2)],
    ];
    this._xlsxDownload(dataRows, summaryRows, 'All Transactions', 'Purchase_History_All');
    this._audit('create','Exported full history Excel');
  }

  expHistPDF(){
    const ps=DB.get('_purchases',[]); const cm=this._cmap();
    const prefs=DB.get('_prefs',{currency:'GHS'}); const cur=prefs.currency||'GHS';
    const sorted=ps.slice().sort((a,b)=>b.date.localeCompare(a.date));
    const totL=sorted.reduce((s,p)=>s+p.litres,0), totR=sorted.reduce((s,p)=>s+p.total,0);
    const pL=sorted.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const dL=sorted.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);

    const cols=['Date','Customer','Vehicle','Phone','Fuel','Litres','Unit Price','Total ('+cur+')','Notes'];
    const rows=sorted.map(p=>{ const c=cm[p.cid]||{name:'?',car:'?',phone:''}; return [p.date,c.name,c.car,c.phone||'—',p.fuel,p.litres.toFixed(2),cur+' '+p.ppl,cur+' '+p.total.toFixed(2),p.notes||'—']; });
    const summary=[
      {label:'Total Records',value:sorted.length},
      {label:'Total Litres',value:totL.toFixed(2)+' L'},
      {label:'Petrol',value:pL.toFixed(2)+' L',sub:'Fuel type split'},
      {label:'Diesel',value:dL.toFixed(2)+' L',sub:'Fuel type split'},
      {label:'Total Revenue',value:cur+' '+totR.toFixed(2)},
    ];
    this._reportPDF('Complete Purchase History','All Transactions Report',cols,rows,summary,'Purchase_History_All');
    this._audit('create','Exported full history PDF');
  }

  /* ── YEARLY ── */
  renderYearly(){
    const yr=+document.getElementById('y-year').value, ps=DB.get('_purchases',[]).filter(p=>new Date(p.date+'T00:00:00').getFullYear()===yr);
    const pL=ps.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0), dL=ps.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0), tL=pL+dL, rev=ps.reduce((s,p)=>s+p.total,0), custs=[...new Set(ps.map(p=>p.cid))].length;
    // YoY delta vs previous year
    const prevPs=DB.get('_purchases',[]).filter(p=>new Date(p.date+'T00:00:00').getFullYear()===yr-1);
    const prevRev=prevPs.reduce((s,p)=>s+p.total,0);
    const prevTL=prevPs.reduce((s,p)=>s+p.litres,0);
    const prevCusts=[...new Set(prevPs.map(p=>p.cid))].length;
    const _delta=(cur,prev)=>{ if(!prev) return ''; const pct=((cur-prev)/prev*100); const up=pct>=0; return `<span style="font-size:.65rem;font-weight:600;color:${up?'var(--success)':'var(--danger)'};margin-left:.35rem">${up?'↑':'↓'}${Math.abs(pct).toFixed(1)}% vs ${yr-1}</span>`; };
    document.getElementById('y-kpis').innerHTML=this._kpi('Annual Revenue',F.money(rev)+_delta(rev,prevRev),'kpi-icon','background:var(--brand-lt);color:var(--brand)',`${ps.length} transactions`,'var(--brand)',100,'<path d="M12 2v20M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/>')+this._kpi('Total Fuel',F.n2(tL)+' L'+_delta(tL,prevTL),'kpi-icon','background:var(--petrol-bg);color:var(--petrol)','Combined total','var(--petrol)',F.pctN(pL,tL),'<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>')+this._kpi('Customers Served',custs+_delta(custs,prevCusts),'kpi-icon','background:var(--info-bg);color:var(--info)','Unique customers this year','var(--info)',0,'<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/>');
    const mData=MS.map((_,mo)=>{ const mps=ps.filter(p=>new Date(p.date+'T00:00:00').getMonth()===mo); return {p:mps.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0),d:mps.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0),rev:mps.reduce((s,p)=>s+p.total,0),count:mps.length}; });
    this._barChart('y-chart',mData);
    const tb=document.getElementById('y-tbody'), curM=new Date().getFullYear()===yr?new Date().getMonth():-1;
    tb.innerHTML=mData.map((m,i)=>{ const tot=m.p+m.d; return `<tr class="${i===curM?'month-hl':''}">
      <td class="fw-700">${MF[i]}</td><td>${F.n2(m.p)} L</td>
      <td><span class="badge badge-petrol">${F.pct(m.p,tL)}</span></td>
      <td>${F.n2(m.d)} L</td>
      <td><span class="badge badge-diesel">${F.pct(m.d,tL)}</span></td>
      <td class="fw-700">${F.n2(tot)} L</td>
      <td class="fw-700 text-brand">${F.money(m.rev)}</td>
      <td>${m.count}</td>
    </tr>`; }).join('');
    this._renderYoY();
  }

  _renderYoY(){
    const yrA=+document.getElementById('y-year').value;
    const yrBEl=document.getElementById('y-compare-year');
    const yrB=yrBEl?+yrBEl.value:0;
    const empty=document.getElementById('yoy-empty');
    const charts=document.getElementById('yoy-charts');
    if(!yrB||yrB===yrA){ if(empty) empty.style.display=''; if(charts) charts.style.display='none'; return; }
    if(empty) empty.style.display='none';
    if(charts) charts.style.display='';
    // Labels
    const lblA=document.getElementById('yoy-label-a'); if(lblA) lblA.textContent=yrA;
    const lblB=document.getElementById('yoy-label-b'); if(lblB) lblB.textContent=yrB;
    const thA=document.getElementById('yoy-th-a'); if(thA) thA.textContent=yrA;
    const thB=document.getElementById('yoy-th-b'); if(thB) thB.textContent=yrB;
    const sub=document.getElementById('yoy-subtitle'); if(sub) sub.textContent=`${yrA} vs ${yrB} — revenue, volume & growth`;
    const all=DB.get('_purchases',[]);
    const psA=all.filter(p=>new Date(p.date+'T00:00:00').getFullYear()===yrA);
    const psB=all.filter(p=>new Date(p.date+'T00:00:00').getFullYear()===yrB);
    const _monthly=(ps,field)=>MS.map((_,mo)=>{ const mps=ps.filter(p=>new Date(p.date+'T00:00:00').getMonth()===mo); return field==='rev'?mps.reduce((s,p)=>s+p.total,0):mps.reduce((s,p)=>s+p.litres,0); });
    const revA=_monthly(psA,'rev'), revB=_monthly(psB,'rev');
    const volA=_monthly(psA,'vol'), volB=_monthly(psB,'vol');
    this._yoyGroupedBar('yoy-rev-chart', revA, revB, yrA, yrB, true);
    this._yoyGroupedBar('yoy-vol-chart', volA, volB, yrA, yrB, false);
    // Summary table
    const totRevA=psA.reduce((s,p)=>s+p.total,0), totRevB=psB.reduce((s,p)=>s+p.total,0);
    const totVolA=psA.reduce((s,p)=>s+p.litres,0), totVolB=psB.reduce((s,p)=>s+p.litres,0);
    const custA=[...new Set(psA.map(p=>p.cid))].length, custB=[...new Set(psB.map(p=>p.cid))].length;
    const _chg=(a,b)=>{ if(!b) return '<span style="color:var(--t4)">—</span>'; const pct=(a-b)/b*100; const up=pct>=0; return `<span style="font-weight:600;color:${up?'var(--success)':'var(--danger)'}">${up?'↑':'↓'}${Math.abs(pct).toFixed(1)}%</span>`; };
    const tbody=document.getElementById('yoy-summary-tbody');
    if(tbody) tbody.innerHTML=[
      ['Total Revenue', F.money(totRevA), F.money(totRevB), _chg(totRevA,totRevB)],
      ['Total Fuel (L)', F.n2(totVolA)+' L', F.n2(totVolB)+' L', _chg(totVolA,totVolB)],
      ['Transactions', psA.length, psB.length, _chg(psA.length,psB.length)],
      ['Unique Customers', custA, custB, _chg(custA,custB)],
      ['Petrol (L)', F.n2(psA.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0))+' L', F.n2(psB.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0))+' L', _chg(psA.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0),psB.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0))],
      ['Diesel (L)', F.n2(psA.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0))+' L', F.n2(psB.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0))+' L', _chg(psA.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0),psB.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0))],
    ].map(r=>`<tr><td style="font-weight:500">${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td><td>${r[3]}</td></tr>`).join('');
  }

  _yoyGroupedBar(id, dataA, dataB, yrA, yrB, isRev){
    const canvas=document.getElementById(id); if(!canvas) return;
    const dpr=devicePixelRatio||1;
    const w=canvas.parentElement?.offsetWidth||canvas.offsetWidth||600;
    const h=200;
    canvas.width=w*dpr; canvas.height=h*dpr;
    canvas.style.width=w+'px'; canvas.style.height=h+'px';
    const ctx=canvas.getContext('2d'); ctx.scale(dpr,dpr); ctx.clearRect(0,0,w,h);
    const dark=document.documentElement.getAttribute('data-theme')==='dark';
    const tc=dark?'rgba(255,255,255,.25)':'rgba(0,0,0,.25)';
    const gc=dark?'rgba(255,255,255,.05)':'rgba(0,0,0,.05)';
    const pad={t:16,r:16,b:34,l:54};
    const cw=w-pad.l-pad.r, ch=h-pad.t-pad.b;
    const maxVal=Math.max(...dataA,...dataB,1);
    // Grid
    ctx.font='500 9px DM Sans,system-ui,sans-serif';
    for(let i=0;i<=4;i++){
      const y=pad.t+(ch/4)*i, v=maxVal-(maxVal/4)*i;
      ctx.beginPath(); ctx.strokeStyle=gc; ctx.lineWidth=1; ctx.setLineDash([3,4]);
      ctx.moveTo(pad.l,y); ctx.lineTo(pad.l+cw,y); ctx.stroke(); ctx.setLineDash([]);
      ctx.fillStyle=tc; ctx.textAlign='right';
      ctx.fillText(isRev?(v>=1000?(v/1000).toFixed(0)+'K':v.toFixed(0)):(v>=1000?(v/1000).toFixed(1)+'K':v.toFixed(0)), pad.l-5, y+3.5);
    }
    const slotW=cw/12, gap=2, bw=Math.max(5,Math.min(16,slotW*0.38));
    dataA.forEach((vA,i)=>{
      const vB=dataB[i];
      const cx=pad.l+slotW*i+slotW/2;
      // Bar A (selected year — orange)
      const bhA=(vA/maxVal)*ch, byA=pad.t+ch-bhA;
      if(bhA>1){
        const gA=ctx.createLinearGradient(0,byA,0,byA+bhA);
        gA.addColorStop(0,'#F97316'); gA.addColorStop(1,'rgba(249,115,22,.3)');
        ctx.beginPath();
        if(ctx.roundRect) ctx.roundRect(cx-bw-gap/2,byA,bw,bhA,[3,3,0,0]);
        else ctx.rect(cx-bw-gap/2,byA,bw,bhA);
        ctx.fillStyle=gA; ctx.fill();
      }
      // Bar B (compare year — indigo)
      const bhB=(vB/maxVal)*ch, byB=pad.t+ch-bhB;
      if(bhB>1){
        const gB=ctx.createLinearGradient(0,byB,0,byB+bhB);
        gB.addColorStop(0,'#6366F1'); gB.addColorStop(1,'rgba(99,102,241,.25)');
        ctx.beginPath();
        if(ctx.roundRect) ctx.roundRect(cx+gap/2,byB,bw,bhB,[3,3,0,0]);
        else ctx.rect(cx+gap/2,byB,bw,bhB);
        ctx.fillStyle=gB; ctx.fill();
      }
      // Month label
      ctx.fillStyle=tc; ctx.font='500 8.5px DM Sans,sans-serif'; ctx.textAlign='center';
      ctx.fillText(MS[i],cx,h-pad.b+13);
    });
  }

  expYearly(){
    const yr=+document.getElementById('y-year').value;
    const ps=DB.get('_purchases',[]).filter(p=>new Date(p.date+'T00:00:00').getFullYear()===yr);
    const cm=this._cmap();
    const prefs=DB.get('_prefs',{currency:'GHS'}); const cur=prefs.currency||'GHS';
    const sorted=ps.slice().sort((a,b)=>a.date.localeCompare(b.date));
    const totL=sorted.reduce((s,p)=>s+p.litres,0), totR=sorted.reduce((s,p)=>s+p.total,0);
    const pL=sorted.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const dL=sorted.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);
    const dataRows=[['Date','Customer','Vehicle','Fuel','Litres','Unit Price ('+cur+')','Total ('+cur+')','Notes']];
    sorted.forEach(p=>{ const c=cm[p.cid]||{name:'?',car:'?'}; dataRows.push([p.date,c.name,c.car,p.fuel,p.litres,+p.ppl,+p.total.toFixed(2),p.notes||'']); });
    dataRows.push(['','','','TOTAL',totL,'',+totR.toFixed(2),'']);
    const summaryRows=[['Metric','Value'],['Year',yr],['Total Transactions',sorted.length],['Total Litres',totL.toFixed(2)],['Petrol Litres',pL.toFixed(2)],['Diesel Litres',dL.toFixed(2)],['Total Revenue ('+cur+')',totR.toFixed(2)]];
    this._xlsxDownload(dataRows, summaryRows, `Year ${yr}`, `Yearly_Report_${yr}`);
    this._audit('create',`Exported yearly Excel: ${yr}`);
  }

  /* ── HISTORY ── */
  renderHist(){
    const ps=DB.get('_purchases',[]), cm=this._cmap();
    let arr=ps.slice().sort((a,b)=>b.date.localeCompare(a.date));
    const q=document.getElementById('h-q').value.trim().toLowerCase(), fuel=document.getElementById('h-fuel').value, mo=document.getElementById('h-month').value, from=document.getElementById('h-from').value, to=document.getElementById('h-to').value;
    if(q) arr=arr.filter(p=>{ const c=cm[p.cid]; return (c?.name||'').toLowerCase().includes(q)||(c?.car||'').toLowerCase().includes(q); });
    if(fuel) arr=arr.filter(p=>p.fuel===fuel);
    if(mo!=='') arr=arr.filter(p=>new Date(p.date+'T00:00:00').getMonth()===+mo);
    if(from) arr=arr.filter(p=>p.date>=from); if(to) arr=arr.filter(p=>p.date<=to);
    const pages=Math.ceil(arr.length/this.PS)||1; this.hPage=Math.min(this.hPage,pages);
    const slice=arr.slice((this.hPage-1)*this.PS,this.hPage*this.PS);
    const tb=document.getElementById('h-tbody');
    if(!slice.length){ tb.innerHTML=`<tr><td colspan="11" class="empty-row">No records match filters.</td></tr>`; }
    else tb.innerHTML=slice.map(p=>{ const c=cm[p.cid]||{name:'?',car:'—',phone:'—'}; return `<tr>
      <td>${F.date(p.date)}</td>
      <td><div class="row-gap"><div class="cust-av" style="width:26px;height:26px;font-size:.62rem;border-radius:5px;background:${av(c.name)}">${F.init(c.name)}</div>${F.esc(c.name)}</div></td>
      <td><span style="font-family:monospace;font-size:.78rem">${F.esc(c.car)}</span></td>
      <td class="text-muted">${F.esc(c.phone||'—')}</td>
      <td><span class="badge badge-${p.fuel.toLowerCase()}">${p.fuel}</span></td>
      <td>${F.n2(p.litres)} L</td>
      <td><span class="badge badge-brand">${INV.pct(p.litres,p.fuel)}</span></td>
      <td>${F.money(p.ppl)}</td>
      <td class="fw-700 text-brand">${F.money(p.total)}</td>
      <td class="text-muted clamp1" style="max-width:90px;font-size:.73rem">${F.esc(p.notes||'—')}</td>
      <td><div class="row-gap">
        <button class="btn btn-info btn-xs" data-ac="rcp" data-id="${p.id}">Receipt</button>
        <button class="btn btn-ghost btn-xs" data-ac="ep" data-id="${p.id}">Edit</button>
        <button class="btn btn-danger btn-xs" data-ac="dp" data-id="${p.id}">Del</button>
      </div></td>
    </tr>`; }).join('');
    this._pager('h-pager',this.hPage,pages,p=>{ this.hPage=p; this.renderHist(); });
  }

  /* ── AUDIT ── */
  renderAudit(){
    let logs=DB.get('_audit',[]); const q=document.getElementById('al-q').value.trim().toLowerCase(), type=document.getElementById('al-type').value;
    if(q) logs=logs.filter(l=>(l.action||'').toLowerCase().includes(q)||(l.user||'').toLowerCase().includes(q));
    if(type) logs=logs.filter(l=>l.type===type);
    const pages=Math.ceil(logs.length/20)||1; this.aPage=Math.min(this.aPage,pages);
    const slice=logs.slice((this.aPage-1)*20,this.aPage*20);
    const auditIcons={login:'<path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/>',create:'<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>',edit:'<path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>',delete:'<polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>',settings:'<circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/>'};
    const el=document.getElementById('audit-list');
    if(!slice.length){ el.innerHTML=`<div class="empty-row">No audit records found.</div>`; }
    else el.innerHTML=slice.map(l=>`
      <div class="audit-entry">
        <div class="audit-icon ${l.type||'create'}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${auditIcons[l.type]||auditIcons.create}</svg></div>
        <div>
          <div class="audit-action">${F.esc(l.action)}</div>
          ${l.detail?`<div class="audit-detail">${F.esc(l.detail)}</div>`:''}
          <div class="audit-meta">${F.esc(l.ts)} · ${F.esc(l.user||'System')} · <span class="badge badge-gray" style="font-size:.6rem">${l.type||'system'}</span></div>
        </div>
      </div>`).join('');
    this._pager('al-pager',this.aPage,pages,p=>{ this.aPage=p; this.renderAudit(); });
  }

  expAudit(){ const logs=DB.get('_audit',[]); const rows=[['Timestamp','User','Type','Action','Detail']]; logs.forEach(l=>rows.push([l.ts||'',l.user||'',l.type||'',l.action||'',l.detail||''])); this._csv(rows,'audit_log'); }

  /* ── SETTINGS ── */
  renderSettings(){
    const p=DB.get('_prefs',{currency:'GHS',country:'GH',language:'en',dateformat:'DD/MM/YYYY'});
    document.getElementById('s-display').value=this.user.display||'';
    document.getElementById('s-username').value=this.user.username||'';
    // Show/hide Users tab based on role
    const usersTabBtn=document.getElementById('stab-users-btn');
    if(usersTabBtn) usersTabBtn.style.display=this.user.role==='admin'?'':'none';
    this.renderUsersTable();
    this._loadAvatarPreview();
    this._loadSMSSettings();
    document.getElementById('s-cur').value=p.currency||'GHS';
    document.getElementById('s-country').value=p.country||'GH';
    document.getElementById('s-lang').value=p.language||'en';
    document.getElementById('s-dfmt').value=p.dateformat||'DD/MM/YYYY';
    ['s-old','s-new','s-cnf'].forEach(id=>document.getElementById(id).value='');
    document.getElementById('s-perr').classList.remove('show');
    // Backup counts
    const bkCust=document.getElementById('bk-cust-count');
    const bkPurch=document.getElementById('bk-purch-count');
    const bkDel=document.getElementById('bk-del-count');
    const bkAudit=document.getElementById('bk-audit-count');
    if(bkCust) bkCust.textContent=DB.get('_customers',[]).length+' records';
    if(bkPurch) bkPurch.textContent=DB.get('_purchases',[]).length+' records';
    if(bkDel) bkDel.textContent=DB.get('_deliveries',[]).length+' records';
    if(bkAudit) bkAudit.textContent=DB.get('_audit',[]).length+' entries';
    const bkBtn=document.getElementById('export-backup-btn');
    if(bkBtn){ bkBtn.onclick=null; bkBtn.addEventListener('click',()=>this.exportFullBackup()); }
    // Schedule UI
    const sched=DB.get('_backup_schedule',{freq:'off',time:'08:00'});
    const freqEl=document.getElementById('bk-freq'); if(freqEl) freqEl.value=sched.freq||'off';
    const timeEl=document.getElementById('bk-time'); if(timeEl) timeEl.value=sched.time||'08:00';
    this._updateScheduleBadge();
    const saveSchedBtn=document.getElementById('save-schedule-btn');
    if(saveSchedBtn){ saveSchedBtn.onclick=null; saveSchedBtn.addEventListener('click',()=>this._saveBackupSchedule()); }
    // Last backup info
    const lastTs=DB.get('_last_backup_ts','');
    const lastWrap=document.getElementById('bk-last-wrap');
    const lastEl=document.getElementById('bk-last-time');
    if(lastWrap&&lastEl){ if(lastTs){ lastWrap.style.display=''; lastEl.textContent=lastTs; } else { lastWrap.style.display='none'; } }
    // Thresholds
    const thresh=DB.get('_thresholds',{petrol:0,diesel:0});
    const ptEl=document.getElementById('s-petrol-threshold');
    const dtEl=document.getElementById('s-diesel-threshold');
    if(ptEl) ptEl.value=thresh.petrol||'';
    if(dtEl) dtEl.value=thresh.diesel||'';
    const thrBtn=document.getElementById('save-thresholds-btn');
    if(thrBtn){ thrBtn.onclick=null; thrBtn.addEventListener('click',()=>this._saveThresholds()); }
    // Render notifications on load
    this._renderNotifPanel();
  }

  _saveThresholds(){
    const p=+document.getElementById('s-petrol-threshold').value||0;
    const d=+document.getElementById('s-diesel-threshold').value||0;
    DB.set('_thresholds',{petrol:p,diesel:d});
    this._audit('settings',`Low-stock thresholds updated`,`Petrol: ${p}L, Diesel: ${d}L`);
    this.toast('Thresholds saved');
    this._checkLowStock();
  }

  _checkLowStock(){
    const thresh=DB.get('_thresholds',{petrol:0,diesel:0});
    const inv=DB.get('_inv',{petrolTotal:0,dieselTotal:0});
    const purchases=DB.get('_purchases',[]);
    const pSold=purchases.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0);
    const dSold=purchases.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0);
    const pRem=Math.max(0,(inv.petrolTotal||0)-pSold);
    const dRem=Math.max(0,(inv.dieselTotal||0)-dSold);
    if(thresh.petrol>0 && pRem<=thresh.petrol){
      this._pushNotif('low-petrol',`⚠️ Petrol Stock Low`,`Only ${Math.round(pRem).toLocaleString()} L remaining — threshold is ${thresh.petrol.toLocaleString()} L. Restock soon.`,'warn');
    }
    if(thresh.diesel>0 && dRem<=thresh.diesel){
      this._pushNotif('low-diesel',`⚠️ Diesel Stock Low`,`Only ${Math.round(dRem).toLocaleString()} L remaining — threshold is ${thresh.diesel.toLocaleString()} L. Restock soon.`,'danger');
    }
  }

  _pushNotif(id, title, body, level='warn'){
    const notifs=DB.get('_notifications',[]);
    // Avoid duplicate unread alerts for same issue
    const existing=notifs.find(n=>n.id===id&&!n.read);
    if(existing) return;
    notifs.unshift({id,title,body,level,read:false,ts:F.ts()});
    if(notifs.length>50) notifs.splice(50);
    DB.set('_notifications',notifs);
    this._renderNotifPanel();
  }

  _renderNotifPanel(){
    const notifs=DB.get('_notifications',[]);
    const unread=notifs.filter(n=>!n.read).length;
    const badge=document.getElementById('notif-badge');
    const list=document.getElementById('notif-list');
    if(!badge||!list) return;
    // Badge
    if(unread>0){
      badge.textContent=unread>9?'9+':unread;
      badge.classList.remove('hidden');
      badge.classList.add('pop');
      setTimeout(()=>badge.classList.remove('pop'),400);
    } else {
      badge.classList.add('hidden');
    }
    // List
    if(!notifs.length){
      list.innerHTML=`<div class="notif-empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>No notifications</div>`;
      return;
    }
    list.innerHTML=notifs.map(n=>`
      <div class="notif-item ${n.read?'':'unread'}" data-nid="${n.id}" onclick="A._markNotifRead('${n.id}')">
        <div class="notif-dot ${n.read?'read':n.level==='danger'?'danger':''}"></div>
        <div style="flex:1;min-width:0">
          <div class="notif-item-title">${n.title}</div>
          <div class="notif-item-body">${n.body}</div>
          <div class="notif-item-time">${n.ts}</div>
        </div>
      </div>`).join('');
  }

  _markNotifRead(id){
    const notifs=DB.get('_notifications',[]).map(n=>n.id===id?{...n,read:true}:n);
    DB.set('_notifications',notifs);
    this._renderNotifPanel();
  }

  _toggleNotifPanel(){
    const panel=document.getElementById('notif-panel');
    const isOpen=panel.classList.toggle('open');
    if(isOpen){
      // Mark all as read when opened
      const notifs=DB.get('_notifications',[]).map(n=>({...n,read:true}));
      DB.set('_notifications',notifs);
      setTimeout(()=>this._renderNotifPanel(),300);
    }
  }

  _saveBackupSchedule(){
    const freq=document.getElementById('bk-freq').value;
    const time=document.getElementById('bk-time').value||'08:00';
    DB.set('_backup_schedule',{freq,time});
    this._audit('settings',`Auto-backup schedule updated`,`Frequency: ${freq}, Time: ${time}`);
    this._updateScheduleBadge();
    this.toast(freq==='off'?'Auto-backup disabled':'Schedule saved — backup will run automatically');
  }

  _updateScheduleBadge(){
    const sched=DB.get('_backup_schedule',{freq:'off',time:'08:00'});
    const badge=document.getElementById('bk-schedule-badge');
    const nextWrap=document.getElementById('bk-next-wrap');
    const nextEl=document.getElementById('bk-next-time');
    if(!badge) return;
    if(sched.freq==='off'){
      badge.textContent='Off';
      badge.style.background='var(--surface-3)'; badge.style.color='var(--t4)'; badge.style.borderColor='var(--border)';
      if(nextWrap) nextWrap.style.display='none';
    } else {
      badge.textContent=sched.freq.charAt(0).toUpperCase()+sched.freq.slice(1);
      badge.style.background='var(--success-bg)'; badge.style.color='var(--success)'; badge.style.borderColor='#bbf7d0';
      if(nextWrap&&nextEl){ nextWrap.style.display=''; nextEl.textContent=this._nextBackupLabel(sched); }
    }
  }

  _nextBackupLabel(sched){
    const now=new Date();
    const [hh,mm]=(sched.time||'08:00').split(':').map(Number);
    let next=new Date(now);
    next.setHours(hh,mm,0,0);
    if(sched.freq==='daily'){
      if(next<=now) next.setDate(next.getDate()+1);
    } else if(sched.freq==='weekly'){
      next.setDate(next.getDate()+((7-next.getDay())%7||7));
      next.setHours(hh,mm,0,0);
      if(next<=now) next.setDate(next.getDate()+7);
    } else if(sched.freq==='monthly'){
      next=new Date(now.getFullYear(),now.getMonth()+1,1,hh,mm,0,0);
    }
    return next.toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'});
  }

  _checkAutoBackup(){
    const sched=DB.get('_backup_schedule',{freq:'off',time:'08:00'});
    if(!sched||sched.freq==='off') return;
    const lastTs=DB.get('_last_backup_ts','');
    const now=new Date();
    const [hh,mm]=(sched.time||'08:00').split(':').map(Number);
    const todayKey=now.toISOString().slice(0,10);
    const lastDate=lastTs?lastTs.slice(0,10):'';
    let due=false;
    if(sched.freq==='daily'){
      // Due if today not yet backed up AND current time is past scheduled time
      due=(lastDate!==todayKey)&&(now.getHours()>hh||(now.getHours()===hh&&now.getMinutes()>=mm));
    } else if(sched.freq==='weekly'){
      // Due on Mondays past scheduled time
      due=(now.getDay()===1)&&(lastDate!==todayKey)&&(now.getHours()>hh||(now.getHours()===hh&&now.getMinutes()>=mm));
    } else if(sched.freq==='monthly'){
      // Due on 1st of month past scheduled time
      due=(now.getDate()===1)&&(lastDate!==todayKey)&&(now.getHours()>hh||(now.getHours()===hh&&now.getMinutes()>=mm));
    }
    if(due){
      this.exportFullBackup(true); // silent=true
    }
  }

  exportFullBackup(silent=false){
    const prefs=DB.get('_prefs',{currency:'GHS'});
    const cur=prefs.currency||'GHS';
    const customers=DB.get('_customers',[]);
    const purchases=DB.get('_purchases',[]);
    const deliveries=DB.get('_deliveries',[]);
    const audit=DB.get('_audit',[]);
    const inv=DB.get('_inv',{petrolTotal:0,dieselTotal:0});
    const cmap={};customers.forEach(c=>cmap[c.id]=c);
    const WB=XLSX.utils.book_new();
    const hdr={font:{bold:true,color:{rgb:'FFFFFF'},name:'Arial',sz:10},fill:{fgColor:{rgb:'1A1A2E'}},alignment:{horizontal:'center',vertical:'center'},border:{bottom:{style:'medium',color:{rgb:'F97316'}}}};
    const alt={fill:{fgColor:{rgb:'F9FAFB'}},font:{name:'Arial',sz:9},alignment:{horizontal:'left',vertical:'center'}};
    const norm={fill:{fgColor:{rgb:'FFFFFF'}},font:{name:'Arial',sz:9},alignment:{horizontal:'left',vertical:'center'}};
    const num={fill:{fgColor:{rgb:'FFFFFF'}},font:{name:'Arial',sz:9},alignment:{horizontal:'right',vertical:'center'},numFmt:'#,##0.00'};
    const _sheet=(rows)=>{
      const ws=XLSX.utils.aoa_to_sheet(rows);
      const range=XLSX.utils.decode_range(ws['!ref']||'A1');
      for(let R=range.s.r;R<=range.e.r;R++) for(let C=range.s.c;C<=range.e.c;C++){
        const addr=XLSX.utils.encode_cell({r:R,c:C});
        if(!ws[addr]) ws[addr]={t:'z',v:''};
        ws[addr].s=R===0?hdr:(R%2===0?alt:norm);
      }
      ws['!cols']=rows[0]?.map((_,ci)=>({wch:Math.min(36,Math.max(10,...rows.map(r=>String(r[ci]||'').length)))}))
      ws['!rows']=[{hpt:22}];
      return ws;
    };
    // Sheet 1 — Purchases
    const pRows=[['Date','Customer','Vehicle','Fuel','Litres','Price/L ('+cur+')','Total ('+cur+')','Notes']];
    purchases.slice().sort((a,b)=>a.date.localeCompare(b.date)).forEach(p=>{
      const c=cmap[p.cid]||{name:'Unknown',car:'—'};
      pRows.push([p.date,c.name,c.car,p.fuel,+p.litres,+p.ppl,+(+p.total).toFixed(2),p.notes||'']);
    });
    XLSX.utils.book_append_sheet(WB,_sheet(pRows),'Purchases');
    // Sheet 2 — Customers
    const cRows=[['Name','Vehicle','Phone','Email','Address','Fuel Preference','Created']];
    customers.forEach(c=>cRows.push([c.name,c.car,c.phone||'',c.email||'',c.address||'',c.fuel||'',c.createdAt||'']));
    XLSX.utils.book_append_sheet(WB,_sheet(cRows),'Customers');
    // Sheet 3 — Deliveries
    const dRows=[['Date','Fuel Type','Litres Added','Notes']];
    deliveries.slice().sort((a,b)=>(a.date||'').localeCompare(b.date||'')).forEach(d=>dRows.push([d.date,d.fuel,+d.litres,d.notes||'']));
    XLSX.utils.book_append_sheet(WB,_sheet(dRows),'Deliveries');
    // Sheet 4 — Inventory Summary
    const iRows=[['Metric','Petrol','Diesel'],['Total Added (L)',inv.petrolTotal||0,inv.dieselTotal||0],['Total Sold (L)',purchases.filter(p=>p.fuel==='Petrol').reduce((s,p)=>s+p.litres,0),purchases.filter(p=>p.fuel==='Diesel').reduce((s,p)=>s+p.litres,0)]];
    iRows.push(['Remaining (L)',+(iRows[1][1]-iRows[2][1]).toFixed(2),+(iRows[1][2]-iRows[2][2]).toFixed(2)]);
    XLSX.utils.book_append_sheet(WB,_sheet(iRows),'Inventory');
    // Sheet 5 — Audit Log
    const aRows=[['Timestamp','User','Type','Action','Detail']];
    audit.forEach(l=>aRows.push([l.ts||'',l.user||'',l.type||'',l.action||'',l.detail||'']));
    XLSX.utils.book_append_sheet(WB,_sheet(aRows),'Audit Log');
    const ts=new Date().toISOString().slice(0,10);
    XLSX.writeFile(WB,`Emmanuel_Fuel_Backup_${ts}.xlsx`);
    const tsLabel=new Date().toLocaleString('en-GB');
    DB.set('_last_backup_ts', tsLabel);
    this._audit('create',`Full database backup exported`);
    this._pushNotif('backup-'+ts, '✅ Backup Complete', `Database backup downloaded successfully on ${tsLabel}.`, 'warn');
    if(!silent) this.toast('Backup downloaded successfully');
    else this.toast('Auto-backup downloaded successfully');
  }

  saveProfile(){
    const display=document.getElementById('s-display').value.trim();
    if(!display){ this.toast('Display name required','error'); return; }
    let users=DB.get('_users',[]);
    if(this._pendingAvatar!==undefined){
      const avs=DB.get('_avatars',{});
      if(this._pendingAvatar) avs[this.user.uid||this.user.email]=this._pendingAvatar;
      else delete avs[this.user.uid||this.user.email];
      DB.set('_avatars',avs);
      this._pendingAvatar=undefined;
    }
    users=users.map(u=>(u.uid===this.user.uid)?{...u,display}:u);
    DB.set('_users',users);
    this.user={...this.user,display};
    this._refreshTopbar();
    this._audit('settings','Profile updated',`Display: ${display}`);
    this.toast('Profile saved');
  }
  async savePass(){
    const oldP=document.getElementById('s-old').value;
    const newP=document.getElementById('s-new').value;
    const cnfP=document.getElementById('s-cnf').value;
    const err=document.getElementById('s-perr');
    if(!oldP||!newP||!cnfP){ err.textContent='All fields are required.'; err.classList.add('show'); return; }
    if(newP.length<6){ err.textContent='New password must be at least 6 characters.'; err.classList.add('show'); return; }
    if(newP!==cnfP){ err.textContent='Passwords do not match.'; err.classList.add('show'); return; }
    try {
      // Re-authenticate first (Firebase requires this for password changes)
      const fbUser=fb_auth.currentUser;
      const cred=EmailAuthProvider.credential(fbUser.email, oldP);
      await reauthenticateWithCredential(fbUser, cred);
      await updatePassword(fbUser, newP);
      err.classList.remove('show');
      ['s-old','s-new','s-cnf'].forEach(id=>document.getElementById(id).value='');
      this._audit('settings','Password changed');
      this.toast('Password updated ✓');
    } catch(e){
      const msgs={
        'auth/wrong-password':'Current password is incorrect.',
        'auth/invalid-credential':'Current password is incorrect.',
        'auth/too-many-requests':'Too many attempts. Please try again later.',
        'auth/weak-password':'New password is too weak.',
      };
      err.textContent=msgs[e.code]||'Password update failed. Try again.';
      err.classList.add('show');
    }
  }

  savePrefs(){
    const p={currency:document.getElementById('s-cur').value,country:document.getElementById('s-country').value,language:document.getElementById('s-lang').value,dateformat:document.getElementById('s-dfmt').value};
    DB.set('_prefs',p); this._audit('settings','Preferences updated',`Currency: ${p.currency}, Country: ${p.country}`); this.toast('Preferences saved');
  }

  /* ── USER MANAGEMENT ── */
  renderUsersTable(){
    const ADMIN_EMAIL = 'eb810111@gmail.com';
    const users=DB.get('_users',[]);
    const tb=document.getElementById('users-tbody'); if(!tb) return;
    if(!users.length){ tb.innerHTML='<tr><td colspan="5" class="empty-row">No users found.</td></tr>'; return; }
    tb.innerHTML=users.map(u=>{
      const isSelf=u.uid===this.user?.uid;
      const isAdmin=this.user?.role==='admin';
      const isMasterAdmin=u.email?.toLowerCase()===ADMIN_EMAIL;
      const avs=DB.get('_avatars',{}); const img=avs[u.uid||u.email];
      return `<tr>
        <td><div class="row-gap">
          <div class="user-row-av" style="background:${av(u.display||u.email)};overflow:hidden">
            ${img?`<img src="${img}" style="width:100%;height:100%;object-fit:cover;border-radius:var(--radius-sm)"/>`:`${F.init(u.display||u.email)}`}
          </div>
          <div>
            <div class="fw-600">${F.esc(u.display||u.email)}</div>
            ${isSelf?'<div style="font-size:.65rem;color:var(--brand);font-weight:600">You</div>':''}
            ${isMasterAdmin?'<div style="font-size:.62rem;color:var(--t4)">Master Admin</div>':''}
          </div>
        </div></td>
        <td><span style="font-size:.78rem;color:var(--t3)">${F.esc(u.email||'—')}</span></td>
        <td><span class="badge ${u.role==='admin'?'badge-admin':'badge-staff'}">${u.role==='admin'?'Admin':'Staff'}</span></td>
        <td class="text-muted" style="font-size:.75rem">${u.createdAt||'—'}</td>
        <td><div class="row-gap">
          ${isAdmin&&!isSelf&&!isMasterAdmin?`
            <button class="btn btn-secondary btn-xs" onclick="A.openEditUser('${u.uid}')">Edit</button>
            <button class="btn ${u.role==='admin'?'btn-warning':'btn-info'} btn-xs" onclick="A.toggleUserRole('${u.uid}')">${u.role==='admin'?'Make Staff':'Make Admin'}</button>
            <button class="btn btn-danger btn-xs" onclick="A.deleteUser('${u.uid}')">Remove</button>
          `:''}
          ${isSelf||isMasterAdmin?'<span class="text-muted" style="font-size:.72rem">—</span>':''}
        </div></td>
      </tr>`;
    }).join('');
  }

  toggleUserRole(uid){
    const ADMIN_EMAIL = 'eb810111@gmail.com';
    let users=DB.get('_users',[]); const u=users.find(x=>x.uid===uid);
    if(!u||u.email?.toLowerCase()===ADMIN_EMAIL) return;
    const newRole = u.role==='admin' ? 'staff' : 'admin';
    users=users.map(x=>x.uid===uid?{...x,role:newRole}:x);
    DB.set('_users',users);
    this._audit('settings',`Role changed: ${u.display||u.email}`,`New role: ${newRole}`);
    this.toast(`${u.display||u.email} is now ${newRole}`);
    this.renderUsersTable();
  }

  openAddUser(){
    document.getElementById('eu-title').textContent='Add New Staff';
    document.getElementById('eu-orig').value='__new__';
    document.getElementById('eu-display').value='';
    document.getElementById('eu-username').value='';
    document.getElementById('eu-pwd').value='';
    document.getElementById('eu-role').value='staff';
    document.getElementById('eu-role-staff').classList.add('selected');
    document.getElementById('eu-role-admin').classList.remove('selected');
    document.getElementById('eu-err').classList.remove('show');
    // Show email + password fields for new user
    document.getElementById('eu-username').closest('.form-field').querySelector('label').textContent='Email Address';
    document.getElementById('eu-username').placeholder='staff@example.com';
    document.getElementById('eu-username').type='email';
    document.getElementById('eu-pwd').closest('.form-field').querySelector('label').textContent='Temporary Password';
    document.getElementById('save-eu-btn').textContent='Create Account';
    document.getElementById('m-eu').style.display='flex';
  }

  openEditUser(uid){
    const users=DB.get('_users',[]); const u=users.find(x=>x.uid===uid); if(!u) return;
    document.getElementById('eu-title').textContent='Edit User';
    document.getElementById('eu-orig').value=uid;
    document.getElementById('eu-display').value=u.display||'';
    document.getElementById('eu-username').value=u.email||'';
    document.getElementById('eu-username').type='text';
    document.getElementById('eu-username').closest('.form-field').querySelector('label').textContent='Email';
    document.getElementById('eu-pwd').value='';
    document.getElementById('eu-pwd').closest('.form-field').querySelector('label').textContent='New Password (leave blank to keep current)';
    document.getElementById('eu-role').value=u.role||'staff';
    document.getElementById('eu-role-staff').classList.toggle('selected',u.role!=='admin');
    document.getElementById('eu-role-admin').classList.toggle('selected',u.role==='admin');
    document.getElementById('eu-err').classList.remove('show');
    document.getElementById('save-eu-btn').textContent='Save Changes';
    document.getElementById('m-eu').style.display='flex';
  }

  async saveEditUser(){
    const orig=document.getElementById('eu-orig').value;
    const display=document.getElementById('eu-display').value.trim();
    const emailOrUsername=document.getElementById('eu-username').value.trim().toLowerCase();
    const role=document.getElementById('eu-role').value;
    const pwd=document.getElementById('eu-pwd').value;
    const err=document.getElementById('eu-err');
    const btn=document.getElementById('save-eu-btn');
    if(!display){ err.textContent='Display name is required.'; err.classList.add('show'); return; }

    if(orig==='__new__'){
      // Creating new user via Firebase Auth
      if(!emailOrUsername){ err.textContent='Email address is required.'; err.classList.add('show'); return; }
      if(!pwd||pwd.length<6){ err.textContent='Password must be at least 6 characters.'; err.classList.add('show'); return; }
      btn.textContent='Creating…'; btn.disabled=true;
      try {
        // Create Firebase Auth account
        const { createUserWithEmailAndPassword: _c, getAuth: _g } = await Promise.resolve({ createUserWithEmailAndPassword, getAuth: ()=>fb_auth });
        const cred = await createUserWithEmailAndPassword(fb_auth, emailOrUsername, pwd);
        const ADMIN_EMAIL='eb810111@gmail.com';
        const assignedRole = emailOrUsername===ADMIN_EMAIL ? 'admin' : role;
        const newU = { uid:cred.user.uid, email:emailOrUsername, display, role:assignedRole, createdAt:new Date().toLocaleDateString('en-GB') };
        const users=DB.get('_users',[]); users.push(newU); DB.set('_users',users);
        this._audit('create',`Admin created account: ${display} (${emailOrUsername})`,`Role: ${assignedRole}`);
        this.toast(`Account created for ${display}`);
        btn.textContent='Create Account'; btn.disabled=false;
        err.classList.remove('show');
        this.closeModal('m-eu'); this.renderUsersTable();
      } catch(e){
        btn.textContent='Create Account'; btn.disabled=false;
        const msgs={
          'auth/email-already-in-use':'An account with this email already exists.',
          'auth/invalid-email':'Please enter a valid email address.',
          'auth/weak-password':'Password must be at least 6 characters.',
        };
        err.textContent=msgs[e.code]||'Failed to create account. Try again.';
        err.classList.add('show');
      }
    } else {
      // Editing existing user — only update display name and role in Firestore
      let users=DB.get('_users',[]);
      users=users.map(u=>u.uid===orig?{...u,display,role}:u);
      DB.set('_users',users);
      if(orig===this.user?.uid){ this.user={...this.user,display,role}; this._refreshTopbar(); }
      this._audit('edit',`Edited user: ${display}`,`Role: ${role}`);
      this.toast(`${display} updated`);
      err.classList.remove('show');
      this.closeModal('m-eu'); this.renderUsersTable();
    }
  }

  deleteUser(uid){
    if(uid===this.user?.uid){ this.toast('Cannot delete your own account','error'); return; }
    const users=DB.get('_users',[]); const u=users.find(x=>x.uid===uid);
    this._confirm(`Remove ${u?.display||u?.email}? This cannot be undone.`,()=>{
      DB.set('_users',users.filter(x=>x.uid!==uid));
      this._audit('delete',`Removed user: ${u?.display||u?.email}`);
      this.renderUsersTable(); this.toast(`${u?.display||'User'} removed`,'error');
    });
  }

  /* ── SMS NOTIFICATIONS ── */
  /* ── TOPBAR AVATAR REFRESH ── */
  _refreshTopbar(){
    const avKey=this.user?.uid||this.user?.email;
    const avs=DB.get('_avatars',{}), img=avs[avKey];
    const el=document.getElementById('user-av');
    if(!el) return;
    if(img){ el.innerHTML=`<img src="${img}" alt="" style="width:100%;height:100%;object-fit:cover;border-radius:50%"/>`; }
    else { el.textContent=F.init(this.user?.display||this.user?.email||'?'); }
    const nameEl=document.getElementById('user-chip-name');
    if(nameEl) nameEl.textContent=this.user?.display||this.user?.email||'';
    const sbAv=document.getElementById('sb-user-av');
    const sbName=document.getElementById('sb-user-name');
    const sbRole=document.getElementById('sb-user-role');
    if(sbAv){
      if(img){ sbAv.innerHTML=`<img src="${img}" alt=""/>`; }
      else { sbAv.textContent=F.init(this.user?.display||this.user?.email||'?'); sbAv.style.background=av(this.user?.display||'U'); }
    }
    if(sbName) sbName.textContent=this.user?.display||this.user?.email||'';
    if(sbRole){ const r=this.user?.role||'staff'; sbRole.textContent=r.charAt(0).toUpperCase()+r.slice(1); }
  }

  /* ── PROFILE PICTURE ── */
  _loadAvatarPreview(){
    const avKey=this.user?.uid||this.user?.email;
    const avs=DB.get('_avatars',{}), img=avs[avKey];
    const wrap=document.getElementById('av-preview-wrap');
    const nameEl=document.getElementById('av-name-label');
    if(!wrap) return;
    if(nameEl) nameEl.textContent=this.user?.display||'Your Profile';
    if(img){ wrap.innerHTML=`<img src="${img}" alt="avatar" style="width:100%;height:100%;object-fit:cover;border-radius:var(--radius-md)"/>`; }
    else { wrap.innerHTML=`<div class="avatar-preview-placeholder" style="background:${av(this.user?.display||'U')}">${F.init(this.user?.display||'U')}</div>`; }
    this._pendingAvatar=undefined;
  }

  _loadAvatarFile(file){
    if(file.size>4*1024*1024){ this.toast('Image too large — max 4 MB','error'); return; }
    const reader=new FileReader();
    reader.onload=e=>{
      this._resizeAvatar(e.target.result, 200, resized=>{
        this._pendingAvatar=resized;
        const wrap=document.getElementById('av-preview-wrap');
        if(wrap) wrap.innerHTML=`<img src="${resized}" style="width:100%;height:100%;object-fit:cover;border-radius:var(--radius-md)"/>`;
        this.toast('Photo ready — click Save Profile to apply');
      });
    };
    reader.readAsDataURL(file);
  }

  _resizeAvatar(dataUrl, size, cb){
    const img=new Image();
    img.onload=()=>{
      const c=document.createElement('canvas');
      c.width=c.height=size;
      const ctx=c.getContext('2d');
      const s=Math.min(img.width,img.height);
      ctx.drawImage(img,(img.width-s)/2,(img.height-s)/2,s,s,0,0,size,size);
      cb(c.toDataURL('image/jpeg',0.85));
    };
    img.src=dataUrl;
  }

  _removeAvatar(){
    this._pendingAvatar=null;
    const wrap=document.getElementById('av-preview-wrap');
    if(wrap) wrap.innerHTML=`<div class="avatar-preview-placeholder" style="background:${av(this.user?.display||'U')}">${F.init(this.user?.display||'U')}</div>`;
    this.toast('Photo removed — save to confirm');
  }

  /* ── CAMERA ── */
  openCamera(){
    this._camFacing='user'; this._camStream=null;
    const m=document.getElementById('m-camera'); if(!m) return;
    m.style.display='flex';
    document.getElementById('cam-live-view').style.display='';
    document.getElementById('cam-preview-view').style.display='none';
    document.getElementById('cam-error').style.display='none';
    this._startCamera();
  }

  async _startCamera(){
    if(this._camStream){ this._camStream.getTracks().forEach(t=>t.stop()); this._camStream=null; }
    try{
      const stream=await navigator.mediaDevices.getUserMedia({video:{facingMode:this._camFacing,width:{ideal:640},height:{ideal:640}},audio:false});
      this._camStream=stream;
      const v=document.getElementById('cam-video');
      v.srcObject=stream; v.play();
    }catch(e){
      const lv=document.getElementById('cam-live-view'), ce=document.getElementById('cam-error');
      if(lv) lv.style.display='none'; if(ce) ce.style.display='';
    }
  }

  _flipCamera(){ this._camFacing=this._camFacing==='user'?'environment':'user'; this._startCamera(); }

  _snapPhoto(){
    const v=document.getElementById('cam-video'), c=document.getElementById('cam-canvas');
    if(!v||!c) return;
    const size=Math.min(v.videoWidth,v.videoHeight)||300;
    c.width=c.height=size;
    c.getContext('2d').drawImage(v,(v.videoWidth-size)/2,(v.videoHeight-size)/2,size,size,0,0,size,size);
    const dataUrl=c.toDataURL('image/jpeg',0.85);
    const pi=document.getElementById('cam-preview-img'); if(pi) pi.src=dataUrl;
    const lv=document.getElementById('cam-live-view'), pv=document.getElementById('cam-preview-view');
    if(lv) lv.style.display='none'; if(pv) pv.style.display='';
  }

  _retakePhoto(){
    const lv=document.getElementById('cam-live-view'), pv=document.getElementById('cam-preview-view');
    if(pv) pv.style.display='none'; if(lv) lv.style.display='';
  }

  _usePhoto(){
    const pi=document.getElementById('cam-preview-img'); if(!pi) return;
    this._resizeAvatar(pi.src, 200, resized=>{
      this._pendingAvatar=resized;
      const wrap=document.getElementById('av-preview-wrap');
      if(wrap) wrap.innerHTML=`<img src="${resized}" style="width:100%;height:100%;object-fit:cover;border-radius:var(--radius-md)"/>`;
      this.closeCamera();
      this.toast('Photo captured — click Save Profile to apply');
    });
  }

  closeCamera(){
    if(this._camStream){ this._camStream.getTracks().forEach(t=>t.stop()); this._camStream=null; }
    const m=document.getElementById('m-camera'); if(m) m.style.display='none';
  }

  async sendTestSMS(){
    const s=DB.get('_sms',{});
    const result=document.getElementById('sms-result');
    if(!s.clientId||!s.clientSecret||!s.phone){
      result.textContent='Fill in Client ID, Client Secret and phone number first.';
      result.className='sms-result fail show'; return;
    }
    result.textContent='Sending test SMS via Hubtel…'; result.className='sms-result ok show';
    const tmp=s.enabled; s.enabled=true; DB.set('_sms',s);
    const sent=await this._sendSMS("✅ Test from Emmanuel's Fuel Tracking System. Hubtel SMS is working!",s.phone);
    s.enabled=tmp; DB.set('_sms',s);
    result.textContent=sent?'✅ Test SMS sent successfully!':'❌ Failed. Check your Hubtel credentials and wallet balance.';
    result.className=sent?'sms-result ok show':'sms-result fail show';
    setTimeout(()=>result.classList.remove('show'),8000);
  }

  _loadSMSSettings(){
    const s=DB.get('_sms',{enabled:false,clientId:'',clientSecret:'',sender:'',phone:'',onSale:true,onLow:true,onUser:true});
    document.getElementById('sms-api-user').value=s.clientId||'';
    document.getElementById('sms-api-key').value=s.clientSecret||'';
    document.getElementById('sms-sender').value=s.sender||'';
    document.getElementById('sms-phone').value=s.phone||'';
    document.getElementById('sms-t-sale').checked=s.onSale!==false;
    document.getElementById('sms-t-low').checked=s.onLow!==false;
    document.getElementById('sms-t-user').checked=s.onUser!==false;
    document.getElementById('sms-master').checked=!!s.enabled;
    const badge=document.getElementById('sms-status-badge');
    if(badge){
      badge.textContent=s.enabled?'Enabled':'Disabled';
      badge.style.cssText=s.enabled
        ?'background:var(--diesel-bg);color:var(--diesel);border:1px solid var(--diesel-bdr)'
        :'background:var(--surface-3);color:var(--t3);border:1px solid var(--border)';
    }
  }

  saveSMSSettings(){
    const s={
      enabled: document.getElementById('sms-master').checked,
      clientId: document.getElementById('sms-api-user').value.trim(),
      clientSecret: document.getElementById('sms-api-key').value.trim(),
      sender: document.getElementById('sms-sender').value.trim().slice(0,11),
      phone: document.getElementById('sms-phone').value.trim(),
      onSale: document.getElementById('sms-t-sale').checked,
      onLow:  document.getElementById('sms-t-low').checked,
      onUser: document.getElementById('sms-t-user').checked,
    };
    DB.set('_sms',s);
    // Update badge immediately
    const badge=document.getElementById('sms-status-badge');
    if(badge){
      badge.textContent=s.enabled?'Enabled':'Disabled';
      badge.style.cssText=s.enabled
        ?'background:var(--diesel-bg);color:var(--diesel);border:1px solid var(--diesel-bdr)'
        :'background:var(--surface-3);color:var(--t3);border:1px solid var(--border)';
    }
    this._audit('settings','Hubtel SMS settings updated',`Enabled: ${s.enabled}, Phone: ${s.phone}`);
    this.toast(s.enabled?'Hubtel SMS notifications enabled ✓':'SMS settings saved');
  }

  async sendTestSMS(){
    const s=DB.get('_sms',{});
    const result=document.getElementById('sms-result');
    if(!s.clientId||!s.clientSecret||!s.phone){
      result.textContent='Fill in Client ID, Client Secret and phone number first.';
      result.className='sms-result fail show'; return;
    }
    result.textContent='Sending test SMS via Hubtel…';
    result.className='sms-result ok show';
    // Temporarily enable for test even if master is off
    const tmpEnabled=s.enabled; s.enabled=true;
    DB.set('_sms',s);
    const sent=await this._sendSMS(
      `✅ Test from Emmanuel's Fuel Tracking System. Your Hubtel SMS is working perfectly!`,
      s.phone
    );
    s.enabled=tmpEnabled; DB.set('_sms',s);
    if(sent){
      result.textContent='✅ Test SMS sent successfully via Hubtel!';
      result.className='sms-result ok show';
    } else {
      result.textContent='❌ Failed. Double-check your Hubtel Client ID, Client Secret, and phone number. Ensure your Hubtel wallet has credit.';
      result.className='sms-result fail show';
    }
    setTimeout(()=>result.classList.remove('show'),8000);
  }

  async _sendSMS(message, to){
    const s=DB.get('_sms',{});
    if(!s.enabled||!s.clientId||!s.clientSecret||!to) return false;
    // Normalise Ghanaian number: strip leading 0, add +233
    let num=String(to).replace(/\s+/g,'');
    if(num.startsWith('0')) num='+233'+num.slice(1);
    if(!num.startsWith('+')) num='+'+num;
    try {
      // Hubtel SMS API v1
      // NOTE: In production, proxy this request through your own backend to avoid CORS.
      // Hubtel sandbox: https://devtracker.hubtel.com/
      const creds=btoa(`${s.clientId}:${s.clientSecret}`);
      const body={From:s.sender||'FuelApp',To:num,Content:message};
      const resp=await fetch('https://smsc.hubtel.com/v1/messages/send',{
        method:'POST',
        headers:{
          'Authorization':`Basic ${creds}`,
          'Content-Type':'application/json',
          'Accept':'application/json',
        },
        body:JSON.stringify(body),
      });
      if(resp.ok){
        const data=await resp.json();
        // Hubtel returns status 0 for success
        return data?.status===0||data?.Status===0||resp.status===200||resp.status===201;
      }
      return false;
    } catch(e){
      console.warn('Hubtel SMS error:',e);
      return false;
    }
  }

  _notifySMS(type, detail){
    const s=DB.get('_sms',{}); if(!s.enabled) return;
    const station="Emmanuel's Fuel Tracking System";
    const msgs={
      sale: (d)=>`⛽ ${station}\nNew Sale Recorded\n${d}`,
      low:  (d)=>`⚠️ ${station}\nLOW STOCK ALERT!\n${d}`,
      user: (d)=>`👤 ${station}\nNew User Registered\n${d}`,
    };
    if(type==='sale'&&s.onSale) this._sendSMS(msgs.sale(detail),s.phone);
    if(type==='low' &&s.onLow)  this._sendSMS(msgs.low(detail), s.phone);
    if(type==='user'&&s.onUser) this._sendSMS(msgs.user(detail),s.phone);
  }

  /* ── KPI BUILDER ── */
  _kpi(label,value,cls,style,sub,fillClr,fillW,paths){
    const bar=fillClr?`<div class="kpi-bar"><div class="kpi-fill" style="width:${Math.min(100,fillW||0)}%;background:${fillClr}"></div></div>`:'';
    return `<div class="kpi-card" style="--kpi-accent:${fillClr||'var(--brand)'}">
      <div class="kpi-top">
        <span class="kpi-label">${label}</span>
        <div class="${cls}" style="${style};width:28px;height:28px;border-radius:6px;display:flex;align-items:center;justify-content:center">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${paths}</svg>
        </div>
      </div>
      <div class="kpi-value">${value}</div>
      <div class="kpi-sub">${sub}</div>
      ${bar}
    </div>`;
  }

  /* ── CHARTS ── */
  _donut(id, petrol, diesel){
    const c = document.getElementById(id); if(!c) return;
    const dpr = devicePixelRatio || 1;
    const sz = 160;
    c.width = sz * dpr; c.height = sz * dpr;
    c.style.width = sz + 'px'; c.style.height = sz + 'px';
    const ctx = c.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, sz, sz);
    const cx = sz / 2, cy = sz / 2, R = 68, thick = 18;
    const dark = document.documentElement.getAttribute('data-theme') === 'dark';
    const tot = petrol + diesel;

    // Background track
    ctx.beginPath();
    ctx.arc(cx, cy, R, 0, Math.PI * 2);
    ctx.strokeStyle = dark ? 'rgba(255,255,255,.06)' : 'rgba(0,0,0,.06)';
    ctx.lineWidth = thick;
    ctx.stroke();

    if(!tot) return;

    const segments = [];
    if(petrol > 0) segments.push({ v: petrol, c1: '#F97316', c2: '#FB923C' });
    if(diesel > 0) segments.push({ v: diesel, c1: '#10B981', c2: '#34D399' });

    let angle = -Math.PI / 2;
    const gap = segments.length > 1 ? 0.04 : 0;

    segments.forEach(seg => {
      const sweep = (seg.v / tot) * Math.PI * 2 - gap;
      // Gradient stroke
      const x1 = cx + Math.cos(angle) * R, y1 = cy + Math.sin(angle) * R;
      const x2 = cx + Math.cos(angle + sweep) * R, y2 = cy + Math.sin(angle + sweep) * R;
      const grad = ctx.createLinearGradient(x1, y1, x2, y2);
      grad.addColorStop(0, seg.c1);
      grad.addColorStop(1, seg.c2);
      ctx.beginPath();
      ctx.arc(cx, cy, R, angle, angle + sweep);
      ctx.strokeStyle = grad;
      ctx.lineWidth = thick;
      ctx.lineCap = 'round';
      ctx.stroke();
      // End cap glow dot
      const ex = cx + Math.cos(angle + sweep) * R, ey = cy + Math.sin(angle + sweep) * R;
      ctx.beginPath();
      ctx.arc(ex, ey, thick / 2 - 1, 0, Math.PI * 2);
      ctx.fillStyle = seg.c2;
      ctx.fill();
      angle += sweep + gap;
    });

    // Center glow
    const glow = ctx.createRadialGradient(cx, cy, 0, cx, cy, R - thick);
    glow.addColorStop(0, dark ? 'rgba(249,115,22,.08)' : 'rgba(249,115,22,.05)');
    glow.addColorStop(1, 'transparent');
    ctx.beginPath();
    ctx.arc(cx, cy, R - thick, 0, Math.PI * 2);
    ctx.fillStyle = glow;
    ctx.fill();
  }

  _dlegend(id, p, d, t){
    const el = document.getElementById(id); if(!el) return;
    const dark = document.documentElement.getAttribute('data-theme') === 'dark';
    const pPct = t ? Math.round(p / t * 100) : 0;
    const dPct = t ? Math.round(d / t * 100) : 0;

    const item = (label, val, pct, c1, c2, barPct) => `
      <div style="margin-bottom:.85rem;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.35rem;">
          <div style="display:flex;align-items:center;gap:.5rem;">
            <div style="width:10px;height:10px;border-radius:50%;background:linear-gradient(135deg,${c1},${c2});flex-shrink:0;box-shadow:0 0 6px ${c1}66;"></div>
            <span style="font-size:.75rem;font-weight:600;color:${dark?'rgba(255,255,255,.7)':'#374151'};">${label}</span>
          </div>
          <div style="text-align:right;">
            <span style="font-size:.8rem;font-weight:800;color:${dark?'#fff':'#111'};letter-spacing:-.02em;">${F.n2(val)}<span style="font-size:.62rem;font-weight:500;color:${dark?'rgba(255,255,255,.4)':'#9ca3af'};margin-left:2px;">L</span></span>
          </div>
        </div>
        <div style="height:5px;border-radius:99px;background:${dark?'rgba(255,255,255,.06)':'rgba(0,0,0,.06)'};overflow:hidden;">
          <div style="height:100%;width:${barPct}%;border-radius:99px;background:linear-gradient(90deg,${c1},${c2});transition:width .6s ease;"></div>
        </div>
        <div style="font-size:.62rem;font-weight:700;color:${c1};margin-top:.25rem;">${pct}%</div>
      </div>`;

    el.innerHTML =
      item('Petrol', p, pPct, '#F97316', '#FB923C', pPct) +
      item('Diesel', d, dPct, '#10B981', '#34D399', dPct);
  }

  _trend(id, ps, month, year){
    const canvas = document.getElementById(id); if(!canvas) return;
    const dpr = devicePixelRatio || 1;
    const w = canvas.parentElement?.offsetWidth || canvas.offsetWidth || 0;
    // If layout not ready yet, retry on next frame (happens on first page load)
    if(w < 10){ requestAnimationFrame(()=>this._trend(id, ps, month, year)); return; }
    const h = 185;
    canvas.width = w * dpr; canvas.height = h * dpr;
    canvas.style.width = w + 'px'; canvas.style.height = h + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);

    const days = new Date(year, month + 1, 0).getDate();
    const pD = new Array(days).fill(0), dD = new Array(days).fill(0);
    ps.forEach(p => {
      const day = new Date(p.date + 'T00:00:00').getDate() - 1;
      if(day >= 0 && day < days){ if(p.fuel === 'Petrol') pD[day] += p.litres; else dD[day] += p.litres; }
    });

    const maxV = Math.max(...pD, ...dD, 1);
    const pad = { t: 18, r: 16, b: 32, l: 48 };
    const cw = w - pad.l - pad.r, ch = h - pad.t - pad.b;
    const dark = document.documentElement.getAttribute('data-theme') === 'dark';
    const tc = dark ? 'rgba(255,255,255,.22)' : 'rgba(0,0,0,.22)';
    const gc = dark ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.05)';

    ctx.font = '500 9.5px DM Sans, system-ui, sans-serif';

    // Grid lines + y-axis labels
    const steps = 4;
    for(let i = 0; i <= steps; i++){
      const y = pad.t + (ch / steps) * i;
      ctx.beginPath();
      ctx.strokeStyle = gc;
      ctx.lineWidth = 1;
      if(i < steps){ ctx.setLineDash([3, 4]); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + cw, y); ctx.stroke(); ctx.setLineDash([]); }
      const val = maxV - (maxV / steps) * i;
      ctx.fillStyle = tc;
      ctx.textAlign = 'right';
      ctx.fillText(val >= 1000 ? (val / 1000).toFixed(1) + 'k' : val.toFixed(0), pad.l - 6, y + 3.5);
    }

    // X-axis labels (every 5 days)
    for(let i = 0; i < days; i++){
      if((i + 1) % 5 !== 0 && i !== 0 && i !== days - 1) continue;
      const x = pad.l + (i / (days - 1 || 1)) * cw;
      ctx.fillStyle = tc;
      ctx.textAlign = 'center';
      ctx.fillText(String(i + 1), x, h - pad.b + 13);
    }

    // Smooth bezier line + gradient fill helper
    const drawLine = (data, c1, c2) => {
      const pts = data.map((v, i) => ({
        x: pad.l + (i / (days - 1 || 1)) * cw,
        y: pad.t + ch - (v / maxV) * ch
      }));

      // Gradient fill
      const gradFill = ctx.createLinearGradient(0, pad.t, 0, pad.t + ch);
      gradFill.addColorStop(0, c1 + '55');
      gradFill.addColorStop(0.6, c1 + '18');
      gradFill.addColorStop(1, c1 + '00');

      ctx.beginPath();
      pts.forEach((p, i) => {
        if(i === 0){ ctx.moveTo(p.x, p.y); return; }
        const prev = pts[i - 1];
        const cpx = (prev.x + p.x) / 2;
        ctx.bezierCurveTo(cpx, prev.y, cpx, p.y, p.x, p.y);
      });
      // Close fill area
      ctx.lineTo(pts[pts.length - 1].x, pad.t + ch);
      ctx.lineTo(pts[0].x, pad.t + ch);
      ctx.closePath();
      ctx.fillStyle = gradFill;
      ctx.fill();

      // Stroke line with gradient
      const gradLine = ctx.createLinearGradient(pad.l, 0, pad.l + cw, 0);
      gradLine.addColorStop(0, c1);
      gradLine.addColorStop(1, c2);
      ctx.beginPath();
      pts.forEach((p, i) => {
        if(i === 0){ ctx.moveTo(p.x, p.y); return; }
        const prev = pts[i - 1];
        const cpx = (prev.x + p.x) / 2;
        ctx.bezierCurveTo(cpx, prev.y, cpx, p.y, p.x, p.y);
      });
      ctx.strokeStyle = gradLine;
      ctx.lineWidth = 2.5;
      ctx.lineJoin = 'round';
      ctx.lineCap = 'round';
      ctx.stroke();

      // Highlight dots on non-zero data points
      pts.forEach((p, i) => {
        if(data[i] === 0) return;
        ctx.beginPath();
        ctx.arc(p.x, p.y, 3.5, 0, Math.PI * 2);
        ctx.fillStyle = c2;
        ctx.fill();
        ctx.beginPath();
        ctx.arc(p.x, p.y, 2, 0, Math.PI * 2);
        ctx.fillStyle = '#fff';
        ctx.fill();
      });
    };

    drawLine(dD, '#10B981', '#34D399');
    drawLine(pD, '#F97316', '#FDBA74');

    // Legend inside chart
    const legends = [['Petrol', '#F97316'], ['Diesel', '#10B981']];
    let lx = pad.l;
    legends.forEach(([lbl, clr]) => {
      ctx.beginPath();
      ctx.arc(lx + 5, pad.t - 5, 4, 0, Math.PI * 2);
      ctx.fillStyle = clr;
      ctx.fill();
      ctx.fillStyle = tc;
      ctx.textAlign = 'left';
      ctx.font = '600 9px DM Sans, system-ui, sans-serif';
      ctx.fillText(lbl, lx + 13, pad.t - 1);
      lx += 60;
    });
  }

  _barChart(id, mData){
    const canvas = document.getElementById(id); if(!canvas) return;
    const dpr = devicePixelRatio || 1;
    const w = canvas.parentElement?.offsetWidth || canvas.offsetWidth || 600;
    const h = 215;
    canvas.width = w * dpr; canvas.height = h * dpr;
    canvas.style.width = w + 'px'; canvas.style.height = h + 'px';
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);

    const maxRev = Math.max(...mData.map(m => m.rev), 1);
    const pad = { t: 18, r: 16, b: 36, l: 58 };
    const cw = w - pad.l - pad.r, ch = h - pad.t - pad.b;
    const dark = document.documentElement.getAttribute('data-theme') === 'dark';
    const tc = dark ? 'rgba(255,255,255,.22)' : 'rgba(0,0,0,.22)';
    const gc = dark ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.05)';
    const curM = new Date().getMonth();

    ctx.font = '500 9.5px DM Sans, system-ui, sans-serif';

    // Grid lines
    for(let i = 0; i <= 4; i++){
      const y = pad.t + (ch / 4) * i, v = maxRev - (maxRev / 4) * i;
      ctx.beginPath(); ctx.strokeStyle = gc; ctx.lineWidth = 1;
      ctx.setLineDash([3, 4]); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + cw, y); ctx.stroke(); ctx.setLineDash([]);
      ctx.fillStyle = tc; ctx.textAlign = 'right';
      ctx.fillText(v >= 1000 ? (v / 1000).toFixed(0) + 'K' : v.toFixed(0), pad.l - 6, y + 3.5);
    }

    const slotW = cw / 12;
    const bw = Math.max(8, Math.min(28, slotW * 0.55));

    mData.forEach((m, i) => {
      const cx = pad.l + slotW * i + slotW / 2;
      const bh = (m.rev / maxRev) * ch;
      const by = pad.t + ch - bh;
      const isCur = i === curM;

      if(bh > 1){
        // Gradient bar
        const grad = ctx.createLinearGradient(0, by, 0, by + bh);
        if(isCur){
          grad.addColorStop(0, '#F97316');
          grad.addColorStop(1, '#FB923C88');
        } else {
          grad.addColorStop(0, dark ? 'rgba(249,115,22,.55)' : 'rgba(249,115,22,.45)');
          grad.addColorStop(1, dark ? 'rgba(249,115,22,.2)' : 'rgba(249,115,22,.15)');
        }
        ctx.beginPath();
        if(ctx.roundRect) ctx.roundRect(cx - bw / 2, by, bw, bh, [4, 4, 0, 0]);
        else ctx.rect(cx - bw / 2, by, bw, bh);
        ctx.fillStyle = grad;
        ctx.fill();

        // Highlight top cap for current month
        if(isCur){
          ctx.beginPath();
          ctx.rect(cx - bw / 2, by, bw, 3);
          ctx.fillStyle = '#fff';
          ctx.globalAlpha = 0.35;
          ctx.fill();
          ctx.globalAlpha = 1;
        }

        // Value label on top of bar (only if bar is tall enough)
        if(bh > 20){
          ctx.fillStyle = isCur ? '#F97316' : tc;
          ctx.textAlign = 'center';
          ctx.font = isCur ? '700 9px DM Sans,sans-serif' : '500 8.5px DM Sans,sans-serif';
          ctx.fillText(m.rev >= 1000 ? (m.rev / 1000).toFixed(0) + 'K' : m.rev.toFixed(0), cx, by - 4);
        }
      }

      // Month label
      ctx.fillStyle = isCur ? '#F97316' : tc;
      ctx.font = isCur ? '700 9.5px DM Sans,sans-serif' : '500 9px DM Sans,sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText(MS[i], cx, h - pad.b + 14);
    });
  }

  /* ── UTILS ── */
  _showLoadingOverlay(){ const el=document.getElementById('loading-overlay'); if(el) el.style.display='flex'; }
  _hideLoadingOverlay(){ const el=document.getElementById('loading-overlay'); if(el) el.style.display='none'; if(window.__clearForceHide) window.__clearForceHide(); }

  _cmap(){ return Object.fromEntries(DB.get('_customers',[]).map(c=>[c.id,c])); }
  closeModal(id){ document.getElementById(id).style.display='none'; }
  _confirm(msg,cb){ document.getElementById('del-msg').textContent=msg; this._delCb=cb; document.getElementById('m-del').style.display='flex'; }
  toast(msg,type='success'){ const el=document.getElementById('toast'); document.getElementById('toast-msg').textContent=msg; el.className='toast '+type+' show'; clearTimeout(this._tt); this._tt=setTimeout(()=>el.classList.remove('show'),3200); }
  _pager(id,cur,tot,cb){ const el=document.getElementById(id); if(!el) return; if(tot<=1){ el.innerHTML=''; return; } let h=`<button class="pg-btn" ${cur<=1?'disabled':''} data-p="${cur-1}">‹</button>`; for(let p=1;p<=tot;p++){ if(tot>7&&p!==1&&p!==tot&&Math.abs(p-cur)>2){ if(p===2||p===tot-1) h+=`<button class="pg-btn" disabled>…</button>`; continue; } h+=`<button class="pg-btn ${p===cur?'on':''}" data-p="${p}">${p}</button>`; } h+=`<button class="pg-btn" ${cur>=tot?'disabled':''} data-p="${cur+1}">›</button>`; el.innerHTML=h; el.querySelectorAll('[data-p]').forEach(b=>b.addEventListener('click',()=>{ const p=+b.dataset.p; if(p>=1&&p<=tot) cb(p); })); }
  _csv(rows,fn){ const csv=rows.map(r=>r.map(v=>`"${String(v||'').replace(/"/g,'""')}"`).join(',')).join('\r\n'); const a=document.createElement('a'); a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'})); a.download=fn+'.csv'; a.click(); }
}

const A=new App();
window.A = A;
document.addEventListener('DOMContentLoaded',()=>{
  A.init();
  // Redraw charts when screen resizes (mobile orientation change etc)
  if(window.ResizeObserver){
    const ro = new ResizeObserver(()=>{
      if(A.page==='dashboard') A.renderDash();
      if(A.page==='monthly')   A.renderMonthly();
      if(A.page==='yearly')    A.renderYearly();
    });
    const main = document.querySelector('.main');
    if(main) ro.observe(main);
  }
});
