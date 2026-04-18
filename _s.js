
pdfjsLib.GlobalWorkerOptions.workerSrc='https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';

const APP_STORAGE_SUPABASE_KEY_PREFIX='user_state_';
const APP_STORAGE_SESSION_KEY='benai_session_app_storage_v1';
const RUNTIME_MEM_SESSION_PREFIX='benai_runtime_mem_';
const RUNTIME_LEADS_SESSION_PREFIX='benai_runtime_leads_';
const appStorageCache=Object.create(null);
let appStorageHydratedForUid='';
let appStoragePersistTimer=null;
let equipeUITab='annuaire';
const APP_STORAGE_CLOUD_EXACT_ALLOWLIST=new Set([
  'benai_login_user',
  'benai_supabase_url',
  'benai_supabase_enabled',
  'benai_theme',
  'benai_access',
  'benai_evol_votes',
  'benai_evol_dismissed',
  'benai_extra_users',
  'benai_hidden_users',
  'benai_name_overrides',
  'benai_patterns',
  'benai_gdrive_client_id',
  'benai_gdrive_last_backup',
  'benai_gdrive_prompt_seen',
  'benai_lead_obj',
  'benai_projets_sugg',
  'benai_bugs',
  'benai_obj_comm',
  'benai_connexions',
  'benai_ventes_mois',
  'benai_roles_v2',
  'benai_annuaire',
  'benai_pending_user_deletes',
  'benai_pending_user_creates'
]);
const APP_STORAGE_CLOUD_PREFIX_ALLOWLIST=[
  'benai_notifs_',
  'benai_tuto_done_',
  'benai_last_motiv_',
  'benai_anniv_seen_',
  // benai_role_guide_ack_ volontairement EXCLU du cloud : sinon chaque hydrate
  // efface la clé avant fusion et un snapshot serveur en retard fait « sauter » la validation.
  'benai_weekly_',
  'benai_briefing_',
  'benai_rr_idx_'
];

function clearAppStorageCache(){
  Object.keys(appStorageCache).forEach(k=>delete appStorageCache[k]);
}
function persistAppStorageCacheToSession(){
  try{
    sessionStorage.setItem(APP_STORAGE_SESSION_KEY,JSON.stringify(appStorageCache));
  }catch(e){}
}
function loadAppStorageCacheFromSession(){
  try{
    const raw=sessionStorage.getItem(APP_STORAGE_SESSION_KEY);
    if(!raw)return;
    const parsed=JSON.parse(raw);
    if(parsed&&typeof parsed==='object'){
      Object.keys(parsed).forEach(k=>{
        appStorageCache[String(k)]=String(parsed[k]);
      });
    }
  }catch(e){}
}

function getRuntimeMemSessionKey(uid){
  return `${RUNTIME_MEM_SESSION_PREFIX}${uid||'guest'}`;
}
function getRuntimeLeadsSessionKey(uid){
  return `${RUNTIME_LEADS_SESSION_PREFIX}${uid||'guest'}`;
}
function persistRuntimeToSession(uid=currentUser?.id){
  if(!uid)return;
  try{
    sessionStorage.setItem(getRuntimeMemSessionKey(uid),JSON.stringify(runtimeMemState||createEmptyMemState()));
    sessionStorage.setItem(getRuntimeLeadsSessionKey(uid),JSON.stringify(Array.isArray(runtimeLeadsState)?runtimeLeadsState:[]));
  }catch(e){}
}
let persistRuntimeMemTimer=null;
function flushPersistRuntimeToSession(uid=currentUser?.id){
  if(persistRuntimeMemTimer){clearTimeout(persistRuntimeMemTimer);persistRuntimeMemTimer=null;}
  persistRuntimeToSession(uid);
}
function schedulePersistRuntimeToSession(uid=currentUser?.id){
  if(!uid)return;
  if(persistRuntimeMemTimer)clearTimeout(persistRuntimeMemTimer);
  persistRuntimeMemTimer=setTimeout(()=>{persistRuntimeMemTimer=null;persistRuntimeToSession(uid);},200);
}
function hydrateRuntimeFromSession(uid=currentUser?.id){
  if(!uid)return false;
  let restored=false;
  try{
    const rawMem=sessionStorage.getItem(getRuntimeMemSessionKey(uid));
    if(rawMem){
      const mem=JSON.parse(rawMem);
      if(mem&&typeof mem==='object'){
        runtimeMemState=createEmptyMemState();
        Object.assign(runtimeMemState,mem);
        restored=true;
      }
    }
  }catch(e){}
  try{
    const rawLeads=sessionStorage.getItem(getRuntimeLeadsSessionKey(uid));
    if(rawLeads){
      const leads=JSON.parse(rawLeads);
      if(Array.isArray(leads)){
        runtimeLeadsState=leads;
        restored=true;
      }
    }
  }catch(e){}
  return restored;
}
function clearRuntimeSession(uid=currentUser?.id){
  if(!uid)return;
  try{
    sessionStorage.removeItem(getRuntimeMemSessionKey(uid));
    sessionStorage.removeItem(getRuntimeLeadsSessionKey(uid));
  }catch(e){}
}

function isAppStorageCloudKey(key){
  const k=String(key||'');
  if(!k)return false;
  // Données sensibles ou synchronisées ailleurs.
  if(k===STORAGE_KEYS.api||k===STORAGE_KEYS.pwds||k===STORAGE_KEYS.mem||k==='benai_attempts')return false;
  if(k===STORAGE_KEYS.sbPublishable||k===STORAGE_KEYS.sbAnonLegacy)return false;
  if(k==='benai_leads'||k==='benai_auto_backup'||k==='benai_auto_backup_date')return false;
  if(k.startsWith('benai_chat_')||k.startsWith('benai_read_'))return false;
  if(k.startsWith('benai_rappel_done_')||k.startsWith('benai_devis_relance_'))return false;
  if(k.startsWith('benai_abs_reminders_'))return false;
  if(APP_STORAGE_CLOUD_EXACT_ALLOWLIST.has(k))return true;
  return APP_STORAGE_CLOUD_PREFIX_ALLOWLIST.some(prefix=>k.startsWith(prefix));
}
function serializeCloudAppStorage(source=appStorageCache){
  const out={};
  const cid=normalizeId(currentUser?.id||'');
  Object.keys(source).forEach(k=>{
    if(!isAppStorageCloudKey(k))return;
    if(k.startsWith('benai_notifs_')){
      const suf=normalizeId(k.slice('benai_notifs_'.length));
      if(cid&&suf!==cid)return;
    }
    out[k]=source[k];
  });
  return out;
}
function getAppStorageSupabaseKey(uid){
  return `${APP_STORAGE_SUPABASE_KEY_PREFIX}${uid||'guest'}`;
}
const appStorage={
  getItem(key){
    const k=String(key);
    return Object.prototype.hasOwnProperty.call(appStorageCache,k)?appStorageCache[k]:null;
  },
  setItem(key,value){
    const k=String(key);
    appStorageCache[k]=String(value);
    persistAppStorageCacheToSession();
    if(currentUser?.id)scheduleAppStoragePersist();
  },
  removeItem(key){
    const k=String(key);
    delete appStorageCache[k];
    persistAppStorageCacheToSession();
    if(currentUser?.id)scheduleAppStoragePersist();
  },
  clear(){
    clearAppStorageCache();
    persistAppStorageCacheToSession();
    if(currentUser?.id)scheduleAppStoragePersist();
  },
  key(index){
    const keys=Object.keys(appStorageCache);
    return keys[index]||null;
  }
};
window.appStorage=appStorage;
loadAppStorageCacheFromSession();

const BENAI_VERSION = '3.15';
const GUIDE_REQUIRED_VERSION='3.15';
const TUTO_DONE_LOCAL_PREFIX='benai_tuto_done_local_';
const STORAGE_KEYS = {
  mem:'benai_v3',
  api:'benai_api',
  pwds:'benai_pwds',
  access:'benai_access',
  rememberedLogin:'benai_login_user',
  sbUrl:'benai_supabase_url',
  sbPublishable:'benai_supabase_publishable_key',
  sbAnonLegacy:'benai_supabase_anon_key',
  sbEnabled:'benai_supabase_enabled'
};
const SUPABASE_DEFAULT_URL='https://wsstfaqyixnsaimbycmw.supabase.co';
// Clé publique uniquement (Publishable key), jamais la service_role.
const SUPABASE_DEFAULT_PUBLISHABLE_KEY='sb_publishable_WDPUls7WDAvZLYPflwl3yg_su4VZ-_C';
const SUPABASE_TABLES=['sav','notes','absences','annuaire','leads','app_settings'];
const SUPABASE_FETCH_PAGE_SIZE=1000;
const SUPABASE_TEST_ENDPOINT='/auth/v1/settings';
const SHARED_AI_SETTING_KEY='shared_ai_api';
const SHARED_CORE_DATA_KEY='shared_core_data_v1';
let supabaseSyncTimer=null;
let lastSupabaseSyncError='';
let supabaseRetryTimer=null;
let supabaseRetryDelayMs=2000;
let supabaseSyncFailStreak=0;
let supabaseSyncWarnTs=0;
let supabaseRealtimeChannel=null;
let supabaseRealtimeDebounceTimer=null;
let supabaseRealtimeReady=false;
let supabaseLastPushOkTs=0;
let supabaseLastPullOkTs=0;
let supabaseLastStatePushWarnTs=0;
let supabaseAuthSubscription=null;
const SUPABASE_FULL_SHARED_SYNC_INTERVAL_MS=2*60*1000;
let supabaseLastSharedSyncTs=0;
let supabaseSharedDirty={sav:false,notes:false,absences:false,annuaire:false,leads:false};
let supabaseSharedSignatures={sav:'',notes:'',absences:'',annuaire:'',leads:''};
let sharedCoreLastUpdatedAt='';
let sharedCoreLastSignature='';
let sharedCoreLastReadOk=false;
let sharedApiKeyRetryTimer=null;
let sharedApiKeyRetryDelayMs=2500;
let pendingSharedApiKey='';
let lastSelfHealToastTs=0;
const LOGIN_REMEMBER_LOCAL_KEY='benai_login_user_local';
// Préparation Supabase (phase ready): stockage session uniquement.
const SUPABASE_CONFIG = {
  url:SUPABASE_DEFAULT_URL,
  publishableKey:SUPABASE_DEFAULT_PUBLISHABLE_KEY,
  enabled: (appStorage.getItem(STORAGE_KEYS.sbEnabled)??'1')==='1',
  table:'benai_state'
};
let supabaseClient=null;
let currentSupabaseSession=null;
let currentAuthMode='unknown';
let runtimeMemState={sav:[],messages:{},msg_deletions:{},msg_read_cursor:{},activity:[],notes:[],absences:[],tokens:{}};
let runtimeLeadsState=[];
const LEAD_SOURCE_DEFS=[
  {code:'MAG',icon:'📍',label:'Magasin'},
  {code:'TEL',icon:'📞',label:'Téléphone'},
  {code:'WEB',icon:'🌐',label:'Web'},
  {code:'PARRAINAGE',icon:'🤝',label:'Parrainage'},
  {code:'FOIRE',icon:'🎪',label:'Foire'},
  {code:'ANCIEN_CLIENT',icon:'👤',label:'Ancien client'},
  {code:'ACTIF',icon:'🏃',label:'Actif'}
];
const LEAD_SOURCE_ICONS=LEAD_SOURCE_DEFS.reduce((acc,src)=>{acc[src.code]=src.icon;return acc;},{});
const LEAD_SOURCE_LABELS=LEAD_SOURCE_DEFS.reduce((acc,src)=>{acc[src.code]=src.label;return acc;},{});
const LEAD_SOURCE_CODES=LEAD_SOURCE_DEFS.map(src=>src.code);

function normalizeSupabaseUrl(url){
  let out=(url||'').trim();
  if(out&&out.startsWith('http://'))out='https://'+out.slice(7);
  if(out&&out.startsWith('https://https://'))out=out.replace('https://https://','https://');
  if(out&&!out.startsWith('https://')&&out.includes('.supabase.co'))out='https://'+out;
  return out;
}
function isLikelySupabasePublicKey(key){
  if(!key)return false;
  const k=String(key).trim();
  if(!k)return false;
  // On refuse explicitement les clés sensibles.
  if(k.startsWith('sb_secret_'))return false;
  if(/service[_-]?role/i.test(k))return false;
  // Formats publics connus : nouvelle publishable ou ancienne anon JWT.
  return k.startsWith('sb_publishable_')||/^eyJ/.test(k);
}
function isLikelySupabaseProjectUrl(url){
  const u=normalizeSupabaseUrl(url);
  return !!u&&u.startsWith('https://')&&u.includes('.supabase.co');
}

function setStatusMessage(element,color,text){
  if(!element)return;
  element.style.color=color;
  element.textContent=text;
}

async function fetchWithTimeout(url,options={},timeoutMs=10000){
  const controller=new AbortController();
  const timer=setTimeout(()=>controller.abort(),timeoutMs);
  try{
    return await fetch(url,{...options,signal:controller.signal});
  }finally{
    clearTimeout(timer);
  }
}

async function fetchSupabaseRowsPaged(table,headers,select='*'){
  const rows=[];
  let from=0;
  while(true){
    const to=from+SUPABASE_FETCH_PAGE_SIZE-1;
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/${table}?select=${encodeURIComponent(select)}`,{
      headers:{
        ...headers,
        'Range-Unit':'items',
        'Range':`${from}-${to}`,
        'Prefer':'count=exact'
      }
    },10000);
    if(!res.ok){
      let detail='';
      try{detail=await res.text();}catch{}
      throw new Error(`${table}: lecture ${res.status} ${detail||''}`.trim());
    }
    const chunk=await res.json();
    if(Array.isArray(chunk)&&chunk.length){
      rows.push(...chunk);
    }
    if(!Array.isArray(chunk)||chunk.length<SUPABASE_FETCH_PAGE_SIZE){
      break;
    }
    from+=SUPABASE_FETCH_PAGE_SIZE;
  }
  return rows;
}

function applySupabaseConfigFromStorage(){
  const storedUrl=appStorage.getItem(STORAGE_KEYS.sbUrl)||SUPABASE_DEFAULT_URL;
  const rawStoredKey=(appStorage.getItem(STORAGE_KEYS.sbPublishable)||appStorage.getItem(STORAGE_KEYS.sbAnonLegacy)||'').trim();
  const normalizedUrl=normalizeSupabaseUrl(storedUrl)||SUPABASE_DEFAULT_URL;
  const storedKey=isLikelySupabasePublicKey(rawStoredKey)?rawStoredKey:SUPABASE_DEFAULT_PUBLISHABLE_KEY;
  const hasValidConfig=isLikelySupabaseProjectUrl(normalizedUrl)&&isLikelySupabasePublicKey(storedKey);
  SUPABASE_CONFIG.url=hasValidConfig?normalizedUrl:'';
  SUPABASE_CONFIG.publishableKey=hasValidConfig?storedKey.trim():'';
  SUPABASE_CONFIG.enabled=((appStorage.getItem(STORAGE_KEYS.sbEnabled)??'1')==='1')&&hasValidConfig;
}
applySupabaseConfigFromStorage();

function getSupabaseHeaders(){
  if(isSwitchPreviewSession())return null;
  applySupabaseConfigFromStorage();
  const key=SUPABASE_CONFIG.publishableKey;
  if(!key)return null;
  const headers={'Content-Type':'application/json','apikey':key};
  const accessToken=currentSupabaseSession?.access_token||'';
  if(accessToken){
    headers.Authorization='Bearer '+accessToken;
    return headers;
  }
  // Sans session utilisateur : PostgREST attend toujours Authorization Bearer + apikey (JWT anon ou publishable).
  if(/^eyJ/.test(key)||String(key).startsWith('sb_publishable_'))headers.Authorization='Bearer '+key;
  return headers;
}

function hasSupabaseDataAuth(){
  return !!String(SUPABASE_CONFIG.publishableKey||'').trim();
}

function fmtRuntimeTs(ts){
  if(!ts)return '—';
  try{return new Date(ts).toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit',second:'2-digit'});}catch{return '—';}
}

function renderSupabaseRuntimeStatus(){
  const sbMsg=document.getElementById('supabase-msg');
  if(!sbMsg)return;
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url){
    setStatusMessage(sbMsg,'var(--y)','⚠️ Supabase désactivé');
    return;
  }
  if(lastSupabaseSyncError){
    setStatusMessage(sbMsg,'var(--r)',`⚠️ Sync cloud: ${lastSupabaseSyncError}`);
    return;
  }
  const pushTxt=supabaseLastPushOkTs?`push ${fmtRuntimeTs(supabaseLastPushOkTs)}`:'push —';
  const pullTxt=supabaseLastPullOkTs?`pull ${fmtRuntimeTs(supabaseLastPullOkTs)}`:'pull —';
  const rtTxt=supabaseRealtimeReady?'RT OK':'RT polling';
  setStatusMessage(sbMsg,'var(--g)',`✅ Sync active (${pushTxt} · ${pullTxt} · ${rtTxt})`);
}

function computeSupabaseSignature(value){
  try{return JSON.stringify(value??null);}catch{return String(Date.now());}
}

function markSharedDirtyIfChanged(key,value){
  const sig=computeSupabaseSignature(value);
  if(sig!==supabaseSharedSignatures[key]){
    supabaseSharedSignatures[key]=sig;
    supabaseSharedDirty[key]=true;
    return true;
  }
  return false;
}

function refreshSharedSignatures(markDirty=false){
  const mem=getMem();
  supabaseSharedSignatures.sav=computeSupabaseSignature(mem.sav||[]);
  supabaseSharedSignatures.notes=computeSupabaseSignature(mem.notes||[]);
  supabaseSharedSignatures.absences=computeSupabaseSignature(mem.absences||[]);
  supabaseSharedSignatures.annuaire=computeSupabaseSignature(getAnnuaire()||[]);
  supabaseSharedSignatures.leads=computeSupabaseSignature(getLeads()||[]);
  if(markDirty){
    supabaseSharedDirty={sav:true,notes:true,absences:true,annuaire:true,leads:true};
    return;
  }
  supabaseSharedDirty={sav:false,notes:false,absences:false,annuaire:false,leads:false};
}

function buildSharedCoreDataPayload(mem,annuaire,leads){
  const msgs=mem&&typeof mem.messages==='object'&&mem.messages?mem.messages:{};
  const del=mem&&typeof mem.msg_deletions==='object'&&mem.msg_deletions?mem.msg_deletions:{};
  const readC=mem&&typeof mem.msg_read_cursor==='object'&&mem.msg_read_cursor?mem.msg_read_cursor:{};
  return {
    version:1,
    updated_at:new Date().toISOString(),
    data:{
      sav:Array.isArray(mem?.sav)?mem.sav:[],
      notes:Array.isArray(mem?.notes)?mem.notes:[],
      absences:Array.isArray(mem?.absences)?mem.absences:[],
      annuaire:Array.isArray(annuaire)?annuaire:[],
      leads:Array.isArray(leads)?leads:[],
      notif_feed:[],
      messages:msgs,
      msg_deletions:del,
      msg_read_cursor:readC
    }
  };
}

function buildSupabaseStatePayload(mem,annuaire,leads){
  return {
    version:2,
    saved_at:new Date().toISOString(),
    mem:{
      ...createEmptyMemState(),
      ...(mem&&typeof mem==='object'?mem:{})
    },
    annuaire:Array.isArray(annuaire)?annuaire:[],
    leads:Array.isArray(leads)?leads:[]
  };
}

function mergeByKey(remoteList,localList,keyGetter,conflictResolver){
  const map=new Map();
  (Array.isArray(remoteList)?remoteList:[]).forEach(item=>{
    const key=String(keyGetter(item)||'').trim();
    if(!key)return;
    map.set(key,item);
  });
  (Array.isArray(localList)?localList:[]).forEach(item=>{
    const key=String(keyGetter(item)||'').trim();
    if(!key)return;
    if(!map.has(key)){map.set(key,item);return;}
    const prev=map.get(key);
    map.set(key,conflictResolver(prev,item));
  });
  return Array.from(map.values());
}

function pickLatestLead(prev,next){
  const a=Date.parse(prev?.date_modification||prev?.date_creation||0)||0;
  const b=Date.parse(next?.date_modification||next?.date_creation||0)||0;
  return b>=a?next:prev;
}

function pickLatestNote(prev,next){
  const a=Number(prev?.ts||0);
  const b=Number(next?.ts||0);
  return b>=a?next:prev;
}

function pickLatestSav(prev,next){
  const a=Number(prev?.sync_ts||Date.parse(prev?.date_creation||0)||0);
  const b=Number(next?.sync_ts||Date.parse(next?.date_creation||0)||0);
  return b>=a?next:prev;
}

function pickLatestAbsence(prev,next){
  const a=Number(prev?.sync_ts||Date.parse(prev?.createdAt||prev?.debut||0)||0);
  const b=Number(next?.sync_ts||Date.parse(next?.createdAt||next?.debut||0)||0);
  return b>=a?next:prev;
}

function pickLatestAnnuaire(prev,next){
  const a=Number(prev?.sync_ts||0);
  const b=Number(next?.sync_ts||0);
  return b>=a?next:prev;
}

function mergeNotifFeeds(remoteFeed,localFeed){
  const map=new Map();
  const add=(arr)=>{
    (Array.isArray(arr)?arr:[]).forEach(x=>{
      if(!x||String(x.target_uid||'').trim()==='')return;
      const id=Number(x.id);
      if(!Number.isFinite(id)||id<=0)return;
      map.set(id,x);
    });
  };
  add(remoteFeed);
  add(localFeed);
  return Array.from(map.values()).sort((a,b)=>Number(b.ts||b.id||0)-Number(a.ts||a.id||0)).slice(0,200);
}
function crossNotifMsgKey(m){
  return `${String(m?.from||'')}:${Number(m?.ts)||0}:${String(m?.text||'').slice(0,120)}`;
}
function mergeMsgDeletions(remoteDel,localDel){
  const r=remoteDel&&typeof remoteDel==='object'?remoteDel:{};
  const l=localDel&&typeof localDel==='object'?localDel:{};
  const out={};
  const cids=new Set([...Object.keys(r),...Object.keys(l)]);
  const cap=400;
  cids.forEach(cid=>{
    const seen=new Set();
    const arr=[];
    [...(Array.isArray(r[cid])?r[cid]:[]),...(Array.isArray(l[cid])?l[cid]:[])].forEach(k=>{
      const s=String(k||'').trim();
      if(!s||seen.has(s))return;
      seen.add(s);
      arr.push(s);
    });
    if(arr.length>cap)arr.splice(0,arr.length-cap);
    if(arr.length)out[cid]=arr;
  });
  return out;
}
function mergeMsgReadCursors(remoteMap,localMap){
  const r=remoteMap&&typeof remoteMap==='object'?remoteMap:{};
  const l=localMap&&typeof localMap==='object'?localMap:{};
  const cids=new Set([...Object.keys(r),...Object.keys(l)]);
  const out={};
  cids.forEach(cid=>{
    const a=r[cid]&&typeof r[cid]==='object'?r[cid]:{};
    const b=l[cid]&&typeof l[cid]==='object'?l[cid]:{};
    const uids=new Set([...Object.keys(a),...Object.keys(b)]);
    if(!uids.size)return;
    const merged={};
    uids.forEach(uid=>{
      const ta=Number(a[uid]||0);
      const tb=Number(b[uid]||0);
      const mx=Math.max(ta,tb);
      if(mx>0)merged[uid]=mx;
    });
    if(Object.keys(merged).length)out[cid]=merged;
  });
  return out;
}
function mergeMessagesMap(remoteMap,localMap,msgDeletionsByCid){
  const del=msgDeletionsByCid&&typeof msgDeletionsByCid==='object'?msgDeletionsByCid:{};
  const r=remoteMap&&typeof remoteMap==='object'?remoteMap:{};
  const l=localMap&&typeof localMap==='object'?localMap:{};
  const out={};
  const cids=new Set([...Object.keys(r),...Object.keys(l)]);
  cids.forEach(cid=>{
    const a=Array.isArray(r[cid])?r[cid]:[];
    const b=Array.isArray(l[cid])?l[cid]:[];
    const delSet=new Set(Array.isArray(del[cid])?del[cid]:[]);
    const by=new Map();
    [...a,...b].forEach(m=>{
      if(!m||typeof m!=='object')return;
      const k=crossNotifMsgKey(m);
      if(delSet.has(k))return;
      const prev=by.get(k);
      if(!prev||(Number(m.ts||0)>Number(prev.ts||0))||(m.edited&&!prev.edited))by.set(k,m);
    });
    const merged=Array.from(by.values()).sort((x,y)=>Number(x.ts||0)-Number(y.ts||0));
    if(merged.length)out[cid]=merged;
  });
  return out;
}
function countMessagesInMap(map){
  if(!map||typeof map!=='object')return 0;
  return Object.values(map).reduce((s,a)=>s+(Array.isArray(a)?a.length:0),0);
}
function getCrossNotifFeedAppliedSet(){
  try{
    const raw=sessionStorage.getItem('benai_cross_notif_feed_applied')||'[]';
    const arr=JSON.parse(raw);
    return new Set((Array.isArray(arr)?arr:[]).map(Number).filter(x=>Number.isFinite(x)&&x>0));
  }catch{
    return new Set();
  }
}
function rememberCrossNotifFeedId(id){
  const s=getCrossNotifFeedAppliedSet();
  s.add(Number(id));
  sessionStorage.setItem('benai_cross_notif_feed_applied',JSON.stringify([...s].slice(-500)));
}
function applyInboxNotifsFromSharedFeed(feed){
  if(!currentUser?.id)return;
  const me=normalizeId(currentUser.id);
  const applied=getCrossNotifFeedAppliedSet();
  let added=0;
  (Array.isArray(feed)?feed:[]).forEach(item=>{
    if(!item||normalizeId(String(item.target_uid||''))!==me)return;
    const fid=Number(item.id);
    if(!Number.isFinite(fid)||fid<=0||applied.has(fid))return;
    const key='benai_notifs_'+currentUser.id;
    try{
      const notifs=JSON.parse(appStorage.getItem(key)||'[]');
      if(notifs.some(x=>Number(x.id)===fid))return;
      const row={
        id:fid,
        titre:item.titre||'',
        msg:item.msg||'',
        icon:item.icon||'🔔',
        time:item.time||new Date().toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'}),
        read:false
      };
      notifs.unshift(row);
      if(notifs.length>50)notifs.splice(50);
      appStorage.setItem(key,JSON.stringify(notifs));
      rememberCrossNotifFeedId(fid);
      applied.add(fid);
      added++;
      if(shouldUseBrowserOSNotifications()&&'Notification' in window&&Notification.permission==='granted'){
        try{
          new Notification('BenAI — '+String(item.titre||''),{body:String(item.msg||'')});
        }catch(_){}
      }
    }catch(_){}
  });
  if(added){
    refreshNotifBadge();
    try{playNotifSound();}catch(_){}
  }
}
function mergeSharedCoreData(remoteData,localData,mode='pull'){
  const remote=remoteData||{};
  const local=localData||{};
  const msgDel=mergeMsgDeletions(remote.msg_deletions,local.msg_deletions);
  return {
    sav:mergeByKey(remote.sav,local.sav,x=>x?.id,pickLatestSav),
    notes:mergeByKey(remote.notes,local.notes,x=>x?.id,pickLatestNote),
    absences:mergeByKey(remote.absences,local.absences,x=>x?.id,pickLatestAbsence),
    annuaire:mergeByKey(remote.annuaire,local.annuaire,x=>x?.id,pickLatestAnnuaire),
    leads:mergeByKey(remote.leads,local.leads,x=>x?.id,pickLatestLead),
    notif_feed:mergeNotifFeeds(remote.notif_feed,local.notif_feed),
    messages:mergeMessagesMap(remote.messages,local.messages,msgDel),
    msg_deletions:msgDel,
    msg_read_cursor:mergeMsgReadCursors(remote.msg_read_cursor,local.msg_read_cursor)
  };
}

async function fetchSharedCoreDataRaw(headers){
  const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/app_settings?key=eq.${SHARED_CORE_DATA_KEY}&select=value`,{headers},10000);
  if(!res.ok)return {ok:false,data:null,updatedAt:''};
  const rows=await res.json();
  const value=rows?.[0]?.value||{};
  return {
    ok:true,
    data:value?.data||{},
    updatedAt:String(value?.updated_at||'')
  };
}

async function pushSharedCoreDataToSupabase(mem,annuaire,leads,notifFeedAppend){
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return false;
  const headers=getSupabaseHeaders();
  if(!headers)return false;
  try{
    const localData=buildSharedCoreDataPayload(mem,annuaire,leads).data;
    const remoteRaw=await fetchSharedCoreDataRaw(headers);
    const mergedData=mergeSharedCoreData(remoteRaw.ok?remoteRaw.data:{},localData,'push');
    if(Array.isArray(notifFeedAppend)&&notifFeedAppend.length){
      mergedData.notif_feed=mergeNotifFeeds(mergedData.notif_feed||[],notifFeedAppend);
    }
    const payload={
      version:1,
      updated_at:new Date().toISOString(),
      data:mergedData
    };
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/app_settings?on_conflict=key`,{
      method:'POST',
      headers:{...headers,'Prefer':'resolution=merge-duplicates,return=minimal'},
      body:JSON.stringify([{key:SHARED_CORE_DATA_KEY,value:payload,updated_at:payload.updated_at}])
    },10000);
    if(!res.ok)return false;
    saveMem({...createEmptyMemState(),...getMem(),sav:mergedData.sav,notes:mergedData.notes,absences:mergedData.absences,messages:mergedData.messages||{},msg_deletions:mergedData.msg_deletions||{},msg_read_cursor:mergedData.msg_read_cursor||{}},false);
    saveAnnuaire(mergedData.annuaire,false);
    saveLeads(mergedData.leads,false);
    refreshSharedSignatures(false);
    sharedCoreLastUpdatedAt=payload.updated_at;
    sharedCoreLastSignature=computeSupabaseSignature(mergedData);
    return true;
  }catch(e){
    return false;
  }
}

async function loadSharedCoreDataFromSupabase(force=false){
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!currentUser)return false;
  const headers=getSupabaseHeaders();
  if(!headers)return false;
  sharedCoreLastReadOk=false;
  try{
    const remoteRaw=await fetchSharedCoreDataRaw(headers);
    if(!remoteRaw.ok)return false;
    sharedCoreLastReadOk=true;
    const data=remoteRaw.data||{};
    const updatedAt=String(remoteRaw.updatedAt||'');
    const remoteSig=computeSupabaseSignature(data);
    // Anti-désync multi-appareils: ne pas dépendre uniquement des horloges.
    // On saute seulement si le contenu est identique.
    if(!force&&remoteSig&&remoteSig===sharedCoreLastSignature)return false;
    const remoteTotal=(data.sav?.length||0)+(data.notes?.length||0)+(data.absences?.length||0)+(data.annuaire?.length||0)+(data.leads?.length||0)+countMessagesInMap(data.messages)+(Array.isArray(data.notif_feed)?data.notif_feed.length:0);
    const localMemBefore=getMem();
    const localTotal=(localMemBefore.sav?.length||0)+(localMemBefore.notes?.length||0)+(localMemBefore.absences?.length||0)+(getAnnuaire()?.length||0)+(getLeads()?.length||0)+countMessagesInMap(localMemBefore.messages);
    if(remoteTotal===0&&localTotal>0)return false;
    const lrMap=getLastRead(currentUser.id);
    const unreadMsgsBefore=countTotalInternalUnread(currentUser.id,localMemBefore,lrMap);
    const merged=mergeSharedCoreData(data,{
      sav:localMemBefore.sav||[],
      notes:localMemBefore.notes||[],
      absences:localMemBefore.absences||[],
      annuaire:getAnnuaire()||[],
      leads:getLeads()||[],
      messages:localMemBefore.messages||{},
      msg_deletions:localMemBefore.msg_deletions||{},
      msg_read_cursor:localMemBefore.msg_read_cursor||{},
      notif_feed:[]
    },'pull');
    saveMem({
      ...createEmptyMemState(),
      ...localMemBefore,
      sav:merged.sav,
      notes:merged.notes,
      absences:merged.absences,
      messages:merged.messages||{},
      msg_deletions:merged.msg_deletions||{},
      msg_read_cursor:merged.msg_read_cursor||{}
    },false);
    saveAnnuaire(merged.annuaire,false);
    saveLeads(merged.leads,false);
    applyInboxNotifsFromSharedFeed(merged.notif_feed);
    const unreadMsgsAfter=countTotalInternalUnread(currentUser.id,getMem(),lrMap);
    refreshMsgBadge();
    if(unreadMsgsAfter>unreadMsgsBefore)try{playNotifSound();}catch(_){}
    refreshSharedSignatures(false);
    if(updatedAt)sharedCoreLastUpdatedAt=updatedAt;
    sharedCoreLastSignature=computeSupabaseSignature(merged);
    return true;
  }catch(e){
    return false;
  }
}

async function loadCoreDataFromSupabaseStateSnapshot(){
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!currentUser?.id)return false;
  const headers=getSupabaseHeaders();
  if(!headers)return false;
  try{
    const res=await fetchWithTimeout(
      `${SUPABASE_CONFIG.url}/rest/v1/${SUPABASE_CONFIG.table}?uid=eq.${encodeURIComponent(currentUser.id)}&select=payload,updated_at`,
      {headers},
      10000
    );
    if(!res.ok)return false;
    const rows=await res.json();
    const payload=rows?.[0]?.payload;
    if(!payload||typeof payload!=='object')return false;
    const snapMem=payload.mem&&typeof payload.mem==='object'?payload.mem:payload;
    const localBefore=getMem();
    const lrSnap=getLastRead(currentUser.id);
    const unreadMsgsBeforeSnap=countTotalInternalUnread(currentUser.id,localBefore,lrSnap);
    const mergedMem={
      ...createEmptyMemState(),
      ...localBefore,
      ...snapMem,
      sav:Array.isArray(snapMem.sav)?snapMem.sav:[],
      notes:Array.isArray(snapMem.notes)?snapMem.notes:[],
      absences:Array.isArray(snapMem.absences)?snapMem.absences:[]
    };
    saveMem(mergedMem,false);
    if(Array.isArray(payload.annuaire))saveAnnuaire(payload.annuaire,false);
    if(Array.isArray(payload.leads))saveLeads(payload.leads,false);
    const unreadMsgsAfterSnap=countTotalInternalUnread(currentUser.id,getMem(),lrSnap);
    refreshMsgBadge();
    if(unreadMsgsAfterSnap>unreadMsgsBeforeSnap)try{playNotifSound();}catch(_){}
    refreshSharedSignatures(false);
    return true;
  }catch(e){
    return false;
  }
}

function normalizeAnthropicKey(raw){
  return String(raw||'').trim().replace(/^["']|["']$/g,'');
}

function isLikelyAnthropicKey(raw){
  const key=normalizeAnthropicKey(raw);
  return key.startsWith('sk-ant-')&&key.length>=20;
}

function useMobilePersistedSupabaseAuth(){
  // Session persistée sur tous les appareils pour stabiliser la synchro cloud.
  return true;
}

function getSupabaseClient(){
  applySupabaseConfigFromStorage();
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!SUPABASE_CONFIG.publishableKey)return null;
  if(!window.supabase?.createClient)return null;
  if(!supabaseClient){
    const persistSession=useMobilePersistedSupabaseAuth();
    supabaseClient=window.supabase.createClient(SUPABASE_CONFIG.url,SUPABASE_CONFIG.publishableKey,{
      // Session persistée seulement sur « mobile » (largeur ≤768px) : rester connecté après fermeture d’onglet.
      // Sur ordi (fenêtre large) : pas de stockage de session Auth — à reconnecter après fermeture du navigateur.
      // Les données métier restent sur Supabase dans tous les cas.
      auth:{persistSession,autoRefreshToken:true,detectSessionInUrl:true}
    });
    try{
      const sub=supabaseClient.auth.onAuthStateChange((_event,session)=>{
        currentSupabaseSession=session||null;
        if(session?.access_token){
          currentAuthMode='supabase';
        }else if(currentAuthMode==='supabase'){
          currentAuthMode='unknown';
        }
      });
      supabaseAuthSubscription=sub?.data?.subscription||null;
    }catch(e){}
  }
  return supabaseClient;
}

async function ensureSupabaseSession(){
  if(isSwitchPreviewSession())return null;
  if(currentSupabaseSession?.access_token)return currentSupabaseSession;
  const client=getSupabaseClient();
  if(!client)return null;
  try{
    const {data}=await client.auth.getSession();
    currentSupabaseSession=data?.session||null;
    if(currentSupabaseSession?.access_token){
      currentAuthMode='supabase';
      return currentSupabaseSession;
    }
    // Récupération douce: retente un refresh si un token est présent en storage navigateur.
    const refreshed=await client.auth.refreshSession();
    currentSupabaseSession=refreshed?.data?.session||null;
    if(currentSupabaseSession?.access_token)currentAuthMode='supabase';
    return currentSupabaseSession;
  }catch(e){
    currentSupabaseSession=null;
    return null;
  }
}
function shouldMonitorSupabaseSession(){
  if(!SUPABASE_CONFIG.enabled)return false;
  if(isSwitchPreviewSession())return false;
  if(currentAuthMode==='supabase')return true;
  return !!currentSupabaseSession?.access_token;
}

async function persistAppStorageToSupabaseNow(uid=currentUser?.id,snapshot=null){
  if(!uid||!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return false;
  await ensureSupabaseSession();
  if(!hasSupabaseDataAuth())return false;
  const headers=getSupabaseHeaders();
  if(!headers)return false;
  try{
    const cloudPayload=(snapshot&&typeof snapshot==='object'&&!Array.isArray(snapshot))
      ?snapshot
      :serializeCloudAppStorage();
    const body=[{
      key:getAppStorageSupabaseKey(uid),
      value:cloudPayload
    }];
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/app_settings?on_conflict=key`,{
      method:'POST',
      headers:{...headers,'Prefer':'resolution=merge-duplicates,return=minimal'},
      body:JSON.stringify(body)
    },8000);
    return !!res.ok;
  }catch(e){
    return false;
  }
}
function scheduleAppStoragePersist(){
  if(appStoragePersistTimer)clearTimeout(appStoragePersistTimer);
  appStoragePersistTimer=setTimeout(()=>{
    appStoragePersistTimer=null;
    void persistAppStorageToSupabaseNow();
  },1200);
}
async function hydrateAppStorageFromSupabase(uid=currentUser?.id,force=false){
  if(!uid)return false;
  if(appStorageHydratedForUid&&appStorageHydratedForUid!==uid){
    clearAppStorageCache();
    persistAppStorageCacheToSession();
    appStorageHydratedForUid='';
  }
  // Déconnexion : resetAppStorageRuntime() vide la RAM sans réécrire sessionStorage.
  // Les clés hors snapshot cloud (ex. benai_read_*) restent dans sessionStorage — on les
  // recharge ici pour éviter badges / notifs « non lus » sur les messages déjà vus.
  loadAppStorageCacheFromSession();
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return false;
  if(appStorageHydratedForUid===uid&&!force)return true;
  await ensureSupabaseSession();
  if(!hasSupabaseDataAuth())return false;
  const headers=getSupabaseHeaders();
  if(!headers)return false;
  try{
    const storageKey=getAppStorageSupabaseKey(uid);
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/app_settings?key=eq.${encodeURIComponent(storageKey)}&select=value`,{headers},8000);
    if(!res.ok)return false;
    const rows=await res.json();
    const raw=rows?.[0]?.value;
    const obj=(raw&&typeof raw==='object'&&!Array.isArray(raw))?raw:{};
    // Ne pas effacer les clés locales (anti-spam, rappels, état UI local).
    // On remplace uniquement les clés gérées par le snapshot cloud.
    const mergedCache={...appStorageCache};
    Object.keys(mergedCache).forEach(k=>{
      if(isAppStorageCloudKey(k))delete mergedCache[k];
    });
    Object.entries(obj).forEach(([k,v])=>{
      if(v===null||v===undefined)return;
      mergedCache[String(k)]=typeof v==='string'?v:JSON.stringify(v);
    });
    clearAppStorageCache();
    Object.assign(appStorageCache,mergedCache);
    persistAppStorageCacheToSession();
    appStorageHydratedForUid=uid;
    applySupabaseConfigFromStorage();
    return true;
  }catch(e){
    return false;
  }
}
function resetAppStorageRuntime(preserveSessionCopy=false){
  clearAppStorageCache();
  if(preserveSessionCopy)persistAppStorageCacheToSession();
  if(!preserveSessionCopy)clearRuntimeSession(currentUser?.id);
  runtimeMemState={sav:[],messages:{},msg_deletions:{},msg_read_cursor:{},activity:[],notes:[],absences:[],tokens:{}};
  runtimeLeadsState=[];
  supabaseSharedDirty={sav:false,notes:false,absences:false,annuaire:false,leads:false};
  supabaseSharedSignatures={sav:'',notes:'',absences:'',annuaire:'',leads:''};
  supabaseLastSharedSyncTs=0;
  sharedCoreLastUpdatedAt='';
  sharedCoreLastSignature='';
  appStorageHydratedForUid='';
}

function getBuiltinLoginEmails(){
  return {
    benjamin:'b.muller@monsieur-store.net'
  };
}

function getEmailCandidateForUid(uid,rawLogin=''){
  const normalized=normalizeId(uid);
  const raw=String(rawLogin||'').trim().toLowerCase();
  if(raw.includes('@'))return raw;
  if(!normalized)return '';
  if(normalized.includes('@'))return normalized;
  const builtinEmail=getBuiltinLoginEmails()[normalized];
  if(builtinEmail)return builtinEmail;
  const ann=getAnnuaireActive();
  const directMatch=ann.find(e=>normalizeId(e.prenom)===normalized||normalizeId(`${e.prenom} ${e.nom}`)===normalized);
  if(directMatch)return (directMatch.emailPro||directMatch.email||'').trim().toLowerCase();
  const extra=getExtraUserById(normalized);
  if(extra?.email)return String(extra.email).trim().toLowerCase();
  return '';
}

/** Résout app_uid (pseudo BenAI) → email via Edge Function (service role). */
async function fetchLookupLoginEmail(rawLogin){
  const raw=String(rawLogin||'').trim();
  if(!raw||raw.includes('@'))return '';
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!SUPABASE_CONFIG.publishableKey)return '';
  try{
    const fnUrl=`${SUPABASE_CONFIG.url}/functions/v1/lookup-login-email`;
    const res=await fetchWithTimeout(fnUrl,{
      method:'POST',
      headers:{
        'Content-Type':'application/json',
        apikey:SUPABASE_CONFIG.publishableKey,
        Authorization:'Bearer '+SUPABASE_CONFIG.publishableKey
      },
      body:JSON.stringify({login:raw})
    },8000);
    if(!res.ok)return '';
    const j=await res.json().catch(()=>({}));
    const e=String(j?.email||'').trim().toLowerCase();
    return e.includes('@')?e:'';
  }catch{
    return '';
  }
}

/** Email utilisé pour auth.signInWithPassword (Supabase n’accepte que l’email). */
async function resolveSupabaseAuthEmail(rawLogin){
  const raw=String(rawLogin||'').trim();
  if(!raw)return '';
  if(raw.includes('@'))return raw.toLowerCase();
  const fromEdge=await fetchLookupLoginEmail(raw);
  if(fromEdge)return fromEdge;
  const uid=normalizeId(raw);
  return getEmailCandidateForUid(uid,raw)||'';
}

async function fetchSupabaseProfile(accessToken,userAuthId=''){
  if(!accessToken)return null;
  try{
    const idFilter=userAuthId?`&id=eq.${encodeURIComponent(userAuthId)}`:'';
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/profiles?select=id,email,full_name,role,company,app_uid${idFilter}`,{
      headers:{
        apikey:SUPABASE_CONFIG.publishableKey,
        Authorization:'Bearer '+accessToken
      }
    },8000);
    if(!res.ok)return null;
    const rows=await res.json();
    return rows?.[0]||null;
  }catch(e){
    return null;
  }
}

function getColorForRole(role){
  return role==='admin'?'linear-gradient(135deg,#E8943A,#B45309)'
    : role==='assistante'?'linear-gradient(135deg,#22C55E,#15803D)'
    : role==='commercial'?'linear-gradient(135deg,#F472B6,#DB2777)'
    : role==='directeur_co'?'linear-gradient(135deg,#A78BFA,#7C3AED)'
    : role==='directeur_general'?'linear-gradient(135deg,#94A3B8,#475569)'
    : 'linear-gradient(135deg,#60A5FA,#1D4ED8)';
}

function makeUserFromSupabaseProfile(profile,emailFallback=''){
  if(!profile)return null;
  const parts=String(profile.full_name||'Utilisateur').trim().split(/\s+/).filter(Boolean);
  const firstName=parts[0]||'Utilisateur';
  const emailSlug=normalizeId(String((profile.email||emailFallback||'').split('@')[0]||firstName));
  let uid=normalizeId(profile.app_uid||emailSlug||firstName);
  if(profile.role==='admin'&&/benjamin/i.test(String(profile.full_name||'')))uid='benjamin';
  return {
    id:uid,
    auth_uid:profile.id||'',
    email:(profile.email||emailFallback||'').trim().toLowerCase(),
    name:profile.full_name||firstName,
    role:profile.role||'assistante',
    societe:profile.company||'nemausus',
    color:getColorForRole(profile.role),
    initial:firstName.charAt(0).toUpperCase()||'U',
    builtin:false
  };
}

function getSystemPromptForUser(user){
  if(USERS[user.id]?.system)return USERS[user.id].system;
  return CRM_PAGES_ONLY.includes(user.role)
    ?`Tu es BenAI CRM, assistant de ${user.name} (${ROLE_LABELS[user.role]||user.role}).`
    :`Tu es BenAI, l'assistant de ${user.name}. Tu proposes toujours — ${user.name} décide toujours.`;
}

function isTransientSupabaseAuthFailure(message=''){
  const m=String(message||'').toLowerCase();
  return m.includes('service unavailable')||m.includes('gateway timeout')||m.includes('timed out')||m.includes('temporarily unavailable');
}
async function signInWithPasswordRetry(client,email,password,maxAttempts=2){
  let lastError=null;
  for(let attempt=1;attempt<=maxAttempts;attempt++){
    const {data,error}=await client.auth.signInWithPassword({email,password});
    if(!error&&data?.session)return {data,error:null};
    lastError=error||new Error('invalid_credentials');
    if(attempt<maxAttempts&&isTransientSupabaseAuthFailure(lastError?.message)){
      await new Promise(resolve=>setTimeout(resolve,700*attempt));
      continue;
    }
    break;
  }
  return {data:null,error:lastError};
}

async function trySupabaseLogin(resolvedEmail,pwd,rawLogin=''){
  const email=String(resolvedEmail||'').trim().toLowerCase();
  const client=getSupabaseClient();
  if(!client||!email||!email.includes('@')||!pwd)return {ok:false,reason:'missing_credentials'};
  try{
    const {data,error}=await signInWithPasswordRetry(client,email,pwd,2);
    if(error||!data?.session){
      return {ok:false,reason:error?.message||'invalid_credentials'};
    }
    currentSupabaseSession=data.session;
    currentAuthMode='supabase';
    const profile=await fetchSupabaseProfile(data.session.access_token,data.session.user?.id||'');
    if(!profile)return {ok:false,reason:'profile_missing'};
    const user=makeUserFromSupabaseProfile(profile,email);
    if(!user)return {ok:false,reason:'profile_invalid'};
    user.system=getSystemPromptForUser(user);
    return {ok:true,user,session:data.session};
  }catch(e){
    return {ok:false,reason:e?.message||'network'};
  }
}

async function trySupabaseSessionFromLocalCredentials(uid,pwd){
  const email=getEmailCandidateForUid(uid);
  const client=getSupabaseClient();
  if(!client||!email||!pwd)return {ok:false,reason:'missing_credentials'};
  try{
    const {data,error}=await signInWithPasswordRetry(client,email,pwd,2);
    if(error||!data?.session)return {ok:false,reason:error?.message||'invalid_credentials'};
    currentSupabaseSession=data.session;
    currentAuthMode='supabase';
    return {ok:true};
  }catch(e){
    return {ok:false,reason:'network'};
  }
}

async function createSupabaseUserProvisioning(payload){
  const client=getSupabaseClient();
  await ensureSupabaseSession();
  const accessToken=currentSupabaseSession?.access_token||'';
  if(!client||!accessToken)return {ok:false,error:'Session Supabase admin manquante'};
  try{
    const fnUrl=`${SUPABASE_CONFIG.url}/functions/v1/create-user`;
    const res=await fetchWithTimeout(fnUrl,{
      method:'POST',
      headers:{
        'Content-Type':'application/json',
        apikey:SUPABASE_CONFIG.publishableKey,
        Authorization:'Bearer '+accessToken
      },
      body:JSON.stringify(payload)
    },12000);
    let bodyJson=null;
    const rawBody=await res.text();
    if(rawBody){
      try{bodyJson=JSON.parse(rawBody);}catch(_){}
    }
    if(!res.ok){
      const detail=bodyJson?.error||bodyJson?.message||rawBody||`HTTP ${res.status}`;
      return {ok:false,error:String(detail)};
    }
    return {ok:true,data:bodyJson||{}};
  }catch(e){
    return {ok:false,error:e?.message||'Erreur réseau create-user (CORS/connexion)'};
  }
}

async function deleteSupabaseUserProvisioning(payload){
  const client=getSupabaseClient();
  await ensureSupabaseSession();
  const accessToken=currentSupabaseSession?.access_token||'';
  if(!client||!accessToken)return {ok:false,error:'Session Supabase admin manquante'};
  try{
    const fnUrl=`${SUPABASE_CONFIG.url}/functions/v1/delete-user`;
    const res=await fetchWithTimeout(fnUrl,{
      method:'POST',
      headers:{
        'Content-Type':'application/json',
        apikey:SUPABASE_CONFIG.publishableKey,
        Authorization:'Bearer '+accessToken
      },
      body:JSON.stringify(payload||{})
    },12000);
    let bodyJson=null;
    const rawBody=await res.text();
    if(rawBody){
      try{bodyJson=JSON.parse(rawBody);}catch(_){}
    }
    if(!res.ok){
      const detail=bodyJson?.error||bodyJson?.message||rawBody||`HTTP ${res.status}`;
      return {ok:false,error:String(detail)};
    }
    return {ok:true,data:bodyJson||{}};
  }catch(e){
    return {ok:false,error:e?.message||'Erreur réseau delete-user (CORS/connexion)'};
  }
}

async function updateSupabaseUserPasswordProvisioning(body){
  const client=getSupabaseClient();
  await ensureSupabaseSession();
  const accessToken=currentSupabaseSession?.access_token||'';
  if(!client||!accessToken)return {ok:false,error:'Session Supabase manquante'};
  try{
    const fnUrl=`${SUPABASE_CONFIG.url}/functions/v1/update-user-password`;
    const res=await fetchWithTimeout(fnUrl,{
      method:'POST',
      headers:{
        'Content-Type':'application/json',
        apikey:SUPABASE_CONFIG.publishableKey,
        Authorization:'Bearer '+accessToken
      },
      body:JSON.stringify(body)
    },12000);
    let bodyJson=null;
    const rawBody=await res.text();
    if(rawBody){
      try{bodyJson=JSON.parse(rawBody);}catch(_){}
    }
    if(!res.ok){
      const detail=bodyJson?.error||bodyJson?.message||rawBody||`HTTP ${res.status}`;
      return {ok:false,error:String(detail)};
    }
    return {ok:true,data:bodyJson||{}};
  }catch(e){
    return {ok:false,error:e?.message||'Erreur réseau update-user-password (CORS/connexion)'};
  }
}

async function updateSupabaseUserAppUidProvisioning(body){
  const client=getSupabaseClient();
  await ensureSupabaseSession();
  const accessToken=currentSupabaseSession?.access_token||'';
  if(!client||!accessToken)return {ok:false,error:'Session Supabase manquante'};
  try{
    const fnUrl=`${SUPABASE_CONFIG.url}/functions/v1/update-user-app-uid`;
    const res=await fetchWithTimeout(fnUrl,{
      method:'POST',
      headers:{
        'Content-Type':'application/json',
        apikey:SUPABASE_CONFIG.publishableKey,
        Authorization:'Bearer '+accessToken
      },
      body:JSON.stringify(body||{})
    },12000);
    let bodyJson=null;
    const rawBody=await res.text();
    if(rawBody){
      try{bodyJson=JSON.parse(rawBody);}catch(_){}
    }
    if(!res.ok){
      const detail=bodyJson?.error||bodyJson?.message||rawBody||`HTTP ${res.status}`;
      return {ok:false,error:String(detail)};
    }
    return {ok:true,data:bodyJson||{}};
  }catch(e){
    return {ok:false,error:e?.message||'Erreur réseau update-user-app-uid (CORS/connexion)'};
  }
}

const PENDING_USER_DELETE_KEY='benai_pending_user_deletes';
let pendingUserDeleteInFlight=false;
let pendingUserDeleteLastRunTs=0;

function getPendingUserDeletes(){
  try{
    const arr=JSON.parse(appStorage.getItem(PENDING_USER_DELETE_KEY)||'[]');
    return Array.isArray(arr)?arr:[];
  }catch{
    return [];
  }
}

function savePendingUserDeletes(list){
  appStorage.setItem(PENDING_USER_DELETE_KEY,JSON.stringify(Array.isArray(list)?list:[]));
}

function enqueuePendingUserDelete(job){
  if(!job?.uid)return;
  const list=getPendingUserDeletes();
  if(list.some(x=>normalizeId(x.uid)===normalizeId(job.uid)))return;
  list.push({
    uid:job.uid,
    user_id:String(job.user_id||'').trim(),
    email:job.email||'',
    name:job.name||job.uid,
    attempts:0,
    ts:Date.now()
  });
  savePendingUserDeletes(list);
}

async function processPendingUserDeletes(force=false){
  if(pendingUserDeleteInFlight)return;
  const now=Date.now();
  if(!force&&now-pendingUserDeleteLastRunTs<30000)return;
  pendingUserDeleteLastRunTs=now;
  if(!navigator.onLine)return;
  const list=getPendingUserDeletes();
  if(!list.length)return;
  pendingUserDeleteInFlight=true;
  try{
    const keep=[];
    for(let i=0;i<list.length;i++){
      const job=list[i];
      const res=await deleteSupabaseUserProvisioning({
        user_id:String(job.user_id||'').trim(),
        app_uid:job.uid,
        email:job.email||''
      });
      if(res.ok){
        continue;
      }
      const attempts=Number(job.attempts||0)+1;
      keep.push({...job,attempts,ts:Date.now(),last_error:String(res.error||'')});
    }
    savePendingUserDeletes(keep);
    if(!keep.length){
      showDriveNotif('✅ Suppressions cloud en attente terminées');
    }
  }finally{
    pendingUserDeleteInFlight=false;
  }
}

const PENDING_USER_CREATE_KEY='benai_pending_user_creates';
let pendingUserCreateInFlight=false;
let pendingUserCreateLastRunTs=0;

function getPendingUserCreates(){
  try{
    const arr=JSON.parse(appStorage.getItem(PENDING_USER_CREATE_KEY)||'[]');
    return Array.isArray(arr)?arr:[];
  }catch{
    return [];
  }
}
function savePendingUserCreates(list){
  appStorage.setItem(PENDING_USER_CREATE_KEY,JSON.stringify(Array.isArray(list)?list:[]));
}
function enqueuePendingUserCreate(job){
  if(!job?.app_uid||!job?.email)return;
  const list=getPendingUserCreates();
  if(list.some(x=>normalizeId(x.app_uid)===normalizeId(job.app_uid)))return;
  list.push({
    email:String(job.email||'').trim().toLowerCase(),
    password:String(job.password||''),
    full_name:String(job.full_name||'').trim(),
    role:job.role||'assistante',
    company:job.company||'nemausus',
    app_uid:normalizeId(job.app_uid),
    attempts:0,
    ts:Date.now()
  });
  savePendingUserCreates(list);
}
function shouldFallbackLocalUserProvisionAfterCreateError(err){
  const e=String(err||'').toLowerCase();
  if(e.includes('already')&&e.includes('registered'))return false;
  if(e.includes('user already')||e.includes('email already')||e.includes('duplicate'))return false;
  return e.includes('session supabase')||e.includes('admin manquante')||
    e.includes('failed to fetch')||e.includes('network')||e.includes('load failed')||
    e.includes('cors')||e.includes('404')||e.includes('401')||e.includes('403')||
    e.includes('500')||e.includes('502')||e.includes('503')||e.includes('504')||
    e.includes('functions/v1')||e.includes('edge function');
}
async function processPendingUserCreates(force=false){
  if(pendingUserCreateInFlight)return;
  const now=Date.now();
  if(!force&&now-pendingUserCreateLastRunTs<30000)return;
  pendingUserCreateLastRunTs=now;
  if(!navigator.onLine)return;
  const list=getPendingUserCreates();
  if(!list.length)return;
  pendingUserCreateInFlight=true;
  try{
    const keep=[];
    for(let i=0;i<list.length;i++){
      const job=list[i];
      const res=await createSupabaseUserProvisioning({
        email:job.email,
        password:job.password,
        full_name:job.full_name,
        role:job.role,
        company:job.company,
        app_uid:job.app_uid
      });
      const errTxt=String(res.error||'').toLowerCase();
      if(res.ok||errTxt.includes('already')||errTxt.includes('registered')||errTxt.includes('exists')||errTxt.includes('duplicate')){
        continue;
      }
      const attempts=Number(job.attempts||0)+1;
      if(attempts>=25){
        continue;
      }
      keep.push({...job,attempts,ts:Date.now(),last_error:String(res.error||'')});
    }
    savePendingUserCreates(keep);
    if(!keep.length&&list.length){
      showDriveNotif('✅ Créations cloud utilisateur en attente terminées');
    }
  }finally{
    pendingUserCreateInFlight=false;
  }
}
async function finalizeBenAILocalUserAccount({uid,name,email,role,soc,fonction,pwd,extras,roleLabel}){
  const initial=(String(name||'').trim().split(/\s+/)[0]||uid||'U').charAt(0).toUpperCase();
  unhideUserId(uid);
  const newUser={id:uid,name,email,role,societe:soc,fonction,vehicule:'',color:COLORS[extras.length%COLORS.length],initial,builtin:false};
  extras.push(newUser);
  saveExtraUsers(extras);
  const pwds=getPwds();
  pwds[uid]=await hashPassword(uid,pwd);
  savePwds(pwds);
  const systemPrompt=CRM_PAGES_ONLY.includes(role)
    ?`Tu es BenAI CRM, assistant de ${name} (${roleLabel}) chez Nemausus Fermetures.`
    :`Tu es BenAI, l'assistant de ${name} (${fonction}) chez Nemausus Fermetures / Lambert SAS. Tu proposes toujours — ${name} décide toujours.`;
  USERS[uid]={name,pwd:pwds[uid],role,color:newUser.color,initial:newUser.initial,system:systemPrompt};
  pushBenAINotif('👋 Bienvenue',`Bienvenue ${name} 👋 Ton accès BenAI est prêt. Tu peux ouvrir l’onglet Guide 📘 quand tu veux t’y repérer, puis enchaîner avec ce qui te semble utile aujourd’hui.`,'👋',uid);
  try{void navigator.clipboard.writeText(pwd);}catch(_){}
  scheduleAppStoragePersist();
}

function mapSavToSupabaseRows(items){
  return (items||[]).map(s=>({
    legacy_id:String(s.id),
    societe:s.societe||'nemausus',
    client:s.client||'',
    probleme:s.probleme||s.type||'',
    rappel_date:s.rappel||s.date_rappel||null,
    commentaire:s.commentaire||'',
    urgent:!!s.urgent,
    statut:s.statut||'nouveau',
    archive:!!s.archive,
    mute_reminder:!!s.muteReminder,
    payload:s
  }));
}

function mapNotesToSupabaseRows(items){
  return (items||[]).filter(n=>!!n?._deleted||String(n?.text||n?.t||'').trim()).map(n=>({
    legacy_id:String(n.id),
    text:n.text||n.t||'',
    author_uid:n.by||n.from||currentUser?.id||'benjamin',
    target_uid:n.to||'all',
    ts:n.ts||Date.now(),
    payload:n
  }));
}

function mapAbsencesToSupabaseRows(items){
  return (items||[]).map(a=>({
    legacy_id:String(a.id),
    employe:a.employe||'',
    debut:a.debut||null,
    fin:a.fin||null,
    type:a.type||'Congé',
    note:a.note||'',
    notifs:Array.isArray(a.notifs)?a.notifs:[],
    heure_debut:a.heureDebut||'',
    heure_fin:a.heureFin||'',
    payload:a
  }));
}

function mapAnnuaireToSupabaseRows(items){
  return (items||[]).map(a=>({
    legacy_id:String(a.id),
    prenom:a.prenom||'',
    nom:a.nom||'',
    email:a.email||'',
    email_pro:a.emailPro||'',
    tel:a.tel||'',
    naissance:a.naissance||null,
    fonction:a.fonction||'Autre',
    societe:a.societe||'nemausus',
    payload:a
  }));
}

function isLikelyUuid(v){
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(v||'').trim());
}
function resolveAuthUidForUserId(uid){
  const normalized=normalizeId(uid);
  if(!normalized)return null;
  if(normalized===normalizeId(currentUser?.id)){
    const selfAuth=currentSupabaseSession?.user?.id||'';
    if(isLikelyUuid(selfAuth))return selfAuth;
  }
  const fromUsers=getAllUsers().find(u=>normalizeId(u.id)===normalized);
  const authUid=fromUsers?.auth_uid||fromUsers?.authUid||'';
  if(isLikelyUuid(authUid))return authUid;
  return null;
}

function mapLeadsToSupabaseRows(items){
  const createdBy=currentSupabaseSession?.user?.id||null;
  return (items||[]).map(l=>{
    const rawId=Number(l?.id);
    const leadId=(Number.isFinite(rawId)&&rawId>0)?Math.trunc(rawId):Date.now()+Math.floor(Math.random()*1000);
    const societe=(l.societe_crm==='lambert'||l.societe==='lambert')?'lambert':'nemausus';
    const allowedStatuts=['gris','rdv','jaune','vert','rouge'];
    const statut=allowedStatuts.includes(l.statut)?l.statut:'gris';
    const commercialAuthUid=l.commercial_user_id||resolveAuthUidForUserId(l.commercial)||null;
    const payload=(l&&typeof l==='object')?{...l,id:leadId}:l;
    return {
      id:leadId,
      societe_crm:societe,
      nom:l.nom||'',
      telephone:l.telephone||'',
      ville:l.ville||'',
      cp:l.cp||'',
      type_projet:l.type_projet||'',
      statut,
      raison_mort:l.raison_mort||null,
      created_by:createdBy,
      commercial_user_id:isLikelyUuid(commercialAuthUid)?commercialAuthUid:null,
      archive:!!l.archive,
      payload
    };
  });
}

function isSupabaseDuplicateConflict(status,bodyText=''){
  if(Number(status)!==409)return false;
  const txt=String(bodyText||'');
  return txt.includes('"23505"')||txt.toLowerCase().includes('duplicate')||txt.toLowerCase().includes('unique');
}

async function upsertSupabaseTable(table,rows){
  if(!rows?.length)return true;
  const headers=getSupabaseHeaders();
  if(!headers){
    lastSupabaseSyncError=`${table}: en-têtes Supabase manquants`;
    return false;
  }
  const conflictCol=table==='leads'?'id':'legacy_id';
  // Déduplication défensive: évite les conflits intra-batch.
  const seen=new Set();
  const sanitizedRows=(rows||[]).filter(r=>{
    const key=String(r?.[conflictCol]??'').trim();
    if(!key)return false;
    if(seen.has(key))return false;
    seen.add(key);
    return true;
  });
  if(!sanitizedRows.length)return true;
  const payload=JSON.stringify(sanitizedRows);
  const upsertRes=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/${table}?on_conflict=${conflictCol}`,{
    method:'POST',
    headers:{...headers,'Prefer':'resolution=merge-duplicates'},
    body:payload
  },10000);
  if(upsertRes.ok){
    lastSupabaseSyncError='';
    return true;
  }

  let upsertBody='';
  try{upsertBody=await upsertRes.text();}catch{}

  // Fallback 1: mode insert-only avec ignore duplicates (RLS-friendly).
  // Utile quand UPDATE est interdit mais INSERT autorisé.
  const ignoreRes=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/${table}?on_conflict=${conflictCol}`,{
    method:'POST',
    headers:{...headers,'Prefer':'resolution=ignore-duplicates,return=minimal'},
    body:payload
  },10000);
  if(ignoreRes.ok){
    lastSupabaseSyncError='';
    return true;
  }
  let ignoreBody='';
  try{ignoreBody=await ignoreRes.text();}catch{}
  if(isSupabaseDuplicateConflict(ignoreRes.status,ignoreBody)){
    lastSupabaseSyncError='';
    return true;
  }

  // Fallback 1b: granularité ligne par ligne pour éviter qu'un doublon bloque tout le lot.
  const perRowFailures=[];
  for(let i=0;i<sanitizedRows.length;i++){
    const row=sanitizedRows[i];
    const rowRes=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/${table}?on_conflict=${conflictCol}`,{
      method:'POST',
      headers:{...headers,'Prefer':'resolution=ignore-duplicates,return=minimal'},
      body:JSON.stringify([row])
    },10000);
    if(!rowRes.ok){
      let body='';
      try{body=await rowRes.text();}catch{}
      if(isSupabaseDuplicateConflict(rowRes.status,body)){
        continue;
      }
      perRowFailures.push(`${rowRes.status} ${String(body||'').slice(0,80)}`.trim());
      if(perRowFailures.length>=3)break;
    }
  }
  if(perRowFailures.length===0){
    lastSupabaseSyncError='';
    return true;
  }

  // Fallback : certains schémas n'ont pas legacy_id unique.
  const insertRes=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/${table}`,{
    method:'POST',
    headers:{...headers,'Prefer':'return=minimal'},
    body:payload
  },10000);
  if(insertRes.ok){
    lastSupabaseSyncError='';
    return true;
  }
  let insertBody='';
  try{insertBody=await insertRes.text();}catch{}
  if(isSupabaseDuplicateConflict(insertRes.status,insertBody)){
    lastSupabaseSyncError='';
    return true;
  }
  lastSupabaseSyncError=`${table}: upsert ${upsertRes.status} / ignore ${ignoreRes.status} / insert ${insertRes.status} ${insertBody||upsertBody||perRowFailures.join(' | ')||''}`.trim();
  return false;
}

async function replaceSupabaseTable(table,rows,idCol='legacy_id'){
  try{
    const headers=getSupabaseHeaders();
    if(!headers){
      lastSupabaseSyncError=`${table}: en-têtes Supabase manquants`;
      return false;
    }

    const localRows=Array.isArray(rows)?rows:[];
    // Garde-fou anti-perte: ne jamais vider une table entière sur une source locale vide.
    if(localRows.length===0){
      lastSupabaseSyncError='';
      return true;
    }

    // 1) Upsert des lignes locales
    const upsertOk=await upsertSupabaseTable(table,localRows);
    if(!upsertOk)return false;

    // 2) Suppression ciblée des lignes absentes localement (pas de wipe global).
    const remoteRows=await fetchSupabaseRowsPaged(table,headers,idCol);
    const remoteIds=(remoteRows||[]).map(r=>String(r?.[idCol]??'')).filter(Boolean);
    const localIds=new Set(localRows.map(r=>String(r?.[idCol]??'')).filter(Boolean));
    const toDelete=remoteIds.filter(id=>!localIds.has(id));
    if(!toDelete.length){
      lastSupabaseSyncError='';
      return true;
    }

    // Garde-fou: bloquer les suppressions anormalement massives.
    if(remoteIds.length>=20){
      const keepRatio=localIds.size/remoteIds.length;
      if(keepRatio<0.25){
        lastSupabaseSyncError=`${table}: sync bloquée (suppression massive suspecte évitée)`;
        return false;
      }
    }

    const chunkSize=120;
    for(let i=0;i<toDelete.length;i+=chunkSize){
      const chunk=toDelete.slice(i,i+chunkSize).map(v=>encodeURIComponent(v)).join(',');
      const delUrl=`${SUPABASE_CONFIG.url}/rest/v1/${table}?${idCol}=in.(${chunk})`;
      const delRes=await fetchWithTimeout(delUrl,{
        method:'DELETE',
        headers:{...headers,'Prefer':'return=minimal'}
      },10000);
      if(!delRes.ok){
        let body='';
        try{body=await delRes.text();}catch{}
        lastSupabaseSyncError=`${table}: suppression ciblée ${delRes.status} ${body||''}`.trim();
        return false;
      }
    }
    lastSupabaseSyncError='';
    return true;
  }catch(e){
    lastSupabaseSyncError=`${table}: erreur sync sécurisée ${e?.message||e||'inconnue'}`;
    return false;
  }
}

async function syncSharedTablesToSupabase(mem,annuaire,leads){
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return false;
  // Tentative "best effort": si la session manque, on continue avec l'apikey
  // (utile quand les policies autorisent les opérations anon/authenticated).
  await ensureSupabaseSession();
  const headers=getSupabaseHeaders();
  if(!headers){
    lastSupabaseSyncError='En-têtes Supabase manquants.';
    return false;
  }
  try{
    const now=Date.now();
    const forceFull=(now-supabaseLastSharedSyncTs)>SUPABASE_FULL_SHARED_SYNC_INTERVAL_MS;
    const jobs=[];
    if(forceFull||supabaseSharedDirty.sav)jobs.push(['sav',upsertSupabaseTable('sav',mapSavToSupabaseRows(mem?.sav))]);
    if(forceFull||supabaseSharedDirty.notes)jobs.push(['notes',upsertSupabaseTable('notes',mapNotesToSupabaseRows(mem?.notes))]);
    if(forceFull||supabaseSharedDirty.absences)jobs.push(['absences',upsertSupabaseTable('absences',mapAbsencesToSupabaseRows(mem?.absences))]);
    if(forceFull||supabaseSharedDirty.annuaire)jobs.push(['annuaire',upsertSupabaseTable('annuaire',mapAnnuaireToSupabaseRows(annuaire))]);
    if(forceFull||supabaseSharedDirty.leads)jobs.push(['leads',upsertSupabaseTable('leads',mapLeadsToSupabaseRows(leads))]);
    if(!jobs.length){
      return true;
    }
    const results=await Promise.all(jobs.map(async([key,promise])=>({key,ok:await promise})));
    const ok=results.every(r=>r.ok);
    results.forEach(r=>{
      supabaseSharedDirty[r.key]=!r.ok;
    });
    if(ok)supabaseLastSharedSyncTs=Date.now();
    if(!ok&&!lastSupabaseSyncError)lastSupabaseSyncError='Échec sync tables partagées (cause non détaillée).';
    return ok;
  }catch(e){
    lastSupabaseSyncError=`Erreur sync tables: ${e?.message||e||'inconnue'}`;
    return false;
  }
}

function scheduleSupabaseSync(mem,annuaire){
  clearTimeout(supabaseSyncTimer);
  supabaseSyncTimer=setTimeout(()=>{
    supabaseSyncTimer=null;
    void syncMemToSupabase(mem,annuaire).then(ok=>{
      if(ok){
        supabaseSyncFailStreak=0;
        supabaseRetryDelayMs=2000;
        return;
      }
      supabaseSyncFailStreak++;
      queueSupabaseSyncRetry('debounced-save');
    });
  },400);
}

function queueSupabaseSyncRetry(reason=''){
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!currentUser?.id)return;
  if(!navigator.onLine)return;
  if(supabaseRetryTimer)return;
  const wait=Math.min(Math.max(supabaseRetryDelayMs,2000),30000);
  supabaseRetryTimer=setTimeout(async()=>{
    supabaseRetryTimer=null;
    const ok=await syncMemToSupabase(getMem(),getAnnuaire());
    if(ok){
      supabaseSyncFailStreak=0;
      supabaseRetryDelayMs=2000;
      renderSupabaseRuntimeStatus();
      return;
    }
    supabaseSyncFailStreak++;
    supabaseRetryDelayMs=Math.min(supabaseRetryDelayMs*2,30000);
    if(supabaseSyncFailStreak>=3&&Date.now()-supabaseSyncWarnTs>20000){
      supabaseSyncWarnTs=Date.now();
      const detail=lastSupabaseSyncError?` (${String(lastSupabaseSyncError).slice(0,90)})`:'';
      showDriveNotif('⚠️ Sync cloud instable: BenAI réessaie automatiquement.'+detail);
    }
    renderSupabaseRuntimeStatus();
    queueSupabaseSyncRetry(`retry:${reason}`);
  },wait);
}

async function flushSupabaseSyncNow(){
  if(supabaseSyncTimer){
    clearTimeout(supabaseSyncTimer);
    supabaseSyncTimer=null;
  }
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!currentUser?.id)return true;
  const ok=await syncMemToSupabase(getMem(),getAnnuaire());
  if(ok){
    supabaseSyncFailStreak=0;
    supabaseRetryDelayMs=2000;
    renderSupabaseRuntimeStatus();
    return true;
  }
  supabaseSyncFailStreak++;
  queueSupabaseSyncRetry('flush');
  renderSupabaseRuntimeStatus();
  return false;
}

async function syncMemToSupabase(mem,annuaire=getAnnuaire()){
  if(isSwitchPreviewSession()){
    lastSupabaseSyncError='';
    supabaseSyncFailStreak=0;
    renderSupabaseRuntimeStatus();
    return true;
  }
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!currentUser){
    lastSupabaseSyncError='Supabase désactivé ou utilisateur non connecté.';
    renderSupabaseRuntimeStatus();
    return false;
  }
  if(!navigator.onLine){
    lastSupabaseSyncError='Hors ligne: synchronisation en attente.';
    renderSupabaseRuntimeStatus();
    return false;
  }
  // Tenter une sync même si la session Auth n'est pas encore prête.
  await ensureSupabaseSession();
  if(!hasSupabaseDataAuth()){
    lastSupabaseSyncError='Session Supabase inactive: reconnectez-vous pour synchroniser.';
    renderSupabaseRuntimeStatus();
    return false;
  }
  const headers=getSupabaseHeaders();
  if(!headers){
    lastSupabaseSyncError='Configuration REST Supabase incomplète (clé ou session).';
    renderSupabaseRuntimeStatus();
    return false;
  }
  try{
    // Stratégie stable: pousser d'abord le miroir global partagé, puis le snapshot par uid.
    const coreOk=await pushSharedCoreDataToSupabase(mem,annuaire,getLeads());
    let stateOk=false;
    const statePayload=buildSupabaseStatePayload(mem,annuaire,getLeads());
    try{
      const stateRes=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/${SUPABASE_CONFIG.table}?on_conflict=uid`,{
        method:'POST',
        headers:{...headers,'Prefer':'resolution=merge-duplicates,return=minimal'},
        body:JSON.stringify([{uid:currentUser.id,payload:statePayload,updated_at:new Date().toISOString()}])
      },10000);
      stateOk=!!stateRes?.ok||stateRes?.status===404;
      if(!stateOk){
        let stateBody='';
        try{stateBody=await stateRes.text();}catch{}
        if(isSupabaseDuplicateConflict(stateRes?.status,stateBody))stateOk=true;
      }
    }catch(e){
      stateOk=false;
    }
    if(coreOk||stateOk){
      lastSupabaseSyncError='';
      supabaseLastPushOkTs=Date.now();
      renderSupabaseRuntimeStatus();
      return true;
    }
    if(!lastSupabaseSyncError){
      lastSupabaseSyncError='Échec sync miroir global + état local.';
    }
    renderSupabaseRuntimeStatus();
    return false;
  }catch(e){
    lastSupabaseSyncError=`Erreur réseau sync: ${e?.message||e||'inconnue'}`;
    renderSupabaseRuntimeStatus();
    return false;
  }
}

async function loadCoreDataFromSupabase(){
  if(isSwitchPreviewSession()){
    lastSupabaseSyncError='';
    renderSupabaseRuntimeStatus();
    return false;
  }
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url||!currentUser)return false;
  if(!navigator.onLine)return false;
  await ensureSupabaseSession();
  if(!hasSupabaseDataAuth()){
    lastSupabaseSyncError='Session Supabase inactive: lecture cloud indisponible.';
    renderSupabaseRuntimeStatus();
    return false;
  }
  const headers=getSupabaseHeaders();
  if(!headers){
    lastSupabaseSyncError='Configuration REST Supabase incomplète (clé ou session).';
    renderSupabaseRuntimeStatus();
    return false;
  }
  try{
    const sharedCoreOk=await loadSharedCoreDataFromSupabase();
    if(sharedCoreOk){
      lastSupabaseSyncError='';
      supabaseLastPullOkTs=Date.now();
      renderSupabaseRuntimeStatus();
      return true;
    }
    // Si le miroir partagé a été lu correctement mais sans nouveauté,
    // ne surtout pas écraser avec un snapshot utilisateur potentiellement plus ancien.
    if(sharedCoreLastReadOk){
      lastSupabaseSyncError='';
      supabaseLastPullOkTs=Date.now();
      renderSupabaseRuntimeStatus();
      return false;
    }
    const stateSnapshotOk=await loadCoreDataFromSupabaseStateSnapshot();
    if(stateSnapshotOk){
      lastSupabaseSyncError='';
      supabaseLastPullOkTs=Date.now();
      renderSupabaseRuntimeStatus();
      return true;
    }
    // Mode durci: on évite les lectures table-par-table qui introduisent des erreurs RLS parasites.
    lastSupabaseSyncError='Aucune source miroir disponible (shared_core_data_v1 / benai_state).';
    renderSupabaseRuntimeStatus();
    return false;
  }catch(e){
    lastSupabaseSyncError=`Lecture cloud impossible: ${e?.message||e||'inconnue'}`;
    renderSupabaseRuntimeStatus();
    return false;
  }
}

function extractSharedAIKey(value){
  if(!value)return '';
  if(typeof value==='string')return normalizeAnthropicKey(value);
  return normalizeAnthropicKey(value.key||'');
}

async function loadSharedApiKeyFromSupabase(){
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return false;
  // Tolère une lecture sans session utilisateur si la policy Supabase l'autorise.
  await ensureSupabaseSession();
  const headers=getSupabaseHeaders();
  if(!headers)return false;
  try{
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/app_settings?key=eq.${SHARED_AI_SETTING_KEY}&select=value`,{headers},8000);
    if(!res.ok)return false;
    const rows=await res.json();
    const remoteKey=extractSharedAIKey(rows?.[0]?.value);
    if(isLikelyAnthropicKey(remoteKey)){
      appStorage.setItem(STORAGE_KEYS.api,remoteKey);
      return true;
    }
    return false;
  }catch(e){
    return false;
  }
}

async function saveSharedApiKeyToSupabase(rawKey){
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url){
    return {ok:false,reason:'missing_session'};
  }
  const session=await ensureSupabaseSession();
  if(!session?.access_token)return {ok:false,reason:'missing_session'};
  const headers=getSupabaseHeaders();
  if(!headers)return {ok:false,reason:'missing_headers'};
  const key=normalizeAnthropicKey(rawKey);
  if(!isLikelyAnthropicKey(key))return {ok:false,reason:'invalid_key'};
  try{
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/app_settings?on_conflict=key`,{
      method:'POST',
      headers:{...headers,'Prefer':'resolution=merge-duplicates,return=minimal'},
      body:JSON.stringify([{
        key:SHARED_AI_SETTING_KEY,
        value:{provider:'anthropic',key},
        updated_at:new Date().toISOString()
      }])
    },10000);
    if(res.ok)return {ok:true};
    if(res.status===401||res.status===403){
      const profile=await fetchSupabaseProfile(session.access_token,session.user?.id||'');
      if(!profile)return {ok:false,reason:'profile_missing'};
      return {ok:false,reason:'forbidden'};
    }
    return {ok:false,reason:'http_'+res.status};
  }catch(e){
    return {ok:false,reason:'network'};
  }
}

function scheduleSharedApiKeySyncRetry(rawKey,reason=''){
  const key=normalizeAnthropicKey(rawKey);
  if(!isLikelyAnthropicKey(key))return;
  pendingSharedApiKey=key;
  if(sharedApiKeyRetryTimer||!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return;
  const wait=Math.min(Math.max(sharedApiKeyRetryDelayMs,2500),30000);
  sharedApiKeyRetryTimer=setTimeout(async()=>{
    sharedApiKeyRetryTimer=null;
    if(!pendingSharedApiKey)return;
    const result=await saveSharedApiKeyToSupabase(pendingSharedApiKey);
    if(result.ok){
      pendingSharedApiKey='';
      sharedApiKeyRetryDelayMs=2500;
      return;
    }
    // Backoff progressif tant que la session/réseau n'est pas prêt.
    sharedApiKeyRetryDelayMs=Math.min(sharedApiKeyRetryDelayMs*2,30000);
    scheduleSharedApiKeySyncRetry(pendingSharedApiKey,`retry:${reason}`);
  },wait);
}

function refreshVisibleDataAfterSupabaseSync(){
  if(!currentUser)return;
  refreshSAVBadge();
  refreshLeadsBadge();
  updateNavAbsencesVisibility();
  refreshNotifBadge();
  if(document.getElementById('page-sav')?.style.display==='flex')renderSAV();
  if(document.getElementById('page-notes')?.style.display==='flex')renderNotes();
  if(document.getElementById('page-absences')?.style.display==='flex'){configureAbsencesPageForRole();renderAbsences();if(currentUser?.role==='admin')fillAbsEmpList();}
  if(document.getElementById('page-annuaire')?.style.display==='flex'){
    if(equipeUITab==='acces'){renderUsersList();renderPwdList();}
    else renderAnnuaire();
  }
  if(document.getElementById('page-leads')?.style.display==='flex')initLeadsPage();
  if(document.getElementById('page-messages')?.style.display==='flex'){
    scheduleRenderConvList();
    if(currentConv)renderThread(currentConv);
  }
}

async function syncSupabasePostLogin(){
  await hydrateAppStorageFromSupabase(currentUser?.id);
  const syncedCore=await loadCoreDataFromSupabase();
  const localApiKey=getApiKey();
  if(currentUser?.role==='admin'&&isLikelyAnthropicKey(localApiKey)){
    const publishResult=await saveSharedApiKeyToSupabase(localApiKey);
    if(!publishResult.ok){
      scheduleSharedApiKeySyncRetry(localApiKey,'post-login-admin-republish');
    }
  }
  let sharedLoaded=await loadSharedApiKeyFromSupabase();
  if(!sharedLoaded){
    await new Promise(r=>setTimeout(r,800));
    sharedLoaded=await loadSharedApiKeyFromSupabase();
  }
  await startSupabaseRealtimeSync();
  void processPendingUserDeletes(true);
  void processPendingUserCreates(true);
  renderSupabaseRuntimeStatus();
  if(syncedCore){
    refreshVisibleDataAfterSupabaseSync();
    return;
  }
  if(currentSupabaseSession?.access_token){
    const profile=await fetchSupabaseProfile(currentSupabaseSession.access_token,currentSupabaseSession.user?.id||'');
    if(!profile){
      showDriveNotif('⚠️ Profil Supabase manquant: ajoutez votre ligne admin dans la table profiles');
    }
  }
}

// ══════════════════════════════════════════
// UTILISATEURS
// ══════════════════════════════════════════
const USERS = {
  benai:{name:'BenAI',pwd:'',role:'system',color:'linear-gradient(135deg,#E8943A,#B45309)',initial:'B',system:''},
  benjamin:{name:'Benjamin',pwd:'1234',role:'admin',color:'linear-gradient(135deg,#E8943A,#B45309)',initial:'B',
    system:`Tu es BenAI, l'assistant personnel de Benjamin, gérant de Nemausus Fermetures et Lambert SAS. Benjamin gère tout : back-office, commandes fournisseurs, SAV, All Manager, emails, réseaux sociaux, comptabilité. Tu peux l'aider sur n'importe quel sujet, professionnel comme personnel. Tu lui proposes toujours — il décide toujours. Tutoie-le.`}
};

const BENAI_CTX=`
Contexte : Nemausus Fermetures (franchise Monsieur Store, Nîmes) + Lambert SAS (fermeture/menuiserie).
Produits : menuiserie alu, volets roulants/battants, portes, fenêtres, coulissants, pergolas, stores, portails, protections solaires.
Logiciels : Hercule Pro (devis), All Manager (ERP). Clientèle : particuliers uniquement.
RÈGLE ABSOLUE : Tu proposes — l'utilisateur décide toujours. Jamais d'action automatique.`;

// ══════════════════════════════════════════
// ÉTAT
// ══════════════════════════════════════════
let currentUser=null, chatHistory=[], currentConv=null, busy=false, currentFilter='tous';
let deferredInstallPrompt=null;

function toISODate(d){
  const dt=new Date(d);
  const y=dt.getFullYear();
  const m=String(dt.getMonth()+1).padStart(2,'0');
  const day=String(dt.getDate()).padStart(2,'0');
  return `${y}-${m}-${day}`;
}
function addDaysISO(days){
  const d=new Date();
  d.setHours(0,0,0,0);
  d.setDate(d.getDate()+Number(days||0));
  return toISODate(d);
}
function hasDirecteurCommercial(){
  return getAllUsers().some(u=>u.role==='directeur_co');
}
function getRoundRobinCommercial(societe){
  const users=getAllUsers().filter(u=>u.role==='commercial'&&(u.societe===societe||u.societe==='les-deux'));
  if(!users.length)return null;
  const key='benai_rr_idx_'+(societe||'all');
  let idx=parseInt(appStorage.getItem(key)||'0',10);
  if(Number.isNaN(idx))idx=0;
  const pick=users[idx%users.length];
  appStorage.setItem(key,String((idx+1)%users.length));
  return pick?.id||null;
}

// ══════════════════════════════════════════
// MÉMOIRE
// ══════════════════════════════════════════
function createEmptyMemState(){
  return {sav:[],messages:{},msg_deletions:{},msg_read_cursor:{},activity:[],notes:[],absences:[],tokens:{}};
}
function getMem(){
  if(!runtimeMemState||typeof runtimeMemState!=='object')runtimeMemState=createEmptyMemState();
  if(!Array.isArray(runtimeMemState.sav))runtimeMemState.sav=[];
  if(!runtimeMemState.messages||typeof runtimeMemState.messages!=='object')runtimeMemState.messages={};
  if(!runtimeMemState.msg_deletions||typeof runtimeMemState.msg_deletions!=='object')runtimeMemState.msg_deletions={};
  if(!runtimeMemState.msg_read_cursor||typeof runtimeMemState.msg_read_cursor!=='object')runtimeMemState.msg_read_cursor={};
  if(!Array.isArray(runtimeMemState.activity))runtimeMemState.activity=[];
  if(!Array.isArray(runtimeMemState.notes))runtimeMemState.notes=[];
  if(!Array.isArray(runtimeMemState.absences))runtimeMemState.absences=[];
  if(!runtimeMemState.tokens||typeof runtimeMemState.tokens!=='object')runtimeMemState.tokens={};
  return runtimeMemState;
}
function saveMem(m,sync=true){
  runtimeMemState=(m&&typeof m==='object')?m:createEmptyMemState();
  // Persistance session différée : évite de re-sérialiser tout l’état mémoire à chaque message (messagerie plus fluide).
  schedulePersistRuntimeToSession(currentUser?.id);
  appStorage.removeItem(STORAGE_KEYS.mem);
  if(sync){
    markSharedDirtyIfChanged('sav',runtimeMemState.sav||[]);
    markSharedDirtyIfChanged('notes',runtimeMemState.notes||[]);
    markSharedDirtyIfChanged('absences',runtimeMemState.absences||[]);
    scheduleSupabaseSync(m,getAnnuaire());
  }
}
function getApiKey(){return normalizeAnthropicKey(appStorage.getItem(STORAGE_KEYS.api)||'');}
async function getApiKeyForChat(){
  let key=getApiKey();
  if(isLikelyAnthropicKey(key))return key;
  await loadSharedApiKeyFromSupabase();
  key=getApiKey();
  return isLikelyAnthropicKey(key)?key:'';
}

function getAnthropicHeaders(apiKey){
  return {
    'Content-Type':'application/json',
    'x-api-key':apiKey,
    'anthropic-version':'2023-06-01',
    'anthropic-dangerous-direct-browser-access':'true'
  };
}

async function requestAnthropicMessages(body,retryOnInvalidKey=true){
  let apiKey=await getApiKeyForChat();
  if(!apiKey){
    throw new Error('Clé API manquante — reconnectez-vous puis revalidez la clé IA dans Paramètres');
  }
  const callApi=async key=>{
    const res=await fetchWithTimeout('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:getAnthropicHeaders(key),
      body:JSON.stringify(body)
    },15000);
    let data={};
    try{data=await res.json();}catch(e){}
    return {res,data};
  };
  let {res,data}=await callApi(apiKey);
  if(res.ok)return data;
  const errorMessage=data?.error?.message||'Erreur API';
  if(retryOnInvalidKey&&/invalid x-api-key/i.test(errorMessage)){
    await loadSharedApiKeyFromSupabase();
    apiKey=await getApiKeyForChat();
    if(apiKey){
      ({res,data}=await callApi(apiKey));
      if(res.ok)return data;
    }
  }
  throw new Error((data?.error?.message||errorMessage));
}

function getPwds(){try{return JSON.parse(appStorage.getItem(STORAGE_KEYS.pwds))||{benjamin:'1234'};}catch{return{benjamin:'1234'};}}
function savePwds(p){appStorage.setItem(STORAGE_KEYS.pwds,JSON.stringify(p));}
function getAccess(){try{return JSON.parse(appStorage.getItem(STORAGE_KEYS.access))||{};}catch{return {};}}
function saveAccess(a){appStorage.setItem(STORAGE_KEYS.access,JSON.stringify(a));}
const PWD_HASH_PREFIX='h$';
function isPwdHashed(v){return typeof v==='string'&&v.startsWith(PWD_HASH_PREFIX);}
async function hashPassword(uid,pwd){
  try{
    if(!window.crypto?.subtle)return pwd;
    const payload=`benai:${uid}:${pwd}`;
    const digest=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(payload));
    const hex=[...new Uint8Array(digest)].map(b=>b.toString(16).padStart(2,'0')).join('');
    return PWD_HASH_PREFIX+hex;
  }catch(e){return pwd;}
}
async function verifyPassword(uid,input,stored){
  if(!stored)return false;
  if(isPwdHashed(stored))return (await hashPassword(uid,input))===stored;
  return input===stored;
}
async function maybeMigratePwds(){
  const pwds=getPwds();
  let changed=false;
  for(const [uid,val] of Object.entries(pwds)){
    if(!isPwdHashed(val)){
      pwds[uid]=await hashPassword(uid,val);
      changed=true;
    }
  }
  if(changed)savePwds(pwds);
}

// CHAT MEMORY par utilisateur
function saveChatMem(uid,msgs){
  const now=Date.now();
  const cutoff=now-30*24*60*60*1000;
  const pinned=msgs.filter(m=>m.pinned);
  const recent=msgs.filter(m=>!m.pinned&&(m.ts||0)>cutoff);
  const trimmed=[...pinned,...recent].slice(-60);
  appStorage.setItem('benai_chat_'+uid,JSON.stringify(trimmed));
}
function loadChatMem(uid){try{return JSON.parse(appStorage.getItem('benai_chat_'+uid))||[];}catch{return [];}}

// TOKEN TRACKING
function trackTokens(uid,inp,out){
  const mem=getMem();
  if(!mem.tokens)mem.tokens={};
  if(!mem.tokens[uid])mem.tokens[uid]={input:0,output:0};
  mem.tokens[uid].input+=inp||0;
  mem.tokens[uid].output+=out||0;
  saveMem(mem);
}
function estimateCost(inp,out){
  // Claude Sonnet: ~$3/MTok input, ~$15/MTok output
  return ((inp*0.000003)+(out*0.000015)).toFixed(4);
}

// MESSAGE UNREAD + accusés de lecture (msg_read_cursor sync cloud)
function migrateLegacyBenaiReadToMem(){
  if(!currentUser?.id)return;
  try{
    const key='benai_read_'+currentUser.id;
    const raw=appStorage.getItem(key);
    if(!raw)return;
    const legacy=JSON.parse(raw)||{};
    const mem=getMem();
    let changed=false;
    Object.keys(legacy).forEach(cid=>{
      const n=Number(legacy[cid]||0);
      if(!n)return;
      if(!mem.msg_read_cursor[cid])mem.msg_read_cursor[cid]={};
      const cur=Number(mem.msg_read_cursor[cid][currentUser.id]||0);
      if(n>cur){mem.msg_read_cursor[cid][currentUser.id]=n;changed=true;}
    });
    appStorage.removeItem(key);
    if(changed)saveMem(mem);
  }catch{}
}
function getLastRead(uid){
  if(!uid)return{};
  try{
    const mem=getMem();
    const out={};
    const cursors=mem.msg_read_cursor&&typeof mem.msg_read_cursor==='object'?mem.msg_read_cursor:{};
    Object.keys(cursors).forEach(cid=>{
      const t=cursors[cid]?.[uid];
      if(Number(t)>0)out[cid]=t;
    });
    return out;
  }catch{
    return{};
  }
}
function markRead(uid,cid){
  if(!uid||!cid)return;
  const mem=getMem();
  if(!mem.msg_read_cursor[cid])mem.msg_read_cursor[cid]={};
  const now=Date.now();
  mem.msg_read_cursor[cid][uid]=Math.max(Number(mem.msg_read_cursor[cid][uid]||0),now);
  saveMem(mem);
}
function getPeerMaxReadTsForConv(cid,excludeUid){
  const c=getMem().msg_read_cursor?.[cid];
  if(!c||typeof c!=='object')return 0;
  let max=0;
  Object.keys(c).forEach(u=>{
    if(u===excludeUid)return;
    max=Math.max(max,Number(c[u]||0));
  });
  return max;
}
function getReadReceiptHtml(cid,m){
  if(!currentUser||m.from!==currentUser.id)return'';
  const msgTs=Number(m.ts||0);
  if(cid==='groupe'){
    const peers=getAllUsers().filter(u=>u.id!==currentUser.id).map(u=>u.id);
    if(!peers.length||!msgTs)return'';
    const c=getMem().msg_read_cursor?.[cid]||{};
    const allRead=peers.every(uid=>Number(c[uid]||0)>=msgTs);
    if(allRead)return'<span class="tmsg-read tmsg-read-seen">✓ Lu</span>';
    return'<span class="tmsg-read">✓</span>';
  }
  if(!msgTs)return'';
  const peerTs=getPeerMaxReadTsForConv(cid,currentUser.id);
  if(peerTs>=msgTs)return'<span class="tmsg-read tmsg-read-seen">✓ Lu</span>';
  return'<span class="tmsg-read">✓</span>';
}
function countUnread(uid,cid,mem,lrMap){
  const m=mem||getMem();
  const msgs=m.messages[cid]||[];
  const lr=(lrMap||getLastRead(uid))[cid]||0;
  let n=0;
  for(let i=0;i<msgs.length;i++){
    const x=msgs[i];
    if(x&&x.from!==uid&&(x.ts||0)>lr)n++;
  }
  return n;
}
function countTotalInternalUnread(uid,mem,lrMap){
  if(!uid||!mem)return 0;
  try{
    const lr=lrMap||getLastRead(uid);
    const convs=getConvsForUser(uid);
    let total=0;
    for(const cid of Object.keys(convs))total+=countUnread(uid,cid,mem,lr);
    return total;
  }catch{
    return 0;
  }
}
function refreshMsgBadge(){
  if(!currentUser)return;
  const total=countTotalInternalUnread(currentUser.id,getMem(),getLastRead(currentUser.id));
  const badge=document.getElementById('msg-badge');
  if(badge){badge.style.display=total>0?'flex':'none';badge.textContent=total;}
}

// ══════════════════════════════════════════
// LOGIN
// ══════════════════════════════════════════
function getRememberedLoginForForm(){
  let raw=appStorage.getItem(STORAGE_KEYS.rememberedLogin)||'';
  if(!raw){
    try{raw=localStorage.getItem(LOGIN_REMEMBER_LOCAL_KEY)||'';}catch{}
  }
  const s=String(raw||'').trim();
  if(!s)return'';
  if(s.toLowerCase()==='prenom.nom@entreprise.fr'){
    appStorage.removeItem(STORAGE_KEYS.rememberedLogin);
    try{localStorage.removeItem(LOGIN_REMEMBER_LOCAL_KEY);}catch{}
    return'';
  }
  return s;
}
function rememberLoginId(value){
  const v=String(value||'').trim();
  if(!v)return;
  appStorage.setItem(STORAGE_KEYS.rememberedLogin,v);
  try{localStorage.setItem(LOGIN_REMEMBER_LOCAL_KEY,v);}catch{}
}
function getLoginAttempts(){try{return JSON.parse(appStorage.getItem('benai_attempts'))||{count:0,blockedUntil:0};}catch{return{count:0,blockedUntil:0};}}
function saveLoginAttempts(a){appStorage.setItem('benai_attempts',JSON.stringify(a));}
function resetLoginAttempts(){saveLoginAttempts({count:0,blockedUntil:0});}

// Normalise identifiant : minuscules + supprime accents (Aurélie = aurelie = AURELIE)
function normalizeId(s){
  return (s||'').toLowerCase().trim()
    .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
    .replace(/\s+/g,'_');
}

function resolveLocalUidForLogin(rawLogin=''){
  const raw=String(rawLogin||'').trim();
  if(!raw)return '';
  if(raw.includes('@')){
    const email=raw.toLowerCase();
    const matchedUser=getAllUsers().find(u=>String(u?.email||'').trim().toLowerCase()===email);
    if(matchedUser?.id)return normalizeId(matchedUser.id);
  }
  return normalizeId(raw);
}

// Validation mot de passe fort
function validatePassword(pwd){
  if(pwd.length<6)return 'Minimum 6 caractères';
  if(!/[A-Z]/.test(pwd))return 'Au moins 1 majuscule requise';
  if(!/[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]/.test(pwd))return 'Au moins 1 caractère spécial requis (!@#$%...)';
  return null;
}

// ══ TIMEOUT 6H ══
let lastActivityTime=Date.now();
const SESSION_TIMEOUT=6*60*60*1000; // 6h
function resetActivityTimer(){lastActivityTime=Date.now();}
function checkSessionTimeout(){
  if(!currentUser||currentUser.id==='benjamin')return;
  if(Date.now()-lastActivityTime>SESSION_TIMEOUT){
    showDriveNotif('⏰ Session expirée après 6h d\'inactivité');
    logout();
  }
}
['click','keydown','touchstart'].forEach(e=>document.addEventListener(e,resetActivityTimer,{passive:true}));
setInterval(checkSessionTimeout,60000);

// ══ VÉRIF ACCÈS EN TEMPS RÉEL ══
function checkAccessRealtime(){
  if(!currentUser||currentUser.id==='benjamin')return;
  const access=getAccess();
  if(access[currentUser.id]===false){
    showDriveNotif('🔒 Votre accès a été révoqué');
    setTimeout(()=>logout(),1500);
  }
}
setInterval(checkAccessRealtime,15000);

async function login(){
  const rawLogin=(document.getElementById('login-user').value||'').trim();
  const uid=normalizeId(rawLogin);
  const localUid=resolveLocalUidForLogin(rawLogin);
  const pwd=document.getElementById('login-pwd').value;
  const isEmailLogin=rawLogin.includes('@');
  const err=document.getElementById('login-err');
  if(err){
    err.style.color='var(--t2)';
    err.textContent='Connexion en cours...';
  }
  if(!uid){err.textContent='Entrez votre pseudo BenAI ou votre email pro';return;}

  const resolvedSupabaseEmail=await resolveSupabaseAuthEmail(rawLogin);
  const supabaseAuthResult=await trySupabaseLogin(resolvedSupabaseEmail,pwd,rawLogin);
  if(supabaseAuthResult?.ok&&supabaseAuthResult.user){
    resetLoginAttempts();
    currentUser=supabaseAuthResult.user;
    await hydrateAppStorageFromSupabase(currentUser.id);
    logConnexion(currentUser.id);
    lastActivityTime=Date.now();
    rememberLoginId(document.getElementById('login-user').value.trim());
    if(err){
      err.style.color='var(--g)';
      err.textContent='';
    }
    document.getElementById('login-screen').style.display='none';
    document.getElementById('app').classList.add('visible');
    initApp();
    void syncSupabasePostLogin();
    return;
  }
  let supabaseFailureLabel='';
  if(isEmailLogin||resolvedSupabaseEmail){
    const reason=String(supabaseAuthResult?.reason||'').toLowerCase();
    if(reason.includes('invalid login credentials')){
      supabaseFailureLabel='Connexion Supabase refusée : email ou mot de passe incorrect.';
    }else if(reason.includes('email not confirmed')){
      supabaseFailureLabel='Connexion Supabase refusée : email non confirmé.';
    }else if(reason.includes('service unavailable')||reason.includes('gateway timeout')||reason.includes('temporarily unavailable')||reason.includes('timed out')){
      supabaseFailureLabel='Service Supabase temporairement indisponible.';
    }else if(reason.includes('failed to fetch')||reason.includes('networkerror')||reason.includes('load failed')||reason.includes('network request failed')){
      supabaseFailureLabel=window.location.protocol==='file:'
        ?'Connexion impossible en ouvrant le fichier en local (file://).'
        :'Erreur réseau vers Supabase.';
    }else if(reason==='profile_missing'){
      supabaseFailureLabel='Connexion Supabase OK mais profil manquant dans la table profiles.';
    }else{
      supabaseFailureLabel='Connexion Supabase refusée : '+(supabaseAuthResult?.reason||'erreur inconnue');
    }
    if(err){
      err.style.color='var(--y)';
      err.textContent=supabaseFailureLabel+' Tentative locale...';
    }
  }

  if(localUid!=='benjamin'){
    const attempts=getLoginAttempts();
    const now=Date.now();
    if(attempts.blockedUntil>now){
      const remaining=Math.ceil((attempts.blockedUntil-now)/1000);
      err.textContent=`Trop de tentatives. Réessayez dans ${remaining}s.`;
      return;
    }
  }

  const now=Date.now();
  const attempts=localUid!=='benjamin'?getLoginAttempts():{count:0,blockedUntil:0};
  const userFound=findUserById(localUid);
  const resolvedUid=userFound?normalizeId(userFound.id):localUid;

  if(!userFound){
    if(localUid!=='benjamin'){
      attempts.count++;
      if(attempts.count>=3){attempts.blockedUntil=now+5*60*1000;attempts.count=0;}
      saveLoginAttempts(attempts);
      err.textContent=supabaseFailureLabel||`Identifiant ou mot de passe incorrect (${Math.min(attempts.count,3)}/3)`;
    } else {
      err.textContent='Identifiant incorrect';
    }
    return;
  }

  const pwds=getPwds();
  const validPwd=pwds[resolvedUid]||USERS[resolvedUid]?.pwd||'1234';
  const isValid=await verifyPassword(resolvedUid,pwd,validPwd);
  if(!isValid){
    if(localUid!=='benjamin'){
      attempts.count++;
      if(attempts.count>=3){attempts.blockedUntil=now+5*60*1000;attempts.count=0;err.textContent='Accès bloqué 5 minutes.';saveLoginAttempts(attempts);return;}
      saveLoginAttempts(attempts);
      err.textContent=`Mot de passe incorrect — tentative ${attempts.count}/3`;
    } else {
      err.textContent='Mot de passe incorrect';
    }
    return;
  }
  if(err){
    err.style.color='var(--g)';
    err.textContent='';
  }
  const access=getAccess();
  if(access[resolvedUid]===false&&resolvedUid!=='benjamin'){err.textContent='Accès bloqué par l\'administrateur';return;}
  resetLoginAttempts();
  currentUser={id:resolvedUid,...(USERS[resolvedUid]||getExtraUserById(resolvedUid))};
  const supabaseBootstrap=await trySupabaseSessionFromLocalCredentials(resolvedUid,pwd);
  if(!supabaseBootstrap.ok){
    currentAuthMode='local';
    lastSupabaseSyncError='';
    supabaseSyncFailStreak=0;
    supabaseRetryDelayMs=2000;
  }
  await hydrateAppStorageFromSupabase(currentUser.id);
  // Log connexion
  logConnexion(resolvedUid);
  lastActivityTime=Date.now();
  if(pwd==='1234'){
    document.getElementById('login-screen').style.display='none';
    document.getElementById('force-pwd-screen').style.display='flex';
    return;
  }
  rememberLoginId(document.getElementById('login-user').value.trim());
  document.getElementById('login-screen').style.display='none';
  document.getElementById('app').classList.add('visible');
  initApp();
  if(!supabaseBootstrap.ok&&SUPABASE_CONFIG.enabled){
    setTimeout(()=>{
      showDriveNotif('⚠️ Connexion locale active, mais session Supabase absente. Connectez-vous avec email + mot de passe Supabase pour activer la synchro.');
    },600);
  }
  void syncSupabasePostLogin();
}

async function forgotPassword(){
  const err=document.getElementById('login-err');
  const rawLogin=(document.getElementById('login-user')?.value||'').trim();
  let email='';
  if(rawLogin.includes('@')){
    email=rawLogin.toLowerCase();
  }else{
    const uid=normalizeId(rawLogin);
    email=(await fetchLookupLoginEmail(rawLogin))||getEmailCandidateForUid(uid,rawLogin)||'';
  }
  if(!email||!email.includes('@')){
    const manual=prompt('Entrez votre email de connexion Supabase pour recevoir le lien de réinitialisation :',rawLogin.includes('@')?rawLogin:'');
    if(!manual)return;
    email=String(manual).trim().toLowerCase();
  }
  if(!email||!email.includes('@')){
    if(err){
      err.style.color='var(--r)';
      err.textContent='Email invalide.';
    }
    return;
  }
  const client=getSupabaseClient();
  if(!SUPABASE_CONFIG.enabled||!client){
    if(err){
      err.style.color='var(--r)';
      err.textContent='Réinitialisation indisponible : Supabase non configuré.';
    }
    alert('Réinitialisation indisponible sur cet appareil.\nContactez un administrateur BenAI.');
    return;
  }
  if(err){
    err.style.color='var(--t2)';
    err.textContent='Envoi du lien de réinitialisation...';
  }
  const redirectTo=window.location.origin+window.location.pathname;
  try{
    const {error}=await client.auth.resetPasswordForEmail(email,{redirectTo});
    if(error)throw error;
    if(err){
      err.style.color='var(--g)';
      err.textContent='Lien envoyé. Vérifiez votre boîte mail.';
    }
    alert(`Un email de réinitialisation a été envoyé à ${email}.\nPensez à vérifier les spams.`);
  }catch(e){
    if(err){
      err.style.color='var(--r)';
      err.textContent='Échec de réinitialisation : '+(e?.message||'erreur réseau');
    }
  }
}

function logConnexion(uid){
  try{
    const key='benai_connexions';
    const surf=detectBenAIClientSurface();
    const logs=JSON.parse(appStorage.getItem(key)||'[]');
    logs.unshift({
      uid,
      date:new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}),
      mobile:surf.mobile,
      pwa:surf.pwa
    });
    if(logs.length>200)logs.splice(200);
    appStorage.setItem(key,JSON.stringify(logs));
  }catch(e){}
}

function getConnexions(uid){
  try{
    const logs=JSON.parse(appStorage.getItem('benai_connexions')||'[]');
    return uid?logs.filter(l=>l.uid===uid):logs;
  }catch{return[];}
}

// Pile de sessions — pour revenir à Benjamin sans mot de passe (déclaré avant logout pour clarté)
let sessionStack=[];

async function logout(){
  flushPersistRuntimeToSession(currentUser?.id);
  if(convListRenderTimer){clearTimeout(convListRenderTimer);convListRenderTimer=null;}
  stopSupabaseRealtimeSync();
  if(pollingInterval){clearInterval(pollingInterval);pollingInterval=null;}
  if(supabaseRetryTimer){clearTimeout(supabaseRetryTimer);supabaseRetryTimer=null;}
  supabaseRetryDelayMs=2000;
  supabaseSyncFailStreak=0;
  const rememberedLogin=getRememberedLoginForForm();
  let syncOk=false;
  try{
    const timeoutMs=5000;
    syncOk=await Promise.race([
      flushSupabaseSyncNow(),
      new Promise(resolve=>setTimeout(()=>resolve(false),timeoutMs))
    ]);
    if(!syncOk&&!lastSupabaseSyncError){
      lastSupabaseSyncError='Timeout de synchronisation (>5s).';
    }
  }catch(e){
    syncOk=false;
    if(!lastSupabaseSyncError)lastSupabaseSyncError=e?.message||'Échec sync avant déconnexion.';
  }
  if(!syncOk&&SUPABASE_CONFIG.enabled){
    const shortDetail=lastSupabaseSyncError?` (${String(lastSupabaseSyncError).slice(0,120)})`:'';
    showDriveNotif('⚠️ Déconnexion forcée : sync Supabase incomplète'+shortDetail);
  }
  if(currentUser?.id){
    const snapshot=serializeCloudAppStorage();
    await persistAppStorageToSupabaseNow(currentUser.id,snapshot);
  }
  resetAppStorageRuntime();
  sessionStack=[];
  const client=getSupabaseClient();
  if(client)client.auth.signOut().catch(()=>{});
  currentSupabaseSession=null;
  currentAuthMode='unknown';
  currentUser=null;chatHistory=[];currentConv=null;
  if(rememberedLogin)rememberLoginId(rememberedLogin);
  ['nav-admin','nav-absences','nav-annuaire','nav-paie'].forEach(id=>{
    const el=document.getElementById(id);if(el)el.style.display='none';
  });
  document.getElementById('login-screen').style.display='flex';
  document.getElementById('app').classList.remove('visible');
  document.getElementById('login-user').value=rememberedLogin;
  document.getElementById('login-pwd').value='';
  document.getElementById('login-err').textContent='';
  document.getElementById('chat-area').innerHTML='';
}

function isSwitchPreviewSession(){
  return !!(currentUser&&currentUser.id!=='benjamin'&&sessionStack.length>0);
}
const ROLE_LABELS={admin:'Admin',assistante:'Assistante',metreur:'Métreur',commercial:'Commercial',directeur_co:'Dir. commercial',directeur_general:'Dir. général'};

function toggleSwitchPanel(){
  // Si pas Benjamin et qu'on peut revenir → retour direct sans panel
  if(currentUser.id!=='benjamin'&&sessionStack.length>0){
    switchBack();
    return;
  }
  // Benjamin → ouvrir le panel
  const panel=document.getElementById('switch-panel');
  const isOpen=panel.style.display!=='none';
  panel.style.display=isOpen?'none':'block';
  if(!isOpen){
    const list=document.getElementById('switch-user-list');
    const users=getAllUsers().filter(u=>u.id!==currentUser.id);
    list.innerHTML=users.map(u=>`
      <div onclick="switchToUser('${u.id}')" style="display:flex;align-items:center;gap:10px;padding:9px 10px;border-radius:9px;cursor:pointer;transition:.12s" onmouseover="this.style.background='var(--s3)'" onmouseout="this.style.background='transparent'">
        <div style="width:32px;height:32px;border-radius:9px;background:${u.color};display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;color:#fff;flex-shrink:0">${u.initial}</div>
        <div>
          <div style="font-size:13px;font-weight:600">${esc(u.name)}</div>
          <div style="font-size:10px;color:var(--t3)">${ROLE_LABELS[u.role]||u.role}</div>
        </div>
      </div>`).join('');
  }
}

function switchBack(){
  if(!sessionStack.length)return;
  flushPersistRuntimeToSession(currentUser?.id);
  const prevUid=sessionStack.pop();
  document.getElementById('switch-panel').style.display='none';
  const u=USERS[prevUid]||getExtraUserById(prevUid);
  if(!u)return;
  stopSupabaseRealtimeSync();
  if(pollingInterval){clearInterval(pollingInterval);pollingInterval=null;}
  chatHistory=[];currentConv=null;
  document.getElementById('chat-area').innerHTML='';
  currentUser={id:prevUid,...u};
  logActivity(`Retour sur la session de ${u.name}`);
  void (async()=>{
    await hydrateAppStorageFromSupabase(prevUid);
    initApp(true);
    showDriveNotif('✅ Session admin restaurée (sync cloud active).');
  })();
}

function switchToUser(uid){
  if(currentUser.id!=='benjamin'){return;}// Seul Benjamin peut switcher
  flushPersistRuntimeToSession(currentUser?.id);
  document.getElementById('switch-panel').style.display='none';
  stopSupabaseRealtimeSync();
  if(pollingInterval){clearInterval(pollingInterval);pollingInterval=null;}
  if(supabaseRetryTimer){clearTimeout(supabaseRetryTimer);supabaseRetryTimer=null;}
  if(supabaseSyncTimer){clearTimeout(supabaseSyncTimer);supabaseSyncTimer=null;}
  supabaseRetryDelayMs=2000;
  supabaseSyncFailStreak=0;
  lastSupabaseSyncError='';
  const u=USERS[uid]||getExtraUserById(uid);
  if(!u){alert('Utilisateur introuvable');return;}
  const access=getAccess();
  if(access[uid]===false){alert(`Accès bloqué pour ${u.name}`);return;}
  if(currentUser)sessionStack.push(currentUser.id);
  chatHistory=[];currentConv=null;
  document.getElementById('chat-area').innerHTML='';
  currentUser={id:uid,...u};
  logActivity(`Benjamin a basculé vers ${u.name}`);
  void (async()=>{
    initApp(true);
    showDriveNotif('👁️ Mode aperçu utilisateur: sync cloud désactivée pendant le switch.');
  })();
}

function switchSession(){toggleSwitchPanel();}


function applySidebarSectionOrderForRole(role){
  const sidebar=document.querySelector('.sidebar');
  const q=document.getElementById('sb-quotidien');
  const m=document.getElementById('sb-metier');
  const e=document.getElementById('sb-entreprise');
  if(!sidebar||!q||!m||!e)return;
  const metierLbl=m.querySelector('.sb-label');
  if(metierLbl)metierLbl.textContent=CRM_PAGES_ONLY.includes(role)?'CRM':'Métier';
  if(CRM_PAGES_ONLY.includes(role)){
    sidebar.append(m);
    sidebar.append(q);
    sidebar.append(e);
  }else{
    sidebar.append(q);
    sidebar.append(m);
    sidebar.append(e);
  }
}

function initApp(silent=false){
  const u=currentUser;
  hydrateRuntimeFromSession(u?.id);
  migrateLegacyBenaiReadToMem();
  migrateMotivationMessagesToBenaiThread();
  refreshSharedSignatures(false);
  if(u?.role==='admin'){
    void syncExtraUsersFromSupabaseProfiles().then(ok=>{
      if(ok&&document.getElementById('page-annuaire')?.style.display==='flex'&&equipeUITab==='acces'){
        renderUsersList();
        renderPwdList();
      }
    });
  }
  applyTheme();
  const chatArea=document.getElementById('chat-area');
  if(chatArea)chatArea.innerHTML='';
  document.getElementById('tb-avatar').style.background=u.color;
  document.getElementById('tb-avatar').textContent=u.initial;
  document.getElementById('tb-username').textContent=u.name;
  // Bouton changer de session — Benjamin uniquement OU retour si en mode switch
  const btnSwitch=document.getElementById('btn-switch-session');
  if(btnSwitch){
    const showSwitch=u.id==='benjamin'||sessionStack.length>0;
    btnSwitch.style.display=showSwitch?'inline-block':'none';
    btnSwitch.textContent=sessionStack.length>0&&u.id!=='benjamin'?'↩️ Retour':'🔄 Changer';
  }

  // Réinitialiser TOUS les éléments admin avant d'appliquer les droits
  ['nav-admin','nav-absences','nav-annuaire','nav-paie','nav-leads',
   'nav-benai','nav-notes','nav-messages','nav-sav','nav-guide','nav-evolution','nav-bugs','nav-signaler'].forEach(id=>{
    const el=document.getElementById(id);if(el)el.style.display='none';
  });

  // Afficher nav selon rôle
  const allowed=ROLE_PAGES[u.role]||ROLE_PAGES['assistante'];
  allowed.forEach(p=>{
    const nav=document.getElementById('nav-'+p);
    if(nav)nav.style.display='flex';
  });
  const navSig=document.getElementById('nav-signaler');
  if(navSig)navSig.style.display='flex';
  const navTk=document.getElementById('nav-bugs');
  if(navTk)navTk.style.display=u.role==='admin'?'flex':'none';
  updateNavAbsencesVisibility();
  // Nav leads toujours visible si dans les droits
  if(allowed.includes('leads')){
    const navL=document.getElementById('nav-leads');
    if(navL)navL.style.display='flex';
  }
  applySidebarSectionOrderForRole(u.role);

  // Bouton correction orthographe (IA) — ordinateur / large fenêtre uniquement
  const btnCorrect=document.getElementById('btn-correct');
  if(btnCorrect)btnCorrect.style.display=isBenAIDesktopAutocorrect()?'inline-flex':'none';

  // Page d'accueil selon rôle
  const homePage=CRM_PAGES_ONLY.includes(u.role)?'leads':'benai';

  const mem=loadChatMem(u.id);
  chatHistory=mem.map(m=>({role:m.role,content:m.content}));
  // Message d'accueil ou restauration silencieuse (seulement si pas CRM only)
  if(!CRM_PAGES_ONLY.includes(u.role)){
    if(silent){
      restoreChatVisual();
    } else {
      const h=new Date().getHours();
      const sal=h<18?'Bonjour':'Bonsoir';
      addAIMsg(`${sal} **${u.name}** 👋\n\nJe suis BenAI v${BENAI_VERSION}, votre assistant.\n\n⚠️ Je propose toujours — vous décidez toujours.\n\nComment puis-je vous aider ?`);
    }
  }
  // Vérifier rappels absences (admin : flux historique Benjamin + CRM si absences partagées)
  if(u.role==='admin'){
    checkAbsenceReminders();
    refreshAdmin();
  } else if(!CRM_PAGES_ONLY.includes(u.role)){
    checkAbsenceRemindersForUser(u.id);
  }
  if(CRM_PAGES_ONLY.includes(u.role)&&(ROLE_PAGES[u.role]||[]).includes('absences')){
    checkAbsenceRemindersForUser(u.id);
  }
  refreshSAVBadge();
  refreshMsgBadge();
  refreshLeadsBadge();
  refreshBugsBadge();
  const forceGuide=shouldForceRoleGuide();
  showPage(forceGuide?'guide':homePage);
  if(forceGuide){
    pushBenAINotif('📘 Guide à valider','Après cette mise à jour, merci de parcourir le guide : il résume les changements utiles pour ton rôle.','📘',u.id);
  }
  buildEmojiPicker();
  startPolling();
  if(allowed.includes('absences'))configureAbsencesPageForRole();
  refreshNotifBadge();
  requestNotifPermission();
  // Motivations automatiques pour commerciaux
  if(u.role==='commercial')setTimeout(()=>checkMotivationsAuto(),5000);
  // Rappel anniversaires pour Benjamin
  if(u.id==='benjamin')setTimeout(()=>checkAnniversaires(),3000);
  // Tuto première utilisation (tous les rôles non-admin Benjamin)
  if(shouldShowTuto())setTimeout(()=>startTuto(),1200);
  // Bouton tuto dans paramètres
  const tutoSection=document.getElementById('tuto-settings-section');
  if(tutoSection)tutoSection.style.display=currentUser.id==='benjamin'?'none':'block';
  // Briefing quotidien intelligent (une seule fois par jour)
  if(!silent&&!CRM_PAGES_ONLY.includes(u.role))setTimeout(()=>generateDailyBriefing(),600);
}

// ══════════════════════════════════════════
// NAVIGATION
// ══════════════════════════════════════════
function showPage(page){
  if(currentUser&&shouldForceRoleGuide()&&page!=='guide'){
    page='guide';
  }
  const adminOnly=['admin','annuaire','paie'];
  if(adminOnly.includes(page)&&currentUser?.role!=='admin'){showPage('leads');return;}
  // Vérifier accès selon rôle
  const allowed=ROLE_PAGES[currentUser?.role]||ROLE_PAGES['assistante'];
  const canOpenNotes=allowed.includes('notes');
  const canOpenMessages=allowed.includes('messages');
  const canOpenEvolution=allowed.includes('evolution');
  if(!allowed.includes(page)){showPage(allowed[0]);return;}
  if(page==='absences'&&currentUser?.role!=='admin'&&!hasAbsencesSharedWithUser(currentUser)){
    showPage(allowed.find(p=>p!=='absences')||allowed[0]);return;
  }
  const notesToMsgBtn=document.getElementById('notes-open-messages-btn');
  if(notesToMsgBtn)notesToMsgBtn.style.display=canOpenMessages?'inline-block':'none';
  const msgToNotesBtn=document.getElementById('messages-open-notes-btn');
  if(msgToNotesBtn)msgToNotesBtn.style.display=canOpenNotes?'inline-block':'none';
  const guideToEvoBtn=document.getElementById('guide-open-evolution-btn');
  if(guideToEvoBtn)guideToEvoBtn.style.display=canOpenEvolution?'inline-block':'none';
  ['benai','notes','messages','sav','leads','absences','evolution','guide','admin','annuaire','paie','bugs'].forEach(p=>{
    const el=document.getElementById('page-'+p);if(el)el.style.display='none';
    const nav=document.getElementById('nav-'+p);if(nav)nav.classList.remove('active');
  });
  const el=document.getElementById('page-'+page);if(el)el.style.display='flex';
  const nav=document.getElementById('nav-'+page);if(nav)nav.classList.add('active');
  if(page==='sav'){renderSAV();markSAVVu();showPageContext('sav');}
  if(page==='admin')refreshAdmin();
  if(page==='messages'){
    renderConvList();
    const convs=getConvsForUser(currentUser.id);
    if(currentConv&&convs[currentConv])openConv(currentConv,convs[currentConv]);
    showPageContext('messages');
  }
  if(page==='notes')renderNotes();
  if(page==='absences'){configureAbsencesPageForRole();renderAbsences();if(currentUser?.role==='admin')fillAbsEmpList();}
  if(page==='evolution'){syncEvolutionHabitAiPanel();renderEvolution();}
  if(page==='guide')renderGuidePage();
  if(page==='annuaire')initEquipePage();
  if(page==='paie')renderPaieList();
  if(page==='leads')initLeadsPage();
  if(page==='bugs')initBugsPage();
  recordUsagePageVisit(page);
}

// ══════════════════════════════════════════
// BENAI IA CHAT
// ══════════════════════════════════════════
function addAIMsg(txt,pinnable=false){
  const div=document.createElement('div');div.className='msg ai';
  div.innerHTML=`<div class="msg-avatar">B</div><div class="bubble">${md(txt)}</div>`;
  document.getElementById('chat-area').appendChild(div);scrollChat();return div;
}
function addUserMsg(txt){
  const div=document.createElement('div');div.className='msg user';
  div.innerHTML=`<div class="msg-avatar" style="background:${currentUser.color}">${currentUser.initial}</div>
    <div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px">
      <div class="bubble" style="background:${currentUser.color}">${esc(txt)}</div>
      <div style="display:flex;gap:6px">
        <button onclick="editUserMsg(this,'${esc(txt).replace(/'/g,'&#39;')}')" style="background:none;border:none;color:var(--t3);font-size:10px;cursor:pointer;font-family:inherit;padding:0" title="Modifier">✏️ modifier</button>
      </div>
    </div>`;
  document.getElementById('chat-area').appendChild(div);scrollChat();return div;
}
function addTyping(){
  const div=document.createElement('div');div.className='msg ai';
  div.innerHTML='<div class="msg-avatar">B</div><div class="bubble"><div class="typing"><span></span><span></span><span></span></div></div>';
  document.getElementById('chat-area').appendChild(div);scrollChat();return div;
}
function scrollChat(){const c=document.getElementById('chat-area');c.scrollTop=c.scrollHeight;}

async function sendChat(txtOverride){
  const input=document.getElementById('chat-input');
  let txt=txtOverride||input.value.trim();
  if(!txt||busy)return;
  if(!txtOverride){input.value='';input.style.height='auto';}
  busy=true;document.getElementById('btn-send').disabled=true;

  // Correction auto avant envoi (ordinateur / large fenêtre uniquement)
  if(!txtOverride&&isBenAIDesktopAutocorrect()){
    const apiKey=getApiKey();
    if(apiKey){
      try{
        const r=await fetch('https://api.anthropic.com/v1/messages',{
          method:'POST',
          headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
          body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:300,messages:[{role:'user',content:`Corrige uniquement les fautes d'orthographe et de grammaire françaises, sans changer le sens ni le style. Réponds UNIQUEMENT avec le texte corrigé, rien d'autre. Si pas de faute, réponds le texte exact tel quel :\n\n${txt}`}]})
        });
        const d=await r.json();
        if(d.content?.[0]?.text){
          const corrected=d.content[0].text.trim();
          if(corrected&&corrected!==txt)txt=corrected;
        }
      }catch(e){}
    }
  }
  addUserMsg(txt);
  const now=Date.now();
  chatHistory.push({role:'user',content:txt,ts:now});
  if(chatHistory.length>60)chatHistory=chatHistory.slice(-60);
  if(shouldGenerateLocalCRMReport(txt)){
    const report=generateLocalCRMReport();
    chatHistory.push({role:'assistant',content:report,ts:Date.now()});
    saveChatMem(currentUser.id,chatHistory);
    addAIMsg(report);
    logActivity(`${currentUser.name} a demandé un rapport CRM local`);
    busy=false;document.getElementById('btn-send').disabled=false;
    return;
  }
  const typing=addTyping();
  try{
    const mem=getMem();
    const ctx=`\nSAV ouverts: ${mem.sav.filter(s=>s.statut!=='regle').length}`;
    const crmCtx=getCRMContextForAI();
    // Contexte intelligent Benjamin : planning + rappels
    const smartCtx=currentUser.id==='benjamin'?getBenjaminSmartContext():'';
    const msgs=chatHistory.filter(m=>m.role).map(m=>({role:m.role,content:m.content}));
    const data=await requestAnthropicMessages({model:'claude-sonnet-4-20250514',max_tokens:1500,system:currentUser.system+BENAI_CTX+ctx+crmCtx+smartCtx,messages:msgs});
    const reply=data.content[0].text;
    const usage=data.usage||{};
    trackTokens(currentUser.id,usage.input_tokens||0,usage.output_tokens||0);
    updateTokenInfo(usage);
    chatHistory.push({role:'assistant',content:reply,ts:Date.now()});
    saveChatMem(currentUser.id,chatHistory);
    typing.remove();addAIMsg(reply);
    logActivity(`${currentUser.name} a consulté BenAI IA`);
  }catch(e){
    typing.remove();
    const msg=String(e?.message||'Erreur inconnue');
    if(/invalid x-api-key/i.test(msg)){
      addAIMsg('❌ Clé IA invalide côté Anthropic. Recollez une clé valide dans Paramètres admin, puis reconnectez-vous.');
    }else{
      addAIMsg('❌ '+msg);
    }
  }
  busy=false;document.getElementById('btn-send').disabled=false;
}

function updateTokenInfo(usage){
  const info=document.getElementById('token-info');
  if(info&&usage.input_tokens){
    const cost=estimateCost(usage.input_tokens,usage.output_tokens);
    info.textContent=`${(usage.input_tokens||0)+(usage.output_tokens||0)} tokens · ~${cost}€`;
  }
}

function getCRMContextForAI(){
  const leads=getCompanyScopedLeads(getLeads());
  const actifs=leads.filter(l=>!l.archive);
  const perdus=actifs.filter(l=>l.statut==='rouge');
  const vendus=actifs.filter(l=>l.statut==='vert');
  const devis=actifs.filter(l=>l.statut==='jaune');
  const reasons=getLeadLostAnalytics(actifs).rows.slice(0,3).map(r=>`${r.label}: ${r.count}`).join(' | ');
  return `\nCRM: ${actifs.length} leads actifs, ${vendus.length} vendus, ${devis.length} en devis, ${perdus.length} perdus.${reasons?` Motifs pertes: ${reasons}.`:''}`;
}

function shouldGenerateLocalCRMReport(txt){
  const q=String(txt||'').toLowerCase();
  const asksReport=/(rapport|analyse|diagnostic|audit|synthese|bilan)/.test(q);
  const crmScope=/(crm|lead|leads|commercial|commerciaux|vente|ventes|secteur|tableau|kpi)/.test(q);
  const asksProblems=/(ne va pas|anomal|risque|probleme|corrig|faible|point faible)/.test(q);
  return (asksReport&&crmScope)||(crmScope&&asksProblems);
}

function generateLocalCRMReport(){
  const scopedLeads=getCompanyScopedLeads(getLeads());
  const actifs=scopedLeads.filter(l=>!l.archive);
  const now=new Date();
  const month=now.getMonth(),year=now.getFullYear();
  const leadsMonth=actifs.filter(l=>{const d=new Date(l.date_creation||0);return d.getMonth()===month&&d.getFullYear()===year;});
  const vendus=actifs.filter(l=>l.statut==='vert');
  const devis=actifs.filter(l=>l.statut==='jaune');
  const perdus=actifs.filter(l=>l.statut==='rouge');
  const nonAttrib=actifs.filter(l=>!l.commercial&&l.statut==='gris');
  const alertes=actifs.filter(l=>isLeadAlerte(l));
  const zoneBlanche=actifs.filter(l=>l.zone_blanche||l.secteur==='zone_blanche');
  const horsSecteurSansJustif=actifs.filter(l=>l.hors_secteur&&!String(l.justification_hors_secteur||'').trim());
  const rdvSansDate=actifs.filter(l=>(l.statut==='rdv'||l.sous_statut==='rdv_programme')&&!l.rappel&&!l.date_rdv_fait);
  const devisSansMontant=actifs.filter(l=>l.statut==='jaune'&&!Number(l.montant_devis||0));
  const olderNoCommercial=actifs.filter(l=>!l.commercial&&getHeuresOuvrees(new Date(l.date_creation||Date.now()))>24);
  const caTotal=vendus.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
  const caMonth=leadsMonth.filter(l=>l.statut==='vert').reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
  const lostTop=getLeadLostAnalytics(leadsMonth).rows.slice(0,3);

  const issues=[];
  if(nonAttrib.length)issues.push(`- ${nonAttrib.length} lead(s) non attribué(s).`);
  if(alertes.length)issues.push(`- ${alertes.length} lead(s) en alerte de traitement.`);
  if(olderNoCommercial.length)issues.push(`- ${olderNoCommercial.length} lead(s) sans commercial depuis +24h ouvrées.`);
  if(rdvSansDate.length)issues.push(`- ${rdvSansDate.length} RDV sans date.`);
  if(devisSansMontant.length)issues.push(`- ${devisSansMontant.length} devis sans montant.`);
  if(horsSecteurSansJustif.length)issues.push(`- ${horsSecteurSansJustif.length} lead(s) hors secteur sans justification.`);

  const secteurs=['nimes','avignon','bagnoles','zone_blanche'];
  const secteursTxt=secteurs.map(s=>{
    const label=getLeadSecteurLabel(s);
    const sl=actifs.filter(l=>l.secteur===s);
    const ca=sl.filter(l=>l.statut==='vert').reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
    return `- ${label}: ${sl.length} leads, ${ca.toLocaleString('fr-FR')} €`;
  }).join('\n');

  const lostTxt=lostTop.length
    ?lostTop.map(r=>`- ${r.label}: ${r.count}`).join('\n')
    :'- Aucun motif majeur ce mois.';

  return `## Rapport CRM automatique

- Périmètre analysé: ${actifs.length} leads actifs
- Ce mois: ${leadsMonth.length} leads, ${caMonth.toLocaleString('fr-FR')} € signés
- Total signé (actifs): ${caTotal.toLocaleString('fr-FR')} €
- Devis en cours: ${devis.length}
- Leads perdus: ${perdus.length}
- Zone blanche: ${zoneBlanche.length}

### Vue par secteur
${secteursTxt}

### Points de vigilance
${issues.length?issues.join('\n'):'- Aucun signal critique détecté sur les règles principales.'}

### Motifs de perte (mois en cours)
${lostTxt}

### Actions recommandées
- Attribuer en priorité les leads en attente et ceux en alerte.
- Nettoyer les fiches incohérentes (RDV sans date, devis sans montant).
- Vérifier les hors-secteur sans justification pour éviter les pertes de suivi.`;
}

// CORRIGER ORTHOGRAPHE
async function correctSpelling(){
  if(!isBenAIDesktopAutocorrect())return;
  const input=document.getElementById('chat-input');
  const txt=input.value.trim();
  if(!txt)return;
  const apiKey=getApiKey();if(!apiKey)return;
  try{
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:400,messages:[{role:'user',content:`Corrige uniquement les fautes d'orthographe et de grammaire du texte suivant, sans changer le sens ni le style. Réponds UNIQUEMENT avec le texte corrigé, rien d'autre :\n\n${txt}`}]})
    });
    const data=await res.json();
    if(data.content&&data.content[0])input.value=data.content[0].text.trim();
  }catch(e){}
}

// CONTEXTE INTELLIGENT BENJAMIN
function getBenjaminSmartContext(){
  const days=['dimanche','lundi','mardi','mercredi','jeudi','vendredi','samedi'];
  const schedule={1:'Nemausus Fermetures',3:'Nemausus Fermetures',5:'Nemausus Fermetures',2:'Lambert SAS',4:'Lambert SAS'};
  const today=new Date().getDay();
  const site=schedule[today];
  const dayCtx=site?`\nAujourd'hui ${days[today]}, Benjamin est physiquement à ${site}.`:`\nAujourd'hui ${days[today]}, Benjamin travaille depuis chez lui pour les deux sociétés.`;
  const reminders=getSmartReminders();
  const reminderCtx=reminders.length>0?`\nRappels actifs de Benjamin (mentionne naturellement si pertinent) : ${reminders.slice(0,5).join(' | ')}`:'';
  return dayCtx+reminderCtx;
}

function getSmartReminders(){
  const mem=getMem();
  const notes=(mem.notes||[]).filter(n=>!n._deleted&&n.by==='benjamin'&&n.text);
  const keywords=['rappelle','rappeler','rappel','oublie','pense à','à faire','appeler','contacter','envoyer','commander','relancer','vérifier','suivre'];
  return notes.filter(n=>keywords.some(k=>n.text.toLowerCase().includes(k))).map(n=>n.text.substring(0,120));
}

// AUTO-CORRECTION BENJAMIN
async function autoCorrectForBenjamin(txt,msgEl){
  if(!isBenAIDesktopAutocorrect())return;
  const apiKey=getApiKey();if(!apiKey||!msgEl)return;
  try{
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:150,messages:[{role:'user',content:`Détecte les fautes d'orthographe et de grammaire. Si aucune faute : réponds uniquement "OK". Sinon : réponds uniquement le texte corrigé sans aucune explication.\n\n${txt}`}]})
    });
    const data=await res.json();if(!data.content)return;
    const corrected=data.content[0].text.trim();
    if(!corrected||corrected.toUpperCase()==='OK'||corrected===txt)return;
    const corrDiv=document.createElement('div');
    corrDiv.className='correction-bubble';
    const encodedCorrect=encodeURIComponent(corrected);
    corrDiv.innerHTML=`📝 <span style="flex:1">${esc(corrected)}</span><button class="correction-use" onclick="useCorrection('${encodedCorrect}',this)">Utiliser</button><button class="correction-use" style="background:var(--s3);color:var(--t3)" onclick="this.parentElement.remove()">✕</button>`;
    const wrapper=msgEl.querySelector('[style*="flex-direction:column"]');
    if(wrapper)wrapper.appendChild(corrDiv);
    if(data.usage)trackTokens(currentUser.id,data.usage.input_tokens||0,data.usage.output_tokens||0);
  }catch(e){}
}

function useCorrection(corrected,btn){
  const input=document.getElementById('chat-input');
  let decoded=corrected||'';
  try{decoded=decodeURIComponent(corrected||'');}catch{}
  input.value=decoded;
  input.style.height='auto';input.style.height=Math.min(input.scrollHeight,100)+'px';
  input.focus();
  btn.closest('.correction-bubble').remove();
}

// MODIFIER MESSAGE UTILISATEUR
function editUserMsg(btn,originalTxt){
  const input=document.getElementById('chat-input');
  input.value=originalTxt.replace(/&#39;/g,"'");
  input.style.height='auto';input.style.height=Math.min(input.scrollHeight,100)+'px';
  input.focus();
  const chatArea=document.getElementById('chat-area');
  const msgs=chatArea.querySelectorAll('.msg');
  const userMsg=btn.closest('.msg');
  const userIdx=Array.from(msgs).indexOf(userMsg);
  for(let i=msgs.length-1;i>=userIdx;i--)msgs[i].remove();
  const lastUserIdx=chatHistory.map(m=>m.role).lastIndexOf('user');
  if(lastUserIdx>-1)chatHistory=chatHistory.slice(0,lastUserIdx);
  saveChatMem(currentUser.id,chatHistory);
}

// GESTION FICHIERS JOINTS
async function handleAttachment(input){
  const files=Array.from(input.files);if(!files.length)return;input.value='';
  for(const file of files){
    if(file.type.startsWith('image/'))await handleImageFile(file);
    else if(file.type==='application/pdf'||file.name.toLowerCase().endsWith('.pdf')){
      const isDevis=file.name.toLowerCase().includes('devis')||file.name.toLowerCase().includes('commande');
      if(isDevis)await handleDevisPDF(file);
      else await handleGenericPDF(file);
    }
  }
}

async function handleImageFile(file){
  busy=true;document.getElementById('btn-send').disabled=true;
  addUserMsg('🖼️ '+file.name);const typing=addTyping();
  try{
    const reader=new FileReader();
    const base64=await new Promise((res,rej)=>{reader.onload=e=>res(e.target.result.split(',')[1]);reader.onerror=rej;reader.readAsDataURL(file);});
    const userPrompt=document.getElementById('chat-input').value.trim()||'Décris et analyse cette image.';
    document.getElementById('chat-input').value='';
    const apiKey=getApiKey();
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:1500,system:currentUser.system+BENAI_CTX,messages:[{role:'user',content:[{type:'image',source:{type:'base64',media_type:file.type||'image/jpeg',data:base64}},{type:'text',text:userPrompt}]}]})
    });
    const data=await res.json();if(!res.ok)throw new Error(data.error?.message||'Erreur API');
    if(data.usage)trackTokens(currentUser.id,data.usage.input_tokens||0,data.usage.output_tokens||0);
    typing.remove();addAIMsg(data.content[0].text);
  }catch(e){typing.remove();addAIMsg('❌ '+e.message);}
  busy=false;document.getElementById('btn-send').disabled=false;
}

async function handleGenericPDF(file){
  busy=true;document.getElementById('btn-send').disabled=true;
  addUserMsg('📄 '+file.name);const typing=addTyping();
  try{
    const ab=await file.arrayBuffer();
    const pdf=await pdfjsLib.getDocument({data:ab}).promise;
    const maxPages=Math.min(pdf.numPages,3);let imgs=[];
    for(let i=1;i<=maxPages;i++){
      const p=await pdf.getPage(i);const vp=p.getViewport({scale:1.5});
      const c=document.createElement('canvas');c.width=vp.width;c.height=vp.height;
      await p.render({canvasContext:c.getContext('2d'),viewport:vp}).promise;
      imgs.push(c.toDataURL('image/jpeg',0.8).split(',')[1]);
    }
    const userPrompt=document.getElementById('chat-input').value.trim()||'Analyse et résume ce document.';
    document.getElementById('chat-input').value='';
    const apiKey=getApiKey();
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:1000,system:currentUser.system+BENAI_CTX,messages:[{role:'user',content:[...imgs.map(img=>({type:'image',source:{type:'base64',media_type:'image/jpeg',data:img}})),{type:'text',text:userPrompt}]}]})
    });
    const data=await res.json();if(!res.ok)throw new Error(data.error?.message||'Erreur API');
    if(data.usage)trackTokens(currentUser.id,data.usage.input_tokens||0,data.usage.output_tokens||0);
    typing.remove();addAIMsg(data.content[0].text);
  }catch(e){typing.remove();addAIMsg('❌ '+e.message);}
  busy=false;document.getElementById('btn-send').disabled=false;
}

async function handleDevisPDF(file){
  busy=true;document.getElementById('btn-send').disabled=true;
  addUserMsg('📋 Devis : '+file.name);const typing=addTyping();
  try{
    const ab=await file.arrayBuffer();
    const pdf=await pdfjsLib.getDocument({data:ab}).promise;
    let imgs=[];
    for(let i=1;i<=pdf.numPages;i++){
      const p=await pdf.getPage(i);const vp=p.getViewport({scale:1.8});
      const c=document.createElement('canvas');c.width=vp.width;c.height=vp.height;
      await p.render({canvasContext:c.getContext('2d'),viewport:vp}).promise;
      imgs.push(c.toDataURL('image/jpeg',0.82).split(',')[1]);
    }
    const apiKey=getApiKey();
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:4000,system:`Tu es BenAI. Analyse ce devis Hercule Pro et réponds UNIQUEMENT en JSON valide :{"devis":{"numero":"","date":"","validite":"","type_prestation":"","contact_nom":"","contact_tel":"","contact_email":""},"client":{"nom_prenom":"","telephone":"","email":"","adresse_facturation":"","adresse_chantier":""},"produits":[{"designation":"","dimensions":"","quantite":1,"pu_ht":0.00,"total_ht":0.00,"tva_taux":""}],"totaux":{"montant_brut_ht":0.00,"remise_pct":"","remise_montant":0.00,"total_ht_net":0.00,"tva_montant":0.00,"total_ttc":0.00}}`,
        messages:[{role:'user',content:[...imgs.map(img=>({type:'image',source:{type:'base64',media_type:'image/jpeg',data:img}})),{type:'text',text:'Extrais toutes les données de ce devis.'}]}]})
    });
    const data=await res.json();if(!res.ok)throw new Error(data.error?.message||'Erreur API');
    if(data.usage)trackTokens(currentUser.id,data.usage.input_tokens||0,data.usage.output_tokens||0);
    typing.remove();
    try{const devis=JSON.parse(data.content[0].text.replace(/```json|```/g,'').trim());afficherDevis(devis,file.name);}
    catch{addAIMsg(data.content[0].text);}
  }catch(e){typing.remove();addAIMsg('❌ Erreur : '+e.message);}
  busy=false;document.getElementById('btn-send').disabled=false;
}

// PAGE ÉQUIPE (annuaire + accès BenAI)
function setEquipeTab(tab){
  if(tab!=='acces'&&tab!=='annuaire')tab='annuaire';
  equipeUITab=tab;
  const pAnn=document.getElementById('equipe-panel-annuaire');
  const pAcc=document.getElementById('equipe-panel-acces');
  const t1=document.getElementById('equipe-tab-annuaire');
  const t2=document.getElementById('equipe-tab-acces');
  if(pAnn)pAnn.style.display=tab==='annuaire'?'flex':'none';
  if(pAcc)pAcc.style.display=tab==='acces'?'flex':'none';
  if(t1)t1.classList.toggle('active',tab==='annuaire');
  if(t2)t2.classList.toggle('active',tab==='acces');
  const addBtn=document.getElementById('ann-btn-add-emp');
  if(addBtn)addBtn.style.display=tab==='annuaire'?'inline-flex':'none';
  const sub=document.getElementById('ann-counter-sub');
  if(sub&&tab==='acces')sub.textContent='Comptes, blocage et mots de passe — liés aux fiches Contacts';
  if(tab==='annuaire')renderAnnuaire();
  else{renderUsersList();renderPwdList();}
}
function initEquipePage(){
  if(!document.getElementById('equipe-panel-annuaire')){renderAnnuaire();return;}
  setEquipeTab(equipeUITab);
}
function showAnnuaireTeamTab(which){
  equipeUITab=which==='acces'?'acces':'annuaire';
  showPage('annuaire');
}

// ANNUAIRE FILTRES
let annFilter='tous';
function setAnnFilter(filter,btn){
  annFilter=filter;
  const chips=document.getElementById('ann-chips-filter');
  (chips?chips.querySelectorAll('.ann-filter'):[]).forEach(b=>b.classList.remove('active'));
  if(btn)btn.classList.add('active');
  renderAnnuaire();
}

// EMOJI PICKER
const EMOJIS=['😊','😄','😂','🙏','👍','👌','✅','❌','⚠️','📋','📎','📧','📞','🔧','🏠','🏢','📅','💰','🎯','✨','💡','🔑','📊','🚀','❤️','😅','🤔','👋','💪','🎉','📝','✏️','🗓️','⏰','📦','🚚','💬','🔔','⭐','🙂'];
function buildEmojiPicker(){
  const grid=document.getElementById('emoji-grid');
  grid.innerHTML=EMOJIS.map(e=>`<button class="emoji-btn" onclick="insertEmoji('${e}')">${e}</button>`).join('');
}
function toggleEmojiPicker(){
  const p=document.getElementById('emoji-picker');
  p.style.display=p.style.display==='none'?'grid':'none';
}
function insertEmoji(e){
  const input=document.getElementById('chat-input');
  input.value+=e;input.focus();
  document.getElementById('emoji-picker').style.display='none';
}
document.addEventListener('click',e=>{
  const p=document.getElementById('emoji-picker');
  if(p&&!p.contains(e.target)&&e.target.textContent!=='😊')p.style.display='none';
});

// PDF
function afficherDevis(data,filename){
  const d=data.devis||{},c=data.client||{},t=data.totaux||{},prods=data.produits||[];
  const fmt=n=>n?Number(n).toFixed(2).replace('.',','):'—';
  const fmtE=n=>fmt(n)!=='—'?fmt(n)+' €':'—';
  const brutHT=t.montant_brut_ht||0,remMt=t.remise_montant||0,totalHT=t.total_ht_net||0,totalTVA=t.tva_montant||0,totalTTC=t.total_ttc||0;
  const tvaTaux=totalHT>0?totalTVA/totalHT:0;
  const div=document.createElement('div');div.className='msg ai';div.style.maxWidth='95%';
  div.innerHTML='<div class="msg-avatar">B</div><div class="bubble" style="width:100%;padding:12px"></div>';
  const bubble=div.querySelector('.bubble');
  bubble.innerHTML=`<div style="font-size:13px;margin-bottom:12px">✅ <strong>${esc(filename)}</strong> — ${prods.length} produit(s) · <strong>${fmtE(totalTTC)}</strong> TTC<br><span style="font-size:11px;color:var(--t3)">Cliquez sur une valeur pour la copier</span></div>`;
  const infoGrid=document.createElement('div');
  infoGrid.style.cssText='display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--b1);border-radius:10px;overflow:hidden;margin-bottom:8px';
  const fields=[{l:'N° Devis',v:d.numero},{l:'Date',v:d.date},{l:'Client',v:c.nom_prenom},{l:'Téléphone',v:c.telephone},{l:'Email',v:c.email,full:true},{l:'Adresse',v:c.adresse_facturation,full:true},{l:'Contact',v:d.contact_nom},{l:'Prestation',v:d.type_prestation}];
  fields.forEach(f=>{
    if(!f.v)return;
    const cell=document.createElement('div');
    cell.style.cssText=`background:var(--s3);padding:9px 11px${f.full?';grid-column:1/-1':''}`;
    cell.innerHTML=`<div style="font-size:10px;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;font-weight:600;margin-bottom:2px">${f.l}</div><div style="font-size:12px;font-weight:500;padding:4px 7px;border-radius:5px;cursor:pointer;border:1px solid transparent;transition:.12s" onmouseover="this.style.borderColor='var(--a)';this.style.background='var(--a3)'" onmouseout="this.style.borderColor='transparent';this.style.background='transparent'" onclick="cpClick(this,'${esc(f.v)}')">${esc(f.v)}</div>`;
    infoGrid.appendChild(cell);
  });
  bubble.appendChild(infoGrid);
  if(prods.length>0){
    const tbl=document.createElement('div');tbl.style.cssText='background:var(--s3);border-radius:10px;overflow:hidden;margin-bottom:8px';
    tbl.innerHTML='<div style="padding:8px 11px;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--t3);border-bottom:1px solid var(--b1)">📦 Produits</div>';
    const table=document.createElement('table');table.style.cssText='width:100%;border-collapse:collapse;font-size:12px';
    table.innerHTML=`<thead><tr style="background:var(--s2)"><th style="padding:7px 10px;text-align:left;font-size:10px;color:var(--t3);font-weight:600;text-transform:uppercase">Désignation</th><th style="padding:7px 10px;text-align:left;font-size:10px;color:var(--t3);font-weight:600;text-transform:uppercase">Dim.</th><th style="padding:7px 10px;text-align:right;font-size:10px;color:var(--t3);font-weight:600;text-transform:uppercase">Qté</th><th style="padding:7px 10px;text-align:right;font-size:10px;color:var(--t3);font-weight:600;text-transform:uppercase">PU HT</th><th style="padding:7px 10px;text-align:right;font-size:10px;color:var(--t3);font-weight:600;text-transform:uppercase">Total HT</th><th style="padding:7px 10px;text-align:right;font-size:10px;color:var(--t3);font-weight:600;text-transform:uppercase">TVA</th><th style="padding:7px 10px;text-align:right;font-size:10px;color:var(--t3);font-weight:600;text-transform:uppercase">TTC</th></tr></thead>`;
    const tbody=document.createElement('tbody');
    prods.forEach(p=>{
      const rm=(p.total_ht||0)*((remMt&&brutHT)?remMt/brutHT:0);
      const hn=(p.total_ht||0)-rm;const ttc=hn*(1+tvaTaux);
      const tr=document.createElement('tr');tr.style.cssText='border-top:1px solid var(--b1);cursor:pointer;transition:.1s';
      tr.onmouseover=()=>tr.style.background='var(--a3)';tr.onmouseout=()=>tr.style.background='';
      const mkTd=(val,right=false)=>`<td style="padding:8px 10px;${right?'text-align:right':''}" onclick="cpClick(this,'${esc(String(val||''))}')">${esc(String(val||'—'))}</td>`;
      tr.innerHTML=`${mkTd(p.designation)}${mkTd(p.dimensions)}${mkTd(p.quantite,true)}${mkTd(fmtE(p.pu_ht),true)}${mkTd(fmtE(p.total_ht),true)}${mkTd(p.tva_taux,true)}<td style="padding:8px 10px;text-align:right;font-weight:600;color:var(--a)" onclick="cpClick(this,'${fmt(ttc)}')">${fmtE(ttc)}</td>`;
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);tbl.appendChild(table);
    const copyBtn=document.createElement('button');
    copyBtn.style.cssText='width:100%;padding:8px;background:transparent;border:none;border-top:1px solid var(--b1);color:var(--t3);font-size:11px;cursor:pointer;font-family:inherit;transition:.15s';
    copyBtn.textContent='📋 Copier tout le tableau';
    copyBtn.onclick=()=>{
      let txt='Désignation\tDimensions\tQté\tPU HT\tTotal HT\tTVA\tTTC\n';
      prods.forEach(p=>{const rm=(p.total_ht||0)*((remMt&&brutHT)?remMt/brutHT:0);const hn=(p.total_ht||0)-rm;const ttc=hn*(1+tvaTaux);txt+=`${p.designation||''}\t${p.dimensions||''}\t${p.quantite||1}\t${fmt(p.pu_ht)}\t${fmt(p.total_ht)}\t${p.tva_taux||''}\t${fmt(ttc)}\n`;});
      navigator.clipboard.writeText(txt).then(()=>{copyBtn.textContent='✅ Copié !';setTimeout(()=>copyBtn.textContent='📋 Copier tout le tableau',1500);});
    };
    tbl.appendChild(copyBtn);bubble.appendChild(tbl);
  }
  const totDiv=document.createElement('div');
  totDiv.style.cssText='display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--b1);border-radius:10px;overflow:hidden';
  [{l:'Brut HT',v:fmtE(brutHT),raw:fmt(brutHT)},{l:'Remise',v:t.remise_pct?t.remise_pct+' = -'+fmtE(remMt):'—',raw:t.remise_pct||''},{l:'Total HT net',v:fmtE(totalHT),raw:fmt(totalHT)},{l:'TVA',v:fmtE(totalTVA),raw:fmt(totalTVA)}].forEach(tot=>{
    const cell=document.createElement('div');cell.style.cssText='background:var(--s3);padding:10px;text-align:center;cursor:pointer;transition:.12s';
    cell.innerHTML=`<div style="font-size:10px;color:var(--t3);text-transform:uppercase;font-weight:600">${tot.l}</div><div style="font-size:13px;font-weight:700;margin-top:3px">${tot.v}</div>`;
    cell.onmouseover=()=>cell.style.background='var(--a3)';cell.onmouseout=()=>cell.style.background='var(--s3)';
    cell.onclick=()=>{navigator.clipboard.writeText(tot.raw);cell.style.background='var(--g2)';setTimeout(()=>cell.style.background='var(--s3)',1200);};
    totDiv.appendChild(cell);
  });
  const ttcCell=document.createElement('div');ttcCell.style.cssText='background:var(--a3);padding:12px;text-align:center;cursor:pointer;grid-column:1/-1;transition:.12s';
  ttcCell.innerHTML=`<div style="font-size:10px;color:var(--a);text-transform:uppercase;font-weight:600">Total TTC</div><div style="font-size:1.4rem;font-weight:800;color:var(--a);margin-top:3px">${fmtE(totalTTC)}</div>`;
  ttcCell.onclick=()=>{navigator.clipboard.writeText(fmt(totalTTC));ttcCell.style.background='var(--g2)';setTimeout(()=>ttcCell.style.background='var(--a3)',1200);};
  totDiv.appendChild(ttcCell);bubble.appendChild(totDiv);
  document.getElementById('chat-area').appendChild(div);scrollChat();
}
function cpClick(el,val){
  if(!val||val==='—')return;
  navigator.clipboard.writeText(val).then(()=>{const o=el.style.background;el.style.background='var(--g2)';el.style.color='var(--g)';setTimeout(()=>{el.style.background=o;el.style.color='';},1200);});
}

// ══════════════════════════════════════════
// MESSAGERIE INTERNE
// ══════════════════════════════════════════
// ══════════════════════════════════════════
// MESSAGERIE — SYSTÈME DYNAMIQUE
// ══════════════════════════════════════════
// Génère un ID de conversation cohérent entre deux utilisateurs
function makeConvId(uid1,uid2){return [uid1,uid2].sort().join('_');}

/** Conversation « Messages internes » avec l’assistant BenAI (séparée de Benjamin). */
function addBenAIInternalConv(uid,convs){
  if(!uid||uid==='benjamin'||uid==='benai'||!convs)return;
  const b=USERS.benai;
  if(!b)return;
  convs[makeConvId(uid,'benai')]={name:'BenAI',other:'benai',color:b.color,initial:b.initial};
}

/** Anciens messages auto (motivation) étaient dans la conv. avec Benjamin — les déplacer une fois vers BenAI. */
function migrateMotivationMessagesToBenaiThread(){
  if(appStorage.getItem('benai_motiv_conv_migrated_v1')==='1')return;
  const mem=getMem();
  if(!mem.messages)return;
  let changed=false;
  getAllUsers().forEach(u=>{
    if(!u||u.id==='benjamin'||u.id==='benai')return;
    const oldCid=makeConvId(u.id,'benjamin');
    const arr=mem.messages[oldCid];
    if(!Array.isArray(arr)||!arr.length)return;
    const motiv=arr.filter(m=>m&&m.motivationBenAI);
    if(!motiv.length)return;
    const newCid=makeConvId(u.id,'benai');
    mem.messages[newCid]=mem.messages[newCid]||[];
    mem.messages[newCid].push(...motiv);
    mem.messages[oldCid]=arr.filter(m=>!m||!m.motivationBenAI);
    if(!mem.messages[oldCid].length)delete mem.messages[oldCid];
    changed=true;
  });
  appStorage.setItem('benai_motiv_conv_migrated_v1','1');
  if(changed)saveMem(mem);
}

const INTERNAL_MSG_THREAD_MAX=400;
let convListRenderTimer=null;
function scheduleRenderConvList(){
  if(convListRenderTimer)clearTimeout(convListRenderTimer);
  convListRenderTimer=setTimeout(()=>{convListRenderTimer=null;renderConvList();},70);
}

// Génère dynamiquement les conversations d'un utilisateur
function getConvsForUser(uid){
  const allUsers=getAllUsers();
  const convs={};
  const me=allUsers.find(u=>u.id===uid);
  const role=me?.role||'assistante';

  // CRM only roles — voient uniquement les autres CRM + Benjamin
  const crmOnly=['commercial','directeur_co','directeur_general'];
  const isCRMOnly=crmOnly.includes(role);

  if(uid==='benjamin'){
    // Benjamin voit tout le monde
    allUsers.filter(u=>u.id!=='benjamin').forEach(u=>{
      const cid=makeConvId('benjamin',u.id);
      convs[cid]={name:u.name,other:u.id,color:u.color,initial:u.initial,role:u.role};
    });
  } else if(isCRMOnly){
    // Commercial/dir. co / dir. général → Benjamin + autres CRM + BenAI (messages auto)
    const ben=allUsers.find(u=>u.id==='benjamin');
    if(ben)convs[makeConvId(uid,'benjamin')]={name:'Benjamin',other:'benjamin',color:ben.color,initial:ben.initial};
    addBenAIInternalConv(uid,convs);
    allUsers.filter(u=>u.id!==uid&&u.id!=='benjamin'&&crmOnly.includes(u.role)).forEach(u=>{
      convs[makeConvId(uid,u.id)]={name:u.name,other:u.id,color:u.color,initial:u.initial,role:u.role};
    });
  } else {
    // Assistante/métreur → Benjamin + toute l'équipe + BenAI (messages auto éventuels)
    const ben=allUsers.find(u=>u.id==='benjamin');
    if(ben)convs[makeConvId(uid,'benjamin')]={name:'Benjamin',other:'benjamin',color:ben.color,initial:ben.initial};
    addBenAIInternalConv(uid,convs);
    allUsers.filter(u=>u.id!==uid&&u.id!=='benjamin').forEach(u=>{
      convs[makeConvId(uid,u.id)]={name:u.name,other:u.id,color:u.color,initial:u.initial,role:u.role};
    });
  }
  // Groupe équipe visible pour tous
  convs['groupe']={name:'Groupe équipe',group:true,color:'linear-gradient(135deg,#E8943A,#B45309)',initial:'👥'};
  return convs;
}

function renderConvList(){
  const list=document.getElementById('conv-list');
  list.innerHTML='<div class="conv-header">Conversations</div>';
  const convs=getConvsForUser(currentUser.id);
  const mem=getMem();
  const lr=getLastRead(currentUser.id);
  if(!currentConv||!convs[currentConv]){
    const firstConvId=Object.keys(convs)[0];
    if(firstConvId)currentConv=firstConvId;
  }
  Object.entries(convs).forEach(([cid,conv])=>{
    const msgs=mem.messages[cid]||[];const last=msgs[msgs.length-1];
    const unread=countUnread(currentUser.id,cid,mem,lr);
    const item=document.createElement('div');
    item.className='conv-item'+(conv.group?' conv-group':'')+(currentConv===cid?' active':'');
    item.dataset.convId=cid;
    item.innerHTML=`<div class="conv-name">${conv.name}</div><div class="conv-preview">${last?esc(last.text.substring(0,30)):'Aucun message'}</div>${unread>0?`<span class="conv-unread-badge">${unread}</span>`:''}`;
    item.onclick=()=>openConv(cid,conv);
    list.appendChild(item);
  });
}

function openConv(cid,conv){
  currentConv=cid;markRead(currentUser.id,cid);refreshMsgBadge();
  renderConvList();
  const header=document.getElementById('thread-header');
  const members=conv.group?getAllUsers().map(u=>u.name).join(', '):'Conversation privée';
  header.innerHTML=`<div class="thread-avatar" style="background:${conv.color}">${conv.initial}</div><div><div class="thread-name">${conv.name}</div><div class="thread-status">${members}</div></div>`;
  document.getElementById('thread-input').style.display='flex';
  renderThread(cid);
}

function createThreadMessageEl(cid,m,idx){
  const mine=m.from===currentUser.id;
  let sender=USERS[m.from]||getExtraUserById(m.from)||{color:'#666',initial:'?'};
  if(m.motivationBenAI){
    sender={color:'linear-gradient(135deg,#E8943A,#B45309)',initial:'B',name:'BenAI'};
  }
  const div=document.createElement('div');div.className='tmsg '+(mine?'mine':'theirs');
  const actionsHtml=mine||currentUser.role==='admin'?`<div class="tmsg-actions"><button class="tmsg-act-btn" onclick="editMsg('${cid}',${idx})">✏️</button><button class="tmsg-act-btn" onclick="deleteMsg('${cid}',${idx})" style="color:var(--r)">🗑️</button></div>`:'';
  div.innerHTML=`
    ${!mine?`<div class="tmsg-av" style="background:${sender.color||'#666'}">${sender.initial||'?'}</div>`:''}
    <div class="tmsg-body">
      <div class="tmsg-bubble" style="${mine?'background:'+currentUser.color:''}">
        ${actionsHtml}${esc(m.text)}
      </div>
      <div class="tmsg-time"><span>${m.time||''}${m.edited?' (modifié)':''}</span>${getReadReceiptHtml(cid,m)}</div>
    </div>
    ${mine?`<div class="tmsg-av" style="background:${currentUser.color}">${currentUser.initial}</div>`:''}`;
  return div;
}

function patchConvListRowForCid(cid){
  const list=document.getElementById('conv-list');
  if(!list)return false;
  const item=list.querySelector('.conv-item[data-conv-id="'+String(cid).replace(/"/g,'')+'"]');
  if(!item)return false;
  const mem=getMem();
  const msgs=mem.messages[cid]||[];
  const last=msgs[msgs.length-1];
  const prev=item.querySelector('.conv-preview');
  if(prev)prev.textContent=last?String(last.text||'').substring(0,30):'Aucun message';
  const unread=countUnread(currentUser.id,cid,mem,getLastRead(currentUser.id));
  let badge=item.querySelector('.conv-unread-badge');
  if(unread>0){
    if(!badge){
      badge=document.createElement('span');
      badge.className='conv-unread-badge';
      item.appendChild(badge);
    }
    badge.textContent=String(unread);
  }else if(badge){
    badge.remove();
  }
  return true;
}

function tryAppendOneThreadMessage(cid){
  if(document.getElementById('page-messages')?.style.display!=='flex'||currentConv!==cid)return false;
  const mem=getMem();
  const raw=mem.messages[cid]||[];
  const L=raw.length;
  if(!L||L>INTERNAL_MSG_THREAD_MAX)return false;
  const area=document.getElementById('thread-msgs');
  if(!area)return false;
  const last=raw[L-1];
  if(!last)return false;
  const domMsgCount=area.querySelectorAll('.tmsg').length;
  if(L===1){
    if(domMsgCount!==0)return false;
  }else if(domMsgCount!==L-1){
    return false;
  }
  const hadBubble=!!area.querySelector('.tmsg-bubble');
  if(!hadBubble)area.innerHTML='';
  area.appendChild(createThreadMessageEl(cid,last,L-1));
  area.scrollTop=area.scrollHeight;
  area.dataset.count=String(L);
  area.dataset.readSig=JSON.stringify(getMem().msg_read_cursor?.[cid]||{});
  return true;
}

function renderThread(cid){
  const mem=getMem();
  const raw=mem.messages[cid]||[];
  const area=document.getElementById('thread-msgs');area.innerHTML='';
  if(!raw.length){
    area.innerHTML='<div style="color:var(--t3);font-size:12px;text-align:center;padding:20px">Aucun message — démarrez la conversation !</div>';
    area.dataset.count='0';
    area.dataset.readSig=JSON.stringify(mem.msg_read_cursor?.[cid]||{});
    return;
  }
  const start=raw.length>INTERNAL_MSG_THREAD_MAX?raw.length-INTERNAL_MSG_THREAD_MAX:0;
  const msgs=raw.slice(start);
  if(start>0){
    const info=document.createElement('div');
    info.style.cssText='text-align:center;font-size:11px;color:var(--t3);padding:8px 6px';
    info.textContent=`${start} message(s) plus ancien(s) — affichage des ${INTERNAL_MSG_THREAD_MAX} derniers`;
    area.appendChild(info);
  }
  const frag=document.createDocumentFragment();
  msgs.forEach((m,relIdx)=>{
    const idx=start+relIdx;
    frag.appendChild(createThreadMessageEl(cid,m,idx));
  });
  area.appendChild(frag);
  area.scrollTop=area.scrollHeight;
  area.dataset.count=String(raw.length);
  area.dataset.readSig=JSON.stringify(mem.msg_read_cursor?.[cid]||{});
}

function sendMsg(){
  const input=document.getElementById('msg-input');const txt=input.value.trim();
  if(!txt||!currentConv)return;input.value='';input.focus();
  const mem=getMem();if(!mem.messages[currentConv])mem.messages[currentConv]=[];
  const now=new Date().toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'});
  mem.messages[currentConv].push({from:currentUser.id,text:txt,time:now,ts:Date.now()});
  saveMem(mem);
  markRead(currentUser.id,currentConv);
  if(!tryAppendOneThreadMessage(currentConv)){
    renderThread(currentConv);
    scheduleRenderConvList();
  }else{
    if(!patchConvListRowForCid(currentConv))scheduleRenderConvList();
    refreshMsgBadge();
  }
  logActivity(`${currentUser.name} a envoyé un message`);
}

function appendBenAIMotivationInternalMessage(recipientUid,plainText){
  const fromUid='benai';
  const cid=makeConvId(recipientUid,fromUid);
  const mem=getMem();
  if(!mem.messages[cid])mem.messages[cid]=[];
  const timeStr=new Date().toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'});
  const text='💪 BenAI\n\n'+String(plainText||'').trim();
  mem.messages[cid].push({from:fromUid,text,time:timeStr,ts:Date.now(),motivationBenAI:true});
  saveMem(mem);
  try{logActivity(`BenAI a envoyé un message d'encouragement à ${getAllUsers().find(u=>u.id===recipientUid)?.name||recipientUid}`);}catch(_){}
  if(currentUser?.id===recipientUid&&document.getElementById('page-messages')?.style.display==='flex'){
    if(currentConv===cid){
      markRead(recipientUid,cid);
      if(!tryAppendOneThreadMessage(cid)){
        renderThread(cid);
        scheduleRenderConvList();
      }else{
        if(!patchConvListRowForCid(cid))scheduleRenderConvList();
        refreshMsgBadge();
      }
    }else{
      scheduleRenderConvList();
    }
  }
  refreshMsgBadge();
}

function deleteMsg(cid,idx){
  if(currentUser.id!=='benjamin'){showDriveNotif('⚠️ Suppression réservée à Benjamin');return;}
  if(!confirm('Supprimer ce message définitivement ?'))return;
  const mem=getMem();
  const arr=mem.messages[cid];
  if(!Array.isArray(arr)||idx<0||idx>=arr.length)return;
  const msg=arr[idx];
  const key=crossNotifMsgKey(msg);
  if(!mem.msg_deletions||typeof mem.msg_deletions!=='object')mem.msg_deletions={};
  if(!Array.isArray(mem.msg_deletions[cid]))mem.msg_deletions[cid]=[];
  if(key&&!mem.msg_deletions[cid].includes(key)){
    mem.msg_deletions[cid].push(key);
    if(mem.msg_deletions[cid].length>400)mem.msg_deletions[cid]=mem.msg_deletions[cid].slice(-400);
  }
  arr.splice(idx,1);
  if(!arr.length)delete mem.messages[cid];
  saveMem(mem);renderThread(cid);scheduleRenderConvList();
}

function editMsg(cid,idx){
  const mem=getMem();const msg=mem.messages[cid]?.[idx];if(!msg)return;
  if(msg.from!==currentUser.id&&currentUser.id!=='benjamin'){showDriveNotif('⚠️ Vous ne pouvez modifier que vos messages');return;}
  const nouveau=prompt('Modifier le message :',msg.text);
  if(!nouveau||!nouveau.trim())return;
  msg.text=nouveau.trim();msg.edited=true;
  saveMem(mem);renderThread(cid);
}

// ══════════════════════════════════════════
// SAV
// ══════════════════════════════════════════
function toggleSavForm(){
  const wrap=document.getElementById('sav-form-wrap');
  if(!wrap)return;
  const isOpen=wrap.style.display!=='none';
  wrap.style.display=isOpen?'none':'block';
  if(!isOpen){
    const socSelect=document.getElementById('sav-soc');
    if(socSelect){
      if(currentUser.role==='assistante'&&(currentUser.societe==='nemausus'||currentUser.societe==='lambert')){
        socSelect.value=currentUser.societe;
        socSelect.disabled=true;
      } else {
        socSelect.disabled=false;
      }
    }
    requestAnimationFrame(()=>{try{wrap.scrollIntoView({behavior:'smooth',block:'nearest'});}catch(_){}});
  }
}

function filterSAV(filter,btn){
  currentFilter=filter;
  document.querySelectorAll('.sav-filter').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');renderSAV();
}

function addSAV(){
  const client=document.getElementById('sav-client').value.trim();
  const pb=document.getElementById('sav-pb').value.trim();
  const soc=document.getElementById('sav-soc').value;
  const commercial=document.getElementById('sav-commercial').value.trim();
  const fourn=document.getElementById('sav-fourn').value.trim();
  const rappelDays=Math.max(1,Number(document.getElementById('sav-rappel-days')?.value||5));
  const rappelInput=document.getElementById('sav-rappel').value;
  const rappel=rappelInput||addDaysISO(rappelDays);
  const commentaire=document.getElementById('sav-comment')?.value.trim()||'';
  const urgent=document.getElementById('sav-urgent').checked;
  const missing=[];
  if(!client)missing.push('Client');
  if(!pb)missing.push('Problème');
  if(missing.length){
    alert('Champs requis ou invalides : '+missing.join(', '));
    return;
  }
  const mem=getMem();
  mem.sav.unshift({id:Date.now(),client,probleme:pb,societe:soc,commercial,fournisseur:fourn,rappel,rappelDays,urgent,statut:'nouveau',date_creation:new Date().toLocaleDateString('fr-FR'),by:currentUser.name,actions:[],commentaire,archive:false,muteReminder:false,sync_ts:Date.now(),_deleted:false});
  saveMem(mem);
  updatePatterns(fourn,client);
  ['sav-client','sav-pb','sav-commercial','sav-fourn','sav-rappel','sav-comment'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  const rappelDaysEl=document.getElementById('sav-rappel-days');if(rappelDaysEl)rappelDaysEl.value='5';
  document.getElementById('sav-urgent').checked=false;
  const ok=document.getElementById('sav-ok');ok.textContent='✅ SAV enregistré !';
  setTimeout(()=>{ok.textContent='';toggleSavForm();},1500);
  renderSAV();refreshSAVBadge();logActivity(`${currentUser.name} a ouvert un SAV : ${client}`);
}

function renderSAV(){
  const mem=getMem();const list=document.getElementById('sav-list');
  let savs=(mem.sav||[]).filter(s=>!s._deleted);const isAdmin=currentUser.role==='admin';
  const search=(document.getElementById('sav-search')?.value||'').toLowerCase();
  // Filtre société
  if(currentUser.role==='assistante'&&(currentUser.societe==='nemausus'||currentUser.societe==='lambert')){
    savs=savs.filter(s=>s.societe===currentUser.societe);
  }
  // Filtre état
  if(currentFilter==='nouveau')savs=savs.filter(s=>s.statut==='nouveau'&&!s.archive);
  else if(currentFilter==='en_cours')savs=savs.filter(s=>s.statut==='en_cours');
  else if(currentFilter==='regle')savs=savs.filter(s=>s.statut==='regle');
  else if(currentFilter==='archive')savs=savs.filter(s=>s.archive);
  else if(currentFilter==='urgent')savs=savs.filter(s=>s.urgent&&s.statut!=='regle');
  else savs=savs.filter(s=>s.statut!=='regle'&&!s.archive);
  // Recherche
  if(search)savs=savs.filter(s=>(s.client||'').toLowerCase().includes(search)||(s.probleme||'').toLowerCase().includes(search)||(s.commercial||'').toLowerCase().includes(search)||(s.fournisseur||'').toLowerCase().includes(search));

  // Rappels SAV
  const rappelArea=document.getElementById('sav-reminders-area');
  if(rappelArea){
    const today=new Date();today.setHours(0,0,0,0);
    const rappels=(mem.sav||[]).filter(s=>{
      if(s._deleted)return false;
      if(!s.rappel||s.statut==='regle'||s.archive||s.muteReminder)return false;
      const d=new Date(s.rappel);d.setHours(0,0,0,0);
      const diff=Math.ceil((d-today)/(1000*60*60*24));
      return diff<=7&&diff>=0;
    });
    rappelArea.innerHTML=rappels.map(s=>{
      const d=new Date(s.rappel);d.setHours(0,0,0,0);
      const diff=Math.ceil((d-today)/(1000*60*60*24));
      const label=diff===0?'AUJOURD\'HUI':diff===1?'DEMAIN':`dans ${diff} jours`;
      return `<div class="reminder-alert">🔔 Rappel SAV — <strong>${esc(s.client)}</strong> — ${label} (${formatDate(s.rappel)})</div>`;
    }).join('');
  }

  if(!savs.length){list.innerHTML=`<div style="color:var(--t3);font-size:13px;padding:24px;text-align:center">${currentFilter==='regle'?'Aucun SAV réglé':'Aucun SAV en cours 🎉'}</div>`;return;}

  // Trier par priorité (admin uniquement, vue normale)
  if(isAdmin&&currentFilter==='tous'){
    savs.sort((a,b)=>computeSAVPriority(b)-computeSAVPriority(a));
  }

  const stateLabel={nouveau:'<span class="state-new">🔴 Nouveau</span>',en_cours:'<span class="state-progress">🟡 En cours</span>',regle:'<span class="state-done">🟢 Réglé</span>'};

  list.innerHTML=savs.map(s=>{
    const derniereAction=s.actions&&s.actions.length>0?s.actions[s.actions.length-1]:null;
    const hist=getClientHistory(s.client,s.id,mem);
    const rappelBadge=s.rappel?`<span class="sav-reminder-badge">🔔 ${formatDate(s.rappel)}</span>`:'';
    const priority=computeSAVPriority(s);
    const priorityBadge=isAdmin?getPriorityBadge(priority):'';
    const daysSinceCreation=getSAVAgeDays(s);
    const proactiveSuggestion=isAdmin&&daysSinceCreation>=5&&s.statut!=='regle'&&s.fournisseur?
      `<div style="margin-top:6px;padding:7px 10px;background:var(--bl2);border:1px solid rgba(96,165,250,.2);border-radius:8px;font-size:11px;color:var(--bl);display:flex;align-items:center;gap:8px">
        💡 <span style="flex:1">Sans réponse depuis ${daysSinceCreation} jours — Veux-tu que je rédige l'email pour <strong>${esc(s.fournisseur)}</strong> ?</span>
        <button onclick="suggestFournisseurEmail('${esc(s.id)}','${esc(s.client)}','${esc(s.probleme)}','${esc(s.fournisseur)}')" style="padding:4px 10px;background:var(--bl);color:#fff;border:none;border-radius:6px;font-size:10px;font-weight:600;cursor:pointer;font-family:inherit;white-space:nowrap">✍️ Rédiger</button>
      </div>`:'';
    return `<div class="sav-card${s.urgent?' urgent':''}">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap">
        ${priorityBadge}
        ${stateLabel[s.statut]||''}
        <span class="badge ${s.societe==='nemausus'?'badge-nem':'badge-lam'}">${s.societe==='nemausus'?'Nemausus':'Lambert'}</span>
        ${s.urgent?'<span class="badge badge-urgent">🚨 URGENT</span>':''}
        ${rappelBadge}
        <span style="font-size:10px;color:var(--t3);margin-left:auto">${s.date_creation||''}${daysSinceCreation>0?' · '+daysSinceCreation+'j':''}</span>
        ${isAdmin?`<button onclick="editSAV(${s.id})" title="Modifier" style="background:none;border:none;color:var(--a);cursor:pointer;font-size:14px;padding:2px 4px">✏️</button><button onclick="deleteSAV(${s.id})" title="Supprimer" style="background:none;border:none;color:var(--r);cursor:pointer;font-size:14px;padding:2px 4px">🗑️</button>`:''}
      </div>
      <div class="sav-client">${esc(s.client)}</div>
      <div class="sav-pb">${esc(s.probleme)}</div>
      ${s.commentaire?`<div style="font-size:11px;color:var(--t2);margin-top:4px">💬 ${esc(s.commentaire)}</div>`:''}
      <div class="sav-meta">${s.commercial?'Commercial : '+esc(s.commercial)+' · ':''}${s.fournisseur?'Fournisseur : '+esc(s.fournisseur)+' · ':''}Créé par ${esc(s.by||'?')}</div>
      ${derniereAction?`<div style="margin-top:6px;padding:6px 10px;background:var(--s3);border-radius:6px;font-size:11px;color:var(--t2)">✅ <strong>${esc(derniereAction.action)}</strong> — ${esc(derniereAction.date)}</div>`:''}
      ${proactiveSuggestion}
      ${currentUser.role!=='admin'?`<div style="margin-top:4px;font-size:10px;color:var(--g)">${getSAVVu(s.id)?'👁️ Vu par Benjamin le '+getSAVVu(s.id):'⏳ Pas encore vu par Benjamin'}</div>`:''}
      ${hist.length>0?`<div style="margin-top:6px"><details><summary style="font-size:11px;color:var(--t3);cursor:pointer">🕐 Historique client (${hist.length} SAV précédent(s))</summary><div style="margin-top:6px">${hist.map(h=>`<div class="hist-item"><div class="hist-date">${h.date_creation||''} · ${h.statut}</div><div class="hist-pb">${esc(h.probleme)}</div></div>`).join('')}</div></details></div>`:''}
      ${isAdmin&&s.statut!=='regle'?`<div style="margin-top:8px;display:flex;gap:6px;flex-wrap:wrap">
        <input type="text" class="sav-action-input" id="action-${s.id}" placeholder="Ce que tu as fait (ex: Commandé chez Somfy le 12/04...)">
        <button onclick="savAction(${s.id})" style="padding:7px 12px;background:var(--a);color:#fff;border:none;border-radius:7px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;white-space:nowrap">✓ Enregistrer</button>
        <button onclick="changeStatut(${s.id},'regle')" class="btn-resolve">🟢 Réglé</button>
        <button onclick="toggleSavArchive(${s.id})" style="padding:7px 12px;background:var(--s3);color:var(--t2);border:1px solid var(--b1);border-radius:7px;font-size:12px;cursor:pointer;font-family:inherit">${s.archive?'🔓 Réouvrir':'📁 Archiver'}</button>
        <button onclick="toggleSavMute(${s.id})" style="padding:7px 12px;background:${s.muteReminder?'var(--y2)':'var(--s3)'};color:${s.muteReminder?'var(--y)':'var(--t2)'};border:1px solid var(--b1);border-radius:7px;font-size:12px;cursor:pointer;font-family:inherit">${s.muteReminder?'🔕 Sourdine':'🔔 Activer rappel'}</button>
      </div>`:''}
    </div>`;
  }).join('');
}

function getClientHistory(client,excludeId,mem){
  return (mem.sav||[]).filter(s=>!s._deleted&&s.client===client&&s.id!==excludeId).slice(0,5);
}

function deleteSAV(id){
  if(!confirm('Supprimer ce SAV définitivement ?'))return;
  const mem=getMem();
  const sav=mem.sav.find(s=>s.id===id);
  if(!sav)return;
  sav._deleted=true;
  sav.sync_ts=Date.now();
  saveMem(mem);renderSAV();refreshSAVBadge();logActivity(`${currentUser.name} a supprimé un SAV`);
}

function editSAV(id){
  const mem=getMem();const s=mem.sav.find(x=>x.id===id);if(!s)return;
  document.getElementById('sav-client').value=s.client||'';
  document.getElementById('sav-commercial').value=s.commercial||'';
  document.getElementById('sav-pb').value=s.probleme||'';
  document.getElementById('sav-soc').value=s.societe||'nemausus';
  document.getElementById('sav-soc').disabled=false;
  document.getElementById('sav-fourn').value=s.fournisseur||'';
  document.getElementById('sav-rappel').value=s.rappel||'';
  document.getElementById('sav-urgent').checked=s.urgent||false;
  const btn=document.querySelector('#sav-form-wrap .btn-primary');
  if(btn){btn.textContent='💾 Modifier';btn.onclick=()=>saveEditSAV(id);}
  document.getElementById('sav-form-wrap').style.display='block';
  document.getElementById('sav-form-wrap').scrollIntoView({behavior:'smooth'});
}

function saveEditSAV(id){
  const client=document.getElementById('sav-client').value.trim();
  const pb=document.getElementById('sav-pb').value.trim();
  const missing=[];
  if(!client)missing.push('Client');
  if(!pb)missing.push('Problème');
  if(missing.length){
    alert('Champs vides : '+missing.join(', '));
    return;
  }
  const mem=getMem();const idx=mem.sav.findIndex(s=>s.id===id);if(idx===-1)return;
  mem.sav[idx]={
    ...mem.sav[idx],
    client,probleme:pb,
    commercial:document.getElementById('sav-commercial').value.trim(),
    societe:document.getElementById('sav-soc').value,
    fournisseur:document.getElementById('sav-fourn').value.trim(),
    rappel:document.getElementById('sav-rappel').value,
    urgent:document.getElementById('sav-urgent').checked,
    commentaire:document.getElementById('sav-comment')?.value.trim()||'',
    sync_ts:Date.now()
  };
  saveMem(mem);
  const btn=document.querySelector('#sav-form-wrap .btn-primary');
  if(btn){btn.textContent='Enregistrer';btn.onclick=addSAV;}
  document.getElementById('sav-form-wrap').style.display='none';
  ['sav-client','sav-pb','sav-commercial','sav-fourn','sav-rappel','sav-comment'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  document.getElementById('sav-urgent').checked=false;
  renderSAV();logActivity(`Benjamin a modifié le SAV de ${client}`);
}

function savAction(id){
  const input=document.getElementById('action-'+id);const txt=input?.value.trim();if(!txt)return;
  const mem=getMem();const sav=mem.sav.find(s=>s.id===id);if(!sav)return;
  if(!sav.actions)sav.actions=[];
  sav.actions.push({action:txt,date:new Date().toLocaleDateString('fr-FR'),by:currentUser.name});
  sav.statut='en_cours';
  sav.sync_ts=Date.now();
  saveMem(mem);renderSAV();refreshSAVBadge();logActivity(`${currentUser.name} a traité SAV : ${sav.client}`);
}

function changeStatut(id,statut){
  const mem=getMem();const sav=mem.sav.find(s=>s.id===id);
  if(sav){sav.statut=statut;if(statut==='regle')sav.date_reglement=new Date().toLocaleDateString('fr-FR');sav.sync_ts=Date.now();saveMem(mem);renderSAV();refreshSAVBadge();logActivity(`${currentUser.name} a réglé SAV : ${sav.client}`);}
}

function refreshSAVBadge(){
  const mem=getMem();
  const today=new Date();today.setHours(0,0,0,0);
  const nb=(mem.sav||[]).filter(s=>{
    if(s._deleted)return false;
    if(s.archive||s.statut==='regle')return false;
    const createdBy=(s.by||'').toLowerCase();
    const viewer=(currentUser?.name||'').toLowerCase();
    if(s.urgent&&createdBy!==viewer)return true;
    if(!s.rappel||s.muteReminder)return false;
    const d=new Date(s.rappel);d.setHours(0,0,0,0);
    const diff=Math.ceil((d-today)/(1000*60*60*24));
    return diff<=1;
  }).length;
  const urgent=(mem.sav||[]).filter(s=>!s._deleted&&s.urgent&&s.statut!=='regle'&&!s.archive&&((s.by||'').toLowerCase()!==(currentUser?.name||'').toLowerCase())).length;
  const badge=document.getElementById('sav-badge');
  badge.style.display=nb>0?'flex':'none';badge.textContent=nb;
  if(urgent>0)badge.style.background='var(--r)';else badge.style.background='var(--a)';
}

function toggleSavArchive(id){
  const mem=getMem();const sav=(mem.sav||[]).find(s=>s.id===id);if(!sav)return;
  sav.archive=!sav.archive;
  sav.sync_ts=Date.now();
  saveMem(mem);renderSAV();refreshSAVBadge();
}

function toggleSavMute(id){
  const mem=getMem();const sav=(mem.sav||[]).find(s=>s.id===id);if(!sav)return;
  sav.muteReminder=!sav.muteReminder;
  sav.sync_ts=Date.now();
  saveMem(mem);renderSAV();refreshSAVBadge();
}

function exportSAVCSV(){
  const mem=getMem();let savs=(mem.sav||[]).filter(s=>!s._deleted);
  if(currentUser.role==='assistante'&&(currentUser.societe==='nemausus'||currentUser.societe==='lambert')){
    savs=savs.filter(s=>s.societe===currentUser.societe);
  }
  const header='Date,Client,Problème,Société,Commercial,Fournisseur,Statut,Urgent,Date rappel\n';
  const rows=savs.map(s=>`${s.date_creation||''},${csv(s.client)},${csv(s.probleme)},${s.societe==='nemausus'?'Nemausus Fermetures':'Lambert SAS'},${csv(s.commercial)},${csv(s.fournisseur)},${s.statut},${s.urgent?'Oui':'Non'},${s.rappel||''}`).join('\n');
  download('SAV_BenAI_'+new Date().toLocaleDateString('fr-FR').replace(/\//g,'-')+'.csv',header+rows,'text/csv');
}

// ══════════════════════════════════════════
// NOTES RAPIDES
// ══════════════════════════════════════════
function addNote(){
  const mem=getMem();if(!mem.notes)mem.notes=[];
  mem.notes.unshift({id:Date.now(),text:'',date:new Date().toLocaleDateString('fr-FR'),by:currentUser.id,draft:true,ts:Date.now(),_deleted:false});
  saveMem(mem);renderNotes();setTimeout(()=>{const first=document.querySelector('.note-card textarea');if(first)first.focus();},50);
}

function noteIdEq(a,b){
  return String(a)===String(b);
}

function renderNotes(){
  const mem=getMem();const area=document.getElementById('notes-area');
  const notes=(mem.notes||[]).filter(n=>!n._deleted&&(n.by===currentUser.id||currentUser.role==='admin')&&(String(n.text||'').trim()||n.draft));
  if(!notes.length){area.innerHTML='<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">Aucune note — cliquez sur + Nouvelle note</div>';return;}
  area.innerHTML=notes.map((n,i)=>`
    <div class="note-card">
      <button class="note-del" onclick='deleteNote(${JSON.stringify(String(n.id))})'>✕</button>
      <textarea oninput='saveNote(${JSON.stringify(String(n.id))},this.value)' onblur='finalizeNoteOnBlur(${JSON.stringify(String(n.id))},this)'>${esc(n.text)}</textarea>
      <div class="note-meta"><span>${n.date}</span><span>${USERS[n.by]?.name||n.by}</span></div>
    </div>`).join('');
}

function saveNote(id,val){
  const mem=getMem();const n=mem.notes?.find(x=>noteIdEq(x.id,id));
  if(n){
    n.text=val;
    n.draft=!String(val||'').trim();
    n.ts=Date.now();
  }
  saveMem(mem);
}

function removeEmptyNoteSilent(id){
  const mem=getMem();
  const n=(mem.notes||[]).find(x=>noteIdEq(x.id,id));
  if(n){
    n._deleted=true;
    n.ts=Date.now();
  }
  saveMem(mem);
  renderNotes();
}

function finalizeNoteOnBlur(id,el){
  const txt=String(el?.value||'').trim();
  if(!txt){
    removeEmptyNoteSilent(id);
    return;
  }
  saveNote(id,txt);
  void correctNoteSpelling(id,el);
}

async function correctNoteSpelling(id,el){
  if(!isBenAIDesktopAutocorrect())return;
  const txt=el.value.trim();
  if(!txt||txt.split(' ').length<3)return;
  const apiKey=getApiKey();if(!apiKey)return;
  try{
    const r=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:400,messages:[{role:'user',content:`Corrige uniquement les fautes d'orthographe et de grammaire françaises, sans changer le sens ni le style. Réponds UNIQUEMENT avec le texte corrigé, rien d'autre. Si aucune faute, réponds le texte exact :\n\n${txt}`}]})
    });
    const d=await r.json();
    if(d.content?.[0]?.text){
      const corrected=d.content[0].text.trim();
      if(corrected&&corrected!==txt){
        el.value=corrected;
        saveNote(id,corrected);
      }
    }
  }catch(e){}
}


function deleteNote(id){
  if(!confirm('Supprimer cette note ?'))return;
  const mem=getMem();
  const n=(mem.notes||[]).find(x=>noteIdEq(x.id,id));
  if(!n)return;
  n._deleted=true;
  n.ts=Date.now();
  saveMem(mem);
  renderNotes();
}

// ══════════════════════════════════════════
// ABSENCES (Benjamin uniquement)
// ══════════════════════════════════════════
function toggleAbsForm(){
  if(currentUser?.role!=='admin')return;
  const wrap=document.getElementById('abs-form-wrap');
  if(!wrap)return;
  const open=wrap.style.display==='none';
  if(open){
    resetAbsenceFormForCreate();
    fillAbsEmpList();
    wrap.style.display='block';
    requestAnimationFrame(()=>{try{wrap.scrollIntoView({behavior:'smooth',block:'nearest'});}catch(_){}});
  } else {
    wrap.style.display='none';
    setAbsFormModeCreate();
  }
}

function getAssistanteUserIds(){
  return getAllUsers().filter(u=>u.role==='assistante').map(u=>u.id);
}
function absenceIdEq(a,b){
  return String(a)===String(b);
}
function absenceNotifsIncludeUser(absence,uid){
  if(!uid||!absence)return false;
  const list=Array.isArray(absence.notifs)&&absence.notifs.length?absence.notifs:['benjamin'];
  const u=String(uid).trim();
  return list.some(n=>String(n).trim()===u);
}
function hasAbsencesSharedWithUser(user){
  if(!user)return false;
  if(user.role==='admin')return true;
  const mem=getMem();
  const uid=user.id||'';
  return (mem.absences||[]).some(a=>!a._deleted&&absenceNotifsIncludeUser(a,uid));
}
function updateNavAbsencesVisibility(){
  const nav=document.getElementById('nav-absences');
  if(!nav||!currentUser)return;
  const allowed=ROLE_PAGES[currentUser.role]||ROLE_PAGES['assistante'];
  if(!allowed.includes('absences')){
    nav.style.display='none';
    refreshAbsBadge();
    return;
  }
  if(currentUser.role==='admin'){
    nav.style.display='flex';
    refreshAbsBadge();
    return;
  }
  const show=hasAbsencesSharedWithUser(currentUser);
  nav.style.display=show?'flex':'none';
  if(!show){
    const absPage=document.getElementById('page-absences');
    if(absPage&&absPage.style.display==='flex'){
      const al=ROLE_PAGES[currentUser.role]||ROLE_PAGES['assistante'];
      const fallback=al.find(p=>p!=='absences')||al[0]||'benai';
      showPage(fallback);
    }
  }
  refreshAbsBadge();
}
function configureAbsencesPageForRole(){
  const isAd=currentUser?.role==='admin';
  document.querySelectorAll('#page-absences .abs-admin-only').forEach(el=>{
    el.style.display=isAd?'flex':'none';
  });
  const sub=document.getElementById('abs-page-sub');
  if(sub)sub.textContent=isAd?'Rappels J-7 et la veille':'Absences partagées avec vous (rappels aussi dans BenAI IA)';
  const wrap=document.getElementById('abs-form-wrap');
  if(wrap&&!isAd)wrap.style.display='none';
}
function buildAbsenceNotifsArray(){
  let notifs=[...document.querySelectorAll('.abs-notif-cb:checked')].map(cb=>cb.dataset.uid);
  const assistanteIds=getAssistanteUserIds();
  const shareAsst=document.getElementById('abs-share-assistantes')?.checked!==false;
  if(shareAsst&&assistanteIds.length)
    notifs=[...new Set([...notifs,...assistanteIds])];
  else if(!shareAsst&&assistanteIds.length){
    const asSet=new Set(assistanteIds);
    notifs=notifs.filter(id=>!asSet.has(id));
  }
  return notifs;
}

function setAbsFormModeCreate(){
  const title=document.getElementById('abs-form-title');
  if(title)title.textContent='📅 Nouvelle absence';
  const btn=document.querySelector('#abs-form-wrap .btn-primary');
  if(btn){
    btn.textContent='Enregistrer';
    btn.onclick=addAbsence;
  }
}

function setAbsFormModeEdit(id){
  const title=document.getElementById('abs-form-title');
  if(title)title.textContent='✏️ Modifier absence';
  const btn=document.querySelector('#abs-form-wrap .btn-primary');
  if(btn){
    btn.textContent='💾 Modifier';
    btn.onclick=()=>saveEditAbsence(id);
  }
}

function resetAbsenceFormForCreate(){
  ['abs-debut','abs-fin','abs-note','abs-heure-debut','abs-heure-fin'].forEach(id=>{
    const el=document.getElementById(id);
    if(el)el.value='';
  });
  const typeEl=document.getElementById('abs-type');
  if(typeEl)typeEl.value='Congé';
  const empEl=document.getElementById('abs-emp-select');
  if(empEl)empEl.value='';
  setAbsFormModeCreate();
}

function addAbsence(){
  if(currentUser?.role!=='admin')return;
  const emp=document.getElementById('abs-emp-select')?.value||'';
  const debut=document.getElementById('abs-debut').value;
  const fin=document.getElementById('abs-fin').value;
  const heureDebut=document.getElementById('abs-heure-debut')?.value||'';
  const heureFin=document.getElementById('abs-heure-fin')?.value||'';
  const type=document.getElementById('abs-type').value;
  const note=document.getElementById('abs-note').value.trim();
  if(!emp){alert('Sélectionnez un employé');return;}
  if(!debut||!fin){alert('Remplissez les dates');return;}
  if(new Date(fin)<new Date(debut)){alert('⚠️ La date de fin doit être après le début');return;}
  const notifs=buildAbsenceNotifsArray();
  const mem=getMem();
  if(!mem.absences)mem.absences=[];
  mem.absences.unshift({
    id:Date.now(),
    employe:emp,
    debut,
    fin,
    heureDebut,
    heureFin,
    type,
    note,
    notifs,
    createdAt:new Date().toISOString(),
    by:currentUser?.id||'benjamin',
    sync_ts:Date.now(),
    _deleted:false
  });
  saveMem(mem);
  resetAbsenceFormForCreate();
  toggleAbsForm();renderAbsences();logActivity(`Benjamin a enregistré une absence pour ${emp}`);
}

function deleteAbsence(id){
  if(currentUser?.role!=='admin')return;
  if(!confirm('Supprimer cette absence ?'))return;
  const mem=getMem();
  const abs=(mem.absences||[]).find(a=>absenceIdEq(a.id,id));
  if(!abs)return;
  abs._deleted=true;
  abs.sync_ts=Date.now();
  saveMem(mem);
  renderAbsences();
}

function editAbsence(id){
  if(currentUser?.role!=='admin')return;
  const mem=getMem();const a=(mem.absences||[]).find(x=>absenceIdEq(x.id,id));if(!a)return;
  fillAbsEmpList({selectedEmp:a.employe||'',selectedNotifs:Array.isArray(a.notifs)?a.notifs:null});
  const sel=document.getElementById('abs-emp-select');
  if(sel)sel.value=a.employe||'';
  document.getElementById('abs-debut').value=a.debut||'';
  document.getElementById('abs-fin').value=a.fin||'';
  if(document.getElementById('abs-heure-debut'))document.getElementById('abs-heure-debut').value=a.heureDebut||'';
  if(document.getElementById('abs-heure-fin'))document.getElementById('abs-heure-fin').value=a.heureFin||'';
  document.getElementById('abs-type').value=a.type||'Congé';
  document.getElementById('abs-note').value=a.note||'';
  setAbsFormModeEdit(id);
  document.getElementById('abs-form-wrap').style.display='block';
  document.getElementById('abs-form-wrap').scrollIntoView({behavior:'smooth'});
}

function saveEditAbsence(id){
  if(currentUser?.role!=='admin')return;
  const emp=document.getElementById('abs-emp-select')?.value||'';
  const debut=document.getElementById('abs-debut').value;
  const fin=document.getElementById('abs-fin').value;
  if(!emp||!debut||!fin){alert('Remplissez tous les champs obligatoires');return;}
  if(new Date(fin)<new Date(debut)){alert('⚠️ La date de fin doit être après le début');return;}
  const notifs=buildAbsenceNotifsArray();
  const mem=getMem();const idx=(mem.absences||[]).findIndex(a=>absenceIdEq(a.id,id));if(idx===-1)return;
  mem.absences[idx]={...mem.absences[idx],employe:emp,debut,fin,
    heureDebut:document.getElementById('abs-heure-debut')?.value||'',
    heureFin:document.getElementById('abs-heure-fin')?.value||'',
    type:document.getElementById('abs-type').value,
    note:document.getElementById('abs-note').value.trim(),
    notifs,
    sync_ts:Date.now()
  };
  saveMem(mem);
  setAbsFormModeCreate();
  document.getElementById('abs-form-wrap').style.display='none';
  renderAbsences();logActivity(`Benjamin a modifié l'absence de ${emp}`);
}

let currentAbsSort='asc';

function setAbsSort(mode,btn){
  currentAbsSort=mode;
  document.querySelectorAll('#abs-sort-asc,#abs-sort-desc,#abs-sort-emp').forEach(b=>b.classList.remove('active'));
  if(btn)btn.classList.add('active');
  renderAbsences();
}

function refreshAbsEmpFilter(){
  const sel=document.getElementById('abs-filter-emp');if(!sel)return;
  const mem=getMem();
  const uid=currentUser?.id||'';
  const isAd=currentUser?.role==='admin';
  const pool=(mem.absences||[]).filter(a=>!a._deleted&&(isAd||absenceNotifsIncludeUser(a,uid)));
  const emps=[...new Set(pool.map(a=>a.employe))].sort((a,b)=>a.localeCompare(b,'fr'));
  const cur=sel.value;
  sel.innerHTML='<option value="">Tous les employés</option>'+emps.map(e=>`<option value="${esc(e)}"${cur===e?' selected':''}>${esc(e)}</option>`).join('');
}

function renderAbsences(){
  const mem=getMem();const list=document.getElementById('abs-list');
  const isAbsAdmin=currentUser?.role==='admin';
  refreshAbsEmpFilter();
  const filterEmp=(document.getElementById('abs-filter-emp')?.value||'').trim();
  let absences=(mem.absences||[]).filter(a=>!a._deleted&&(!filterEmp||a.employe===filterEmp));
  if(!isAbsAdmin){
    const uid=currentUser?.id||'';
    absences=absences.filter(a=>absenceNotifsIncludeUser(a,uid));
  }
  if(!absences.length){
    list.innerHTML=`<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">${isAbsAdmin?'Aucune absence enregistrée':'Aucune absence partagée avec vous pour l’instant'}</div>`;
    updateNavAbsencesVisibility();
    return;
  }
  const today=new Date();today.setHours(0,0,0,0);

  const getStatus=(a)=>{
    const debut=new Date(a.debut);debut.setHours(0,0,0,0);
    const fin=new Date(a.fin);fin.setHours(0,0,0,0);
    if(today>fin)return{status:'Passée',statusClass:'abs-past'};
    if(today>=debut)return{status:'En cours',statusClass:'abs-today'};
    const diff=Math.ceil((debut-today)/(1000*60*60*24));
    return{status:diff===1?'Demain':`Dans ${diff} jour(s)`,statusClass:'abs-future'};
  };

  const renderCard=(a)=>{
    const jsId=JSON.stringify(String(a.id));
    const {status,statusClass}=getStatus(a);
    const notifIcons=(a.notifs||['benjamin']).map(n=>n==='benjamin'?'👨':'👩').join('');
    const heures=a.heureDebut&&a.heureFin?` · ${a.heureDebut}→${a.heureFin}`:a.heureDebut?` · dès ${a.heureDebut}`:'';
    const icon=a.type==='Congé'?'🏖️'
      :a.type==='Maladie'?'🤒'
      :a.type==='RTT'?'🕘'
      :a.type==='Accident du travail'?'🚑'
      :a.type==='Formation'?'📚'
      :'🗓️';
    const actions=isAbsAdmin?`<button type="button" onclick='editAbsence(${jsId})' style="background:none;border:none;color:var(--a);cursor:pointer;font-size:16px;padding:2px" title="Modifier">✏️</button>
      <button type="button" onclick='deleteAbsence(${jsId})' style="background:none;border:none;color:var(--r);cursor:pointer;font-size:16px;padding:2px" title="Supprimer">🗑️</button>`:'';
    return `<div style="padding:10px 14px;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--b1)">
      <span style="font-size:18px">${icon}</span>
      <div style="flex:1">
        <div style="font-size:12px;font-weight:600">${esc(a.employe)} — ${a.type} <span style="font-size:10px;color:var(--t3)">${notifIcons}</span></div>
        <div style="font-size:11px;color:var(--t2)">Du ${formatDate(a.debut)} au ${formatDate(a.fin)}${heures}</div>
        ${a.note?`<div style="font-size:10px;color:var(--t3)">${esc(a.note)}</div>`:''}
      </div>
      <span class="absence-status ${statusClass}" style="font-size:10px">${status}</span>
      ${actions}
    </div>`;
  };

  if(currentAbsSort==='emp'){
    // Grouper par employé alphabétique, puis date croissante dans chaque groupe
    const byEmp={};
    absences.forEach(a=>{
      if(!byEmp[a.employe])byEmp[a.employe]=[];
      byEmp[a.employe].push(a);
    });
    Object.keys(byEmp).forEach(e=>byEmp[e].sort((a,b)=>new Date(a.debut)-new Date(b.debut)));
    const emps=Object.keys(byEmp).sort((a,b)=>a.localeCompare(b,'fr'));
    list.innerHTML=emps.map(emp=>`
      <div style="margin-bottom:14px;background:var(--s2);border:1px solid var(--b1);border-radius:14px;overflow:hidden">
        <div style="padding:10px 14px;background:var(--s3);border-bottom:1px solid var(--b1);display:flex;align-items:center;gap:8px">
          <span style="font-size:16px">👤</span>
          <span style="font-size:13px;font-weight:700;flex:1">${esc(emp)}</span>
          <span style="background:var(--a3);color:var(--a);border-radius:10px;padding:1px 8px;font-size:10px;font-weight:700">${byEmp[emp].length} absence(s)</span>
        </div>
        ${byEmp[emp].map(renderCard).join('')}
      </div>`).join('');
  } else {
    // Tri global par date
    absences.sort((a,b)=>{
      const da=new Date(a.debut),db=new Date(b.debut);
      const fa=new Date(a.fin),fb=new Date(b.fin);
      const aPast=today>fa,bPast=today>fb;
      if(currentAbsSort==='asc'){
        if(aPast&&!bPast)return 1;
        if(!aPast&&bPast)return -1;
        return da-db;
      } else {
        if(aPast&&!bPast)return 1;
        if(!aPast&&bPast)return -1;
        return db-da;
      }
    });
    list.innerHTML=`<div style="background:var(--s2);border:1px solid var(--b1);border-radius:14px;overflow:hidden">
      ${absences.map(renderCard).join('')}
    </div>`;
  }
  updateNavAbsencesVisibility();
}

function checkAbsenceReminders(){_checkRemindersFor('benjamin');}
function checkAbsenceRemindersForUser(uid){_checkRemindersFor(uid);}
function _checkRemindersFor(uid){
  const mem=getMem();const absences=(mem.absences||[]).filter(a=>!a._deleted);
  const today=new Date();today.setHours(0,0,0,0);
  const dayKey=today.toISOString().slice(0,10);
  const seenKey=`benai_abs_reminders_${uid}_${dayKey}`;
  if(appStorage.getItem(seenKey)==='1')return;
  const reminders=[];
  absences.forEach(a=>{
    if(!absenceNotifsIncludeUser(a,uid))return;
    const debut=new Date(a.debut);debut.setHours(0,0,0,0);
    const diff=Math.ceil((debut-today)/(1000*60*60*24));
    if(diff===7)reminders.push(`📅 Dans 7 jours : **${a.employe}** absent(e) — ${a.type} (du ${formatDate(a.debut)} au ${formatDate(a.fin)})`);
    if(diff===1)reminders.push(`⚠️ DEMAIN : **${a.employe}** sera absent(e) — ${a.type}`);
    if(diff===0)reminders.push(`🔴 AUJOURD'HUI : **${a.employe}** est absent(e) (jusqu'au ${formatDate(a.fin)})`);
  });
  if(reminders.length>0){
    appStorage.setItem(seenKey,'1');
    setTimeout(()=>{reminders.forEach(r=>addAIMsg('**Rappel absence** : '+r));},800);
  }
}

// SAV VU PAR BENJAMIN
function markSAVVu(){
  if(currentUser?.role!=='admin')return;
  const mem=getMem();if(!mem.sav_vu)mem.sav_vu={};
  const today=new Date().toLocaleDateString('fr-FR');
  (mem.sav||[]).forEach(s=>{if(!s._deleted&&!mem.sav_vu[s.id])mem.sav_vu[s.id]=today;});
  saveMem(mem);
}
function getSAVVu(id){return((getMem().sav_vu)||{})[id]||null;}

// ANNUAIRE
function getAnnuaire(){try{return JSON.parse(appStorage.getItem('benai_annuaire'))||[];}catch{return[];}}
function getAnnuaireActive(list){
  const items=Array.isArray(list)?list:getAnnuaire();
  return items.filter(e=>e&&!e._deleted);
}
function saveAnnuaire(a,sync=true){
  appStorage.setItem('benai_annuaire',JSON.stringify(a));
  if(sync){
    markSharedDirtyIfChanged('annuaire',a||[]);
    scheduleSupabaseSync(getMem(),a);
  }
}
function toggleEmpForm(){
  const w=document.getElementById('emp-form-wrap');
  if(!w)return;
  const show=w.style.display==='none'||w.style.display==='';
  w.style.display=show?'block':'none';
  if(show)requestAnimationFrame(()=>{try{w.scrollIntoView({behavior:'smooth',block:'nearest'});}catch(_){}});
}

function addEmploye(){
  const prenom=document.getElementById('emp-prenom').value.trim();
  const nom=document.getElementById('emp-nom').value.trim();
  const email=document.getElementById('emp-email').value.trim();
  const emailPro=document.getElementById('emp-email-pro').value.trim();
  const tel=document.getElementById('emp-tel').value.trim();
  const naissance=document.getElementById('emp-naissance').value;
  const fonction=document.getElementById('emp-fonction').value;
  const soc=document.getElementById('emp-soc').value;
  if(!prenom||!nom){alert('Prénom et Nom sont obligatoires');return;}
  const ann=getAnnuaire();
  ann.push({id:Date.now(),prenom,nom,email,emailPro,tel,naissance,fonction,societe:soc,sync_ts:Date.now(),_deleted:false});
  saveAnnuaire(ann);
  ['emp-prenom','emp-nom','emp-email','emp-email-pro','emp-tel','emp-naissance'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  const ok=document.getElementById('emp-ok');ok.textContent='✅ Enregistré !';
  setTimeout(()=>{ok.textContent='';toggleEmpForm();},1200);
  renderAnnuaire();logActivity(`Benjamin a ajouté ${prenom} ${nom} dans l'annuaire`);
}

async function deleteEmploye(id){
  const ann=getAnnuaire();
  const emp=ann.find(e=>e.id===id);
  if(!emp)return;
  const linked=findBenaiAccountLinkedToAnnuaireEmploye(emp);
  const label=`${String(emp.prenom||'').trim()} ${String(emp.nom||'').trim()}`.trim()||'cet employé';
  let msg=`Retirer ${label} de l'annuaire ?`;
  if(linked){
    msg+=`\n\nUn accès BenAI actif (${linked.name||linked.id}) sera supprimé (cloud + local).`;
  }
  if(!confirm(msg))return;
  if(linked){
    if(!canManageBenaiUsersAdmin()){
      alert("La suppression d'accès BenAI est réservée à l'administrateur. L'annuaire n'a pas été modifié.");
      return;
    }
    if(normalizeId(linked.id)!=='benjamin'){
      await supprimerUtilisateur(linked.id,{skipConfirm:true});
    }
  }
  logDeletion('Employé annuaire',(emp?.prenom||'')+' '+(emp?.nom||''));
  emp._deleted=true;
  emp.sync_ts=Date.now();
  saveAnnuaire(ann);renderAnnuaire();
}

function editEmploye(id){
  const ann=getAnnuaire();
  const e=ann.find(x=>x.id===id);if(!e)return;
  document.getElementById('emp-prenom').value=e.prenom||'';
  document.getElementById('emp-nom').value=e.nom||'';
  document.getElementById('emp-email').value=e.email||'';
  const epro=document.getElementById('emp-email-pro');if(epro)epro.value=e.emailPro||'';
  document.getElementById('emp-tel').value=e.tel||'';
  const enais=document.getElementById('emp-naissance');if(enais)enais.value=e.naissance||'';
  document.getElementById('emp-fonction').value=e.fonction||'Autre';
  document.getElementById('emp-soc').value=e.societe||'nemausus';
  const btn=document.querySelector('#emp-form-wrap .btn-primary');
  if(btn){btn.textContent='💾 Modifier';btn.onclick=()=>saveEditEmploye(id);}
  document.getElementById('emp-form-wrap').style.display='block';
  document.getElementById('emp-form-wrap').scrollIntoView({behavior:'smooth'});
}

function saveEditEmploye(id){
  const prenom=document.getElementById('emp-prenom').value.trim();
  const nom=document.getElementById('emp-nom').value.trim();
  if(!prenom||!nom){alert('Prénom et Nom sont obligatoires');return;}
  const ann=getAnnuaire();
  const idx=ann.findIndex(e=>e.id===id);if(idx===-1)return;
  ann[idx]={
    ...ann[idx],prenom,nom,
    email:document.getElementById('emp-email').value.trim(),
    emailPro:document.getElementById('emp-email-pro')?.value.trim()||'',
    tel:document.getElementById('emp-tel').value.trim(),
    naissance:document.getElementById('emp-naissance')?.value||'',
    fonction:document.getElementById('emp-fonction').value,
    societe:document.getElementById('emp-soc').value,
    sync_ts:Date.now()
  };
  saveAnnuaire(ann);
  const btn=document.querySelector('#emp-form-wrap .btn-primary');
  if(btn){btn.textContent='Enregistrer';btn.onclick=addEmploye;}
  ['emp-prenom','emp-nom','emp-email','emp-email-pro','emp-tel','emp-naissance'].forEach(i=>{const el=document.getElementById(i);if(el)el.value='';});
  document.getElementById('emp-form-wrap').style.display='none';
  const ok=document.getElementById('emp-ok');ok.textContent='✅ Modifié !';
  setTimeout(()=>ok.textContent='',1500);
  renderAnnuaire();logActivity(`Benjamin a modifié ${prenom} ${nom}`);
}

function renderAnnuaire(search){
  if(search===undefined)search=document.getElementById('ann-search')?.value||'';
  const list=document.getElementById('annuaire-list');if(!list)return;
  let emps=getAnnuaireActive();
  if(annFilter==='nemausus')emps=emps.filter(e=>e.societe==='nemausus'||e.societe==='les-deux');
  else if(annFilter==='lambert')emps=emps.filter(e=>e.societe==='lambert'||e.societe==='les-deux');
  else if(annFilter!=='tous')emps=emps.filter(e=>e.fonction===annFilter);
  if(search)emps=emps.filter(e=>`${e.prenom} ${e.nom} ${e.email||''} ${e.emailPro||''} ${e.fonction||''} ${e.tel||''}`.toLowerCase().includes(search.toLowerCase()));
  const all=getAnnuaireActive();
  const nem=all.filter(e=>e.societe==='nemausus'||e.societe==='les-deux').length;
  const lam=all.filter(e=>e.societe==='lambert'||e.societe==='les-deux').length;
  const sub=document.getElementById('ann-counter-sub');
  if(sub&&(typeof equipeUITab==='undefined'||equipeUITab==='annuaire'))sub.textContent=`${all.length} employé(s) — Nemausus: ${nem} · Lambert: ${lam}`;
  if(!emps.length){list.innerHTML=`<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">${search||annFilter!=='tous'?'Aucun résultat':'Aucun employé — cliquez sur + Ajouter'}</div>`;return;}
  const fc={'Technicien':'#60A5FA','Métreur':'#A78BFA','Commercial':'#E8943A','Assistante':'#22C55E','Comptable':'#FBBF24','Autre':'#888'};
  const today=new Date();
  const byFonction={};
  emps.forEach(e=>{const f=e.fonction||'Autre';if(!byFonction[f])byFonction[f]=[];byFonction[f].push(e);});
  list.innerHTML=Object.entries(byFonction).map(([fonction,groupe])=>`
    <div style="margin-bottom:16px">
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:${fc[fonction]||'#888'};padding:6px 0;display:flex;align-items:center;gap:6px">
        <span style="flex:1">${fonction}</span><span style="background:${fc[fonction]||'#888'}22;color:${fc[fonction]||'#888'};padding:2px 8px;border-radius:10px;font-size:11px">${groupe.length}</span>
      </div>
      ${groupe.map(e=>{
        // Anniversaire aujourd'hui ?
        let annivToday='';let annivInfo='';
        if(e.naissance){
          const d=new Date(e.naissance);
          if(d.getDate()===today.getDate()&&d.getMonth()===today.getMonth()){
            annivToday='🎂';
          }
          annivInfo=`<div style="font-size:10px;color:var(--t3)">🎂 ${new Date(e.naissance).toLocaleDateString('fr-FR',{day:'2-digit',month:'long'})}</div>`;
        }
        return`<div class="emp-card" style="${annivToday?'border-color:var(--g);background:var(--g2)':''}">
          <div class="emp-avatar">${e.prenom[0].toUpperCase()}${e.nom[0].toUpperCase()}${annivToday}</div>
          <div style="flex:1">
            <div class="emp-name">${esc(e.prenom)} ${esc(e.nom)} <span style="background:var(--s3);color:var(--t3);padding:1px 6px;border-radius:8px;font-size:9px;font-weight:600">${e.societe==='nemausus'?'Nemausus':e.societe==='lambert'?'Lambert':'Les deux'}</span></div>
            <div class="emp-detail">${e.tel?'📞 '+esc(e.tel):''}</div>
            ${e.email?`<div class="emp-email" onclick="navigator.clipboard.writeText('${esc(e.email)}');showDriveNotif('📋 Email copié')" title="Email perso — cliquer pour copier">✉️ ${esc(e.email)} <span style="font-size:9px;color:var(--t3)">(perso)</span></div>`:''}
            ${e.emailPro?`<div class="emp-email" style="color:var(--bl)" onclick="navigator.clipboard.writeText('${esc(e.emailPro)}');showDriveNotif('📋 Email pro copié')" title="Email pro — cliquer pour copier">✉️ ${esc(e.emailPro)} <span style="font-size:9px;color:var(--t3)">(pro)</span></div>`:''}
            ${annivInfo}
          </div>
          <button onclick="editEmploye(${e.id})" style="background:none;border:none;color:var(--a);cursor:pointer;font-size:16px;padding:4px" title="Modifier">✏️</button>
          <button onclick="deleteEmploye(${e.id})" style="background:none;border:none;color:var(--r);cursor:pointer;font-size:16px;padding:4px" title="Supprimer">🗑️</button>
        </div>`;
      }).join('')}
    </div>`).join('');
}

function fillAbsEmpList(opts={}){
  const selectedEmp=(opts.selectedEmp||'').trim();
  const selectedNotifs=Array.isArray(opts.selectedNotifs)?opts.selectedNotifs:null;
  const sel=document.getElementById('abs-emp-select');if(!sel)return;
  const mem=getMem();
  const ann=getAnnuaireActive();
  const annNoms=ann.map(e=>`${e.prenom} ${e.nom}`);
  const absNoms=(mem.absences||[]).filter(a=>!a._deleted).map(a=>String(a.employe||'').trim()).filter(Boolean);
  const users=getAllUsers().filter(u=>u.id!=='benjamin');
  const extraUsers=users
    .filter(u=>!annNoms.some(n=>normalizeId(n).includes(normalizeId(u.name))||normalizeId(n.split(' ')[0]||'').includes(normalizeId(u.name))))
    .map(u=>u.name);
  const all=[...new Set([...annNoms,...extraUsers,...absNoms,selectedEmp].filter(Boolean))].sort((a,b)=>a.localeCompare(b,'fr'));
  sel.innerHTML='<option value="">Sélectionner...</option>'+all.map(n=>`<option value="${esc(n)}">${esc(n)}</option>`).join('');
  if(selectedEmp)sel.value=selectedEmp;
  // Checkboxes notif dynamiques
  const notifList=document.getElementById('abs-notif-list');if(!notifList)return;
  const assistanteIds=new Set(getAssistanteUserIds());
  notifList.innerHTML=getAllUsers().filter(u=>!assistanteIds.has(u.id)).map(u=>`
    <label style="display:flex;align-items:center;gap:5px;font-size:12px;cursor:pointer">
      <input type="checkbox" class="abs-notif-cb" data-uid="${u.id}" ${(selectedNotifs?selectedNotifs.includes(u.id):u.id==='benjamin')?'checked':''} style="accent-color:var(--a)">
      ${esc(u.name)}${u.role==='admin'?' (admin)':''}
    </label>`).join('');
  const shareWrap=document.getElementById('abs-share-assistantes-wrap');
  const shareEl=document.getElementById('abs-share-assistantes');
  const hasAss=assistanteIds.size>0;
  if(shareWrap)shareWrap.style.display=hasAss?'block':'none';
  if(shareEl&&hasAss){
    if(selectedNotifs){
      shareEl.checked=assistanteIds.size===0||[...assistanteIds].some(id=>selectedNotifs.includes(id));
    } else {
      shareEl.checked=true;
    }
  }
}

// PLANNING ABSENCES - téléchargement CSV
function downloadPlanningAbsences(societe){
  if(currentUser?.role!=='admin')return;
  const mem=getMem();
  const absences=(mem.absences||[]).filter(a=>!a._deleted).sort((a,b)=>new Date(a.debut)-new Date(b.debut));
  if(!absences.length){alert('Aucune absence enregistrée');return;}
  const isNem=societe==='nemausus';
  const label=isNem?'Nemausus Fermetures':'Lambert SAS';
  const color=isNem?'linear-gradient(135deg,#E8943A,#C4711A)':'linear-gradient(135deg,#3B82F6,#1D4ED8)';
  const borderColor=isNem?'#E8943A':'#3B82F6';
  const ann=getAnnuaireActive();
  const users=getAllUsers().filter(u=>u.id!=='benjamin');
  const DAY_NAMES=['Dim','Lun','Mar','Mer','Jeu','Ven','Sam'];
  const MONTH_NAMES=['Janvier','Février','Mars','Avril','Mai','Juin','Juillet','Août','Septembre','Octobre','Novembre','Décembre'];
  const TYPE_COLORS={
    'Congé':{bg:'#BFDBFE',icon:'🏖️'},
    'Maladie':{bg:'#FED7AA',icon:'🤒'},
    'RTT':{bg:'#BBF7D0',icon:'🕘'},
    'Accident du travail':{bg:'#FECACA',icon:'🚑'},
    'Absence injustifiée':{bg:'#FDE68A',icon:'⚠️'},
    'Autre':{bg:'#DDD6FE',icon:'📅'}
  };

  function getEmpSociete(name){
    const a=ann.find(e=>`${e.prenom} ${e.nom}`===name);
    if(a)return a.societe;
    const u=users.find(x=>x.name===name);
    return u?.societe||'les-deux';
  }

  function expandDays(a){
    const days={};
    const s=new Date(a.debut);const e=new Date(a.fin);
    s.setHours(0,0,0,0);e.setHours(0,0,0,0);
    for(let d=new Date(s);d<=e;d.setDate(d.getDate()+1)){
      days[d.toISOString().split('T')[0]]=a.type;
    }
    return days;
  }

  // Construire la map absences par employé
  const absMap={};
  absences.forEach(a=>{
    if(!absMap[a.employe])absMap[a.employe]={};
    Object.assign(absMap[a.employe],expandDays(a));
  });

  // Mois concernés
  const monthsSet=new Set();
  absences.forEach(a=>{
    const s=new Date(a.debut);const e=new Date(a.fin);
    for(let d=new Date(s);d<=e;d.setMonth(d.getMonth()+1)){
      monthsSet.add(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`);
      d.setDate(1);
    }
    monthsSet.add(`${s.getFullYear()}-${String(s.getMonth()+1).padStart(2,'0')}`);
    monthsSet.add(`${e.getFullYear()}-${String(e.getMonth()+1).padStart(2,'0')}`);
  });
  const months=[...monthsSet].sort();

  function buildCalendar(societe){
    const compAbs=absences.filter(a=>{const s=getEmpSociete(a.employe);return s===societe||s==='les-deux';});
    if(!compAbs.length)return'<p style="color:#9CA3AF;padding:20px;text-align:center">Aucune absence enregistrée</p>';
    const emps=[...new Set(compAbs.map(a=>a.employe))].sort();
    return months.map(mk=>{
      const [yr,mo]=mk.split('-').map(Number);
      const daysInMonth=new Date(yr,mo,0).getDate();
      // Uniquement les employés avec une absence ce mois
      const empsThisMonth=emps.filter(emp=>{
        for(let d=1;d<=daysInMonth;d++){
          if(absMap[emp]?.[`${yr}-${String(mo).padStart(2,'0')}-${String(d).padStart(2,'0')}`])return true;
        }return false;
      });
      if(!empsThisMonth.length)return'';
      const dayHeaders=Array.from({length:daysInMonth},(_,i)=>{
        const d=i+1;const date=new Date(yr,mo-1,d);
        const isWE=date.getDay()===0||date.getDay()===6;
        return`<th class="day-header${isWE?' we':''}">${d}<br><span style="font-weight:400">${DAY_NAMES[date.getDay()]}</span></th>`;
      }).join('');
      const rows=empsThisMonth.map(emp=>{
        const cells=Array.from({length:daysInMonth},(_,i)=>{
          const d=i+1;const key=`${yr}-${String(mo).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
          const type=absMap[emp]?.[key];
          const date=new Date(yr,mo-1,d);
          const isWE=date.getDay()===0||date.getDay()===6;
          const tc=type?TYPE_COLORS[type]||TYPE_COLORS['Autre']:null;
          return`<td class="day-cell${isWE?' we':''}"${tc?` style="background:${tc.bg}"`:''}>${tc?tc.icon:''}</td>`;
        }).join('');
        return`<tr><td class="emp-cell">👤 ${emp}</td>${cells}</tr>`;
      }).join('');
      return`<div class="month-block">
        <div class="month-title" style="background:var(--mc,#E8943A)">📅 ${MONTH_NAMES[mo-1]} ${yr}</div>
        <table><thead><tr><th class="emp-cell" style="background:#F3F4F6;font-size:9px;color:#6B7280;font-weight:600">Employé</th>${dayHeaders}</tr></thead>
        <tbody>${rows}</tbody></table></div>`;
    }).filter(Boolean).join('');
  }

  const legend=`<div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:10px;padding:8px 12px;background:#F9FAFB;border-radius:6px;border:1px solid #E5E7EB">
    <span style="font-size:11px;font-weight:700;color:#6B7280">Légende :</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#BFDBFE;border-radius:3px"></span>🏖️ Congé</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#FED7AA;border-radius:3px"></span>🤒 Maladie</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#BBF7D0;border-radius:3px"></span>🕘 RTT</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#FECACA;border-radius:3px"></span>🚑 Accident du travail</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#DDD6FE;border-radius:3px"></span>📅 Autre</span>
  </div>`;

  const cal=buildCalendar(societe);
  const pageContent=cal||`<div style="padding:20px;color:#9CA3AF;text-align:center">Aucune absence enregistrée pour ${label}</div>`;

  const html=`<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8"><title>Planning ${label}</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#fff;color:#111827;padding:6mm;font-size:10px}
    .month-block{margin-bottom:14px}
    .month-title{padding:6px 10px;font-weight:800;font-size:12px;color:#fff;border-radius:5px 5px 0 0;background:${borderColor}}
    table{border-collapse:collapse;width:100%;table-layout:fixed}
    td,th{border:1px solid #D1D5DB;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .emp-cell{width:110px;min-width:110px;max-width:110px;padding:5px 8px;font-weight:700;font-size:10px;background:#FAFAFA;color:#111}
    .day-header{width:22px;min-width:22px;max-width:22px;text-align:center;padding:3px 1px;font-size:8px;font-weight:700;background:#F3F4F6}
    .day-cell{width:22px;min-width:22px;max-width:22px;text-align:center;padding:0;height:28px;vertical-align:middle;font-size:11px}
    .we{background:#F3F4F6!important;color:#9CA3AF}
    @media print{body{padding:4mm}.no-print{display:none!important}.month-block{page-break-inside:avoid}}
    @page{size:A4 landscape;margin:5mm}
  </style></head><body>
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;padding-bottom:8px;border-bottom:3px solid ${borderColor}">
    <div style="display:flex;align-items:center;gap:10px">
      <div style="width:34px;height:34px;border-radius:8px;background:${color};display:flex;align-items:center;justify-content:center;font-size:17px;font-weight:800;color:#fff">B</div>
      <div><div style="font-size:15px;font-weight:800">Planning des Absences</div><div style="font-size:12px;color:${borderColor};font-weight:700">${label}</div></div>
    </div>
    <div style="text-align:right">
      <div style="font-size:10px;color:#9CA3AF">Généré le ${new Date().toLocaleDateString('fr-FR',{day:'numeric',month:'long',year:'numeric'})}</div>
      <button onclick="window.print()" class="no-print" style="margin-top:4px;padding:5px 12px;background:${borderColor};color:#fff;border:none;border-radius:6px;font-size:11px;font-weight:600;cursor:pointer">🖨️ Imprimer / PDF</button>
    </div>
  </div>
  ${pageContent}
  <div style="display:flex;gap:14px;flex-wrap:wrap;margin-top:10px;padding:8px 12px;background:#F9FAFB;border-radius:6px;border:1px solid #E5E7EB">
    <span style="font-size:11px;font-weight:700;color:#6B7280">Légende :</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#BFDBFE;border-radius:3px"></span>🏖️ Congé</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#FED7AA;border-radius:3px"></span>🤒 Maladie</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#BBF7D0;border-radius:3px"></span>🕘 RTT</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#FECACA;border-radius:3px"></span>🚑 Accident du travail</span>
    <span style="display:flex;align-items:center;gap:5px;font-size:11px"><span style="display:inline-block;width:14px;height:14px;background:#DDD6FE;border-radius:3px"></span>📅 Autre</span>
  </div>
  </body></html>`;

  window.open(URL.createObjectURL(new Blob([html],{type:'text/html;charset=utf-8'})),'_blank');
  logActivity(`Benjamin a ouvert le planning des absences ${label}`);
}
// ══════════════════════════════════════════
// 🔊 NOTIFICATIONS SONORES
// ══════════════════════════════════════════
function playNotifSound(){
  try{
    const ctx=new(window.AudioContext||window.webkitAudioContext)();
    const osc=ctx.createOscillator();const gain=ctx.createGain();
    osc.connect(gain);gain.connect(ctx.destination);
    osc.frequency.value=880;osc.type='sine';
    gain.gain.setValueAtTime(0,ctx.currentTime);
    gain.gain.linearRampToValueAtTime(0.2,ctx.currentTime+0.05);
    gain.gain.exponentialRampToValueAtTime(0.001,ctx.currentTime+0.4);
    osc.start(ctx.currentTime);osc.stop(ctx.currentTime+0.4);
  }catch(e){}
}

// ══════════════════════════════════════════
// ⌨️ RACCOURCIS CLAVIER (Benjamin)
// ══════════════════════════════════════════
document.addEventListener('keydown',e=>{
  if(currentUser?.id!=='benjamin')return;
  if(e.target.tagName==='INPUT'||e.target.tagName==='TEXTAREA'||e.target.tagName==='SELECT')return;
  if(e.altKey){
    switch(e.key){
      case 'b':e.preventDefault();showPage('benai');break;
      case 's':e.preventDefault();showPage('sav');break;
      case 'm':e.preventDefault();showPage('messages');break;
      case 'n':e.preventDefault();showPage('notes');break;
      case 'a':e.preventDefault();showPage('annuaire');break;
      case 'd':e.preventDefault();showPage('admin');break;
    }
  }
});

// ══════════════════════════════════════════
// 📋 RÉSUMÉ HEBDOMADAIRE VENDREDI
// ══════════════════════════════════════════
async function generateWeeklySummary(){
  if(currentUser?.id!=='benjamin')return;
  const todayKey='benai_weekly_'+new Date().toDateString();
  if(appStorage.getItem(todayKey))return;
  const apiKey=getApiKey();if(!apiKey)return;
  const mem=getMem();
  const savs=mem.sav||[];
  const reglesSemaine=savs.filter(s=>{
    if(s.statut!=='regle'||!s.date_reglement)return false;
    const d=parseDateFR(s.date_reglement);
    return d&&(Date.now()-d)<7*24*60*60*1000;
  });
  const totalMessages=Object.values(mem.messages).reduce((t,m)=>t+m.length,0);
  const patterns=getTopPatterns();
  const ctx=`Semaine terminée. Résumé :
- SAV réglés cette semaine : ${reglesSemaine.length} (${reglesSemaine.map(s=>s.client).join(', ')||'aucun'})
- SAV encore ouverts : ${savs.filter(s=>s.statut!=='regle').length}
- Messages échangés total : ${totalMessages}
- Fournisseurs les plus sollicités : ${patterns.topFournisseurs.join(', ')||'aucun'}
- Total SAV traités depuis le début : ${patterns.savCount||0}`;
  const typing=addTyping();
  try{
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:250,messages:[{role:'user',content:`Tu es BenAI. Génère le résumé de fin de semaine pour Benjamin, 3 phrases max, positif et constructif, tutoie-le. Commence directement par les chiffres importants.\n\n${ctx}`}]})
    });
    const data=await res.json();
    typing.remove();
    if(data.content&&data.content[0]){
      addAIMsg('📊 **Résumé de la semaine**\n\n'+data.content[0].text);
      if(data.usage)trackTokens('benjamin',data.usage.input_tokens||0,data.usage.output_tokens||0);
      appStorage.setItem(todayKey,'1');
    }
  }catch(e){typing.remove();}
}

// ══════════════════════════════════════════
// 📵 MODE HORS-LIGNE
// ══════════════════════════════════════════
function initOfflineDetection(){
  const banner=document.getElementById('offline-banner');
  if(!banner)return;
  const update=()=>{
    banner.style.display=navigator.onLine?'none':'flex';
    if(!navigator.onLine){
      const sendBtn=document.getElementById('btn-send');
      if(sendBtn)sendBtn.title='Mode hors-ligne — IA indisponible';
    }
  };
  window.addEventListener('online',()=>{
    update();
    if(currentUser?.id&&SUPABASE_CONFIG.enabled){
      void flushSupabaseSyncNow();
      void refreshCoreDataFromCloudIfNeeded(true);
      void loadSharedApiKeyFromSupabase();
      void processPendingUserDeletes(true);
      void processPendingUserCreates(true);
      renderSupabaseRuntimeStatus();
    }
  });
  document.addEventListener('visibilitychange',()=>{
    if(document.hidden)return;
    if(currentUser?.id&&SUPABASE_CONFIG.enabled){
      void flushSupabaseSyncNow();
      void refreshCoreDataFromCloudIfNeeded(true);
      void loadSharedApiKeyFromSupabase();
      renderSupabaseRuntimeStatus();
    }
  });
  window.addEventListener('offline',update);
  update();
}

// FICHES DE PAIE
let paieResults=[];

async function processFichesPaie(input){
  const files=Array.from(input.files);if(!files.length)return;
  input.value='';
  const apiKey=getApiKey();if(!apiKey){alert('Clé API manquante');return;}
  paieResults=[];
  document.getElementById('paie-empty').style.display='none';
  document.getElementById('paie-progress').style.display='block';
  document.getElementById('paie-list').innerHTML='';
  for(let i=0;i<files.length;i++){
    const file=files[i];
    document.getElementById('paie-progress-bar').style.width=Math.round((i/files.length)*100)+'%';
    document.getElementById('paie-progress-txt').textContent=`Analyse ${i+1}/${files.length} : ${file.name}`;
    try{
      const ab=await file.arrayBuffer();
      const pdf=await pdfjsLib.getDocument({data:ab}).promise;
      const page=await pdf.getPage(1);
      const vp=page.getViewport({scale:1.5});
      const c=document.createElement('canvas');c.width=vp.width;c.height=vp.height;
      await page.render({canvasContext:c.getContext('2d'),viewport:vp}).promise;
      const img=c.toDataURL('image/jpeg',0.8).split(',')[1];
      const res=await fetch('https://api.anthropic.com/v1/messages',{
        method:'POST',
        headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
        body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:200,system:'Analyse cette fiche de paie et réponds UNIQUEMENT en JSON valide sans rien d\'autre : {"prenom":"","nom":"","mois":"","annee":""}',messages:[{role:'user',content:[{type:'image',source:{type:'base64',media_type:'image/jpeg',data:img}},{type:'text',text:'Extrait prénom, nom, mois et année de cette fiche de paie.'}]}]})
      });
      const data=await res.json();
      if(data.usage)trackTokens(currentUser.id,data.usage.input_tokens||0,data.usage.output_tokens||0);
      const parsed=JSON.parse(data.content[0].text.replace(/```json|```/g,'').trim());
      const ann=getAnnuaireActive();
      const match=ann.find(e=>normalizeId(e.nom)===normalizeId(parsed.nom||'')||normalizeId(`${e.prenom} ${e.nom}`)===normalizeId(`${parsed.prenom||''} ${parsed.nom||''}`));
      paieResults.push({file:file.name,prenom:parsed.prenom||'',nom:parsed.nom||'',mois:parsed.mois||'',annee:parsed.annee||'',email:match?.email||'',found:!!match});
    }catch(e){
      paieResults.push({file:file.name,prenom:'?',nom:'?',mois:'',annee:'',email:'',found:false,error:e.message});
    }
  }
  document.getElementById('paie-progress-bar').style.width='100%';
  setTimeout(()=>document.getElementById('paie-progress').style.display='none',500);
  renderPaieList();
  const autoReady=paieResults.filter(r=>r.email&&r.found&&!r.error);
  if(autoReady.length){
    const ok=confirm(`✅ ${autoReady.length} fiche(s) ont un email perso trouvé.\n\nOuvrir automatiquement les brouillons Outlook maintenant ?`);
    if(ok)openPaieBatchDrafts();
  }
}

function getPaieMailto(r){
  const periode=r.mois&&r.annee?`${r.mois} ${r.annee}`:r.mois||r.annee||'';
  const sujet=encodeURIComponent(`Fiche de paie${periode?' — '+periode:''}`);
  const corps=encodeURIComponent(`Bonjour ${r.prenom||''},\n\nVeuillez trouver ci-joint votre fiche de paie${periode?' du '+periode:''}.\n\nCordialement,\nBenjamin Muller\nNemausus Fermetures`);
  return r.email?`mailto:${r.email}?subject=${sujet}&body=${corps}`:'';
}
function openPaieDraft(idx){
  const r=paieResults[idx];if(!r||!r.email)return;
  const mailto=getPaieMailto(r);if(!mailto)return;
  window.open(mailto,'_blank');
  r.sentAt=Date.now();
  renderPaieList();
  logActivity(`Fiche de paie envoyée : ${(r.prenom||'')+' '+(r.nom||'')}`.trim());
}
function openPaieBatchDrafts(){
  const targets=paieResults.map((r,idx)=>({r,idx})).filter(x=>x.r.email&&!x.r.error);
  targets.forEach((x,i)=>setTimeout(()=>openPaieDraft(x.idx),i*350));
}

function renderPaieList(){
  const list=document.getElementById('paie-list');if(!list)return;
  if(!paieResults.length){document.getElementById('paie-empty').style.display='block';return;}
  document.getElementById('paie-empty').style.display='none';
  const readyCount=paieResults.filter(r=>r.email&&!r.error).length;
  list.innerHTML=`
    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:10px;background:var(--s2);border:1px solid var(--b1);border-radius:10px;padding:8px 10px">
      <div style="font-size:12px;color:var(--t2)">📬 ${readyCount}/${paieResults.length} fiches prêtes pour envoi</div>
      ${readyCount?`<button onclick="openPaieBatchDrafts()" style="padding:7px 10px;background:linear-gradient(135deg,var(--a),var(--a2));color:#fff;border:none;border-radius:8px;font-size:11px;font-weight:700;cursor:pointer;font-family:inherit">📧 Ouvrir tous les brouillons</button>`:''}
    </div>
    ${paieResults.map((r,i)=>{
    const nom=r.prenom&&r.nom&&r.nom!=='?'?`${r.prenom} ${r.nom}`:r.file;
    const periode=r.mois&&r.annee?`${r.mois} ${r.annee}`:r.mois||r.annee||'';
    return `<div class="paie-item">
      <div style="font-size:22px">${r.found?'👤':'❓'}</div>
      <div style="flex:1">
        <div style="font-size:13px;font-weight:600">${esc(nom)}${periode?' <span style="color:var(--t2);font-weight:400;font-size:11px">— '+esc(periode)+'</span>':''}</div>
        ${r.email?`<div style="font-size:11px;color:var(--bl);margin-top:2px">✉️ ${esc(r.email)}</div>`:`<div style="font-size:11px;color:var(--r);margin-top:2px">⚠️ Email non trouvé — <button onclick="askEmailPaie(${i})" style="background:none;border:none;color:var(--a);cursor:pointer;font-size:11px;font-family:inherit;text-decoration:underline">Saisir manuellement</button></div>`}
        <div style="font-size:11px;color:var(--t3);margin-top:2px">📄 ${esc(r.file)}</div>
        ${r.sentAt?`<div style="font-size:10px;color:var(--g);margin-top:2px">✅ Brouillon ouvert</div>`:''}
      </div>
      ${r.email?`<button onclick="openPaieDraft(${i})" style="padding:8px 14px;background:linear-gradient(135deg,var(--a),var(--a2));color:#fff;border:none;border-radius:8px;font-size:12px;font-weight:700;white-space:nowrap;display:inline-block;cursor:pointer;font-family:inherit">📧 Ouvrir Outlook</button>`:`<button disabled style="padding:8px 14px;background:var(--s3);color:var(--t3);border:1px solid var(--b1);border-radius:8px;font-size:12px;cursor:not-allowed">📧 Email manquant</button>`}
    </div>`;
  }).join('')}`;
}

function askEmailPaie(idx){
  const email=prompt(`Email pour ${paieResults[idx].prenom} ${paieResults[idx].nom} :`);
  if(!email||!email.includes('@'))return;
  paieResults[idx].email=email;renderPaieList();
}

// ══════════════════════════════════════════
// USAGE + SUGGESTIONS IA (coût / latence visibles — désactivé par défaut)
// ══════════════════════════════════════════
const USAGE_PAGES_STORAGE_KEY='benai_usage_pages_v1';
const HABIT_AI_ENABLED_KEY='benai_habit_suggestions_enabled';
const HABIT_AI_LAST_METRICS_KEY='benai_habit_ai_last_metrics';

function recordUsagePageVisit(page){
  if(!page||!currentUser)return;
  try{
    const raw=appStorage.getItem(USAGE_PAGES_STORAGE_KEY);
    let o={};
    try{o=raw?JSON.parse(raw):{};}catch{o={};}
    if(!o||typeof o!=='object')o={};
    if(!o.pages||typeof o.pages!=='object')o.pages={};
    o.pages[page]=Number(o.pages[page]||0)+1;
    o.updated=Date.now();
    appStorage.setItem(USAGE_PAGES_STORAGE_KEY,JSON.stringify(o));
  }catch{}
}

function buildUsageHabitContextText(){
  let pages={};
  try{
    const raw=appStorage.getItem(USAGE_PAGES_STORAGE_KEY);
    const o=raw?JSON.parse(raw):{};
    pages=(o&&typeof o.pages==='object')?o.pages:{};
  }catch{}
  const top=Object.entries(pages).sort((a,b)=>b[1]-a[1]).slice(0,14);
  const mem=getMem();
  const savOpen=(mem.sav||[]).filter(s=>!s._deleted&&s.statut!=='regle').length;
  const lines=[
    `Utilisateur: ${currentUser?.name||'?'} (id: ${currentUser?.id||'?'}, rôle: ${currentUser?.role||'?'})`,
    `Pages BenAI les plus ouvertes (compteurs cumulés sur cet appareil / ce profil stockage): ${top.length?top.map(([p,n])=>`${p}:${n}`).join(', '):'aucune donnée — naviguez dans l’app pour alimenter.'}`,
    `SAV ouverts: ${savOpen}`
  ];
  try{
    const leads=getCompanyScopedLeads(getLeads());
    const actifs=leads.filter(l=>!l.archive).length;
    lines.push(`CRM — leads actifs (périmètre société): ${actifs}`);
  }catch{}
  lines.push(`Version BenAI: ${typeof BENAI_VERSION!=='undefined'?BENAI_VERSION:'?'}`);
  return lines.join('\n');
}

function saveHabitAiSettings(){
  const cb=document.getElementById('habit-ai-enabled');
  if(!cb)return;
  appStorage.setItem(HABIT_AI_ENABLED_KEY,cb.checked?'1':'0');
  if(!cb.checked)showDriveNotif('Appels IA usage désactivés');
}

function hydrateHabitAiPanel(){
  const cb=document.getElementById('habit-ai-enabled');
  const met=document.getElementById('habit-ai-metrics');
  if(!cb)return;
  cb.checked=appStorage.getItem(HABIT_AI_ENABLED_KEY)==='1';
  if(!met)return;
  try{
    const m=JSON.parse(appStorage.getItem(HABIT_AI_LAST_METRICS_KEY)||'null');
    if(m&&m.at){
      const d=new Date(m.at).toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
      met.textContent=`Dernier appel (${d}) : ${m.latencyMs||'?'} ms · ${m.input||0} + ${m.output||0} tokens · ~${m.costEur!=null?String(m.costEur):'?'} € (estim.)`;
    }else met.textContent='Aucun appel « usage » encore effectué.';
  }catch{
    met.textContent='';
  }
}

function syncEvolutionHabitAiPanel(){
  const wrap=document.getElementById('habit-ai-wrap');
  if(!wrap)return;
  wrap.style.display=currentUser?.role==='admin'?'block':'none';
}

async function runHabitImprovementSuggestions(){
  if(currentUser?.role!=='admin'){
    showDriveNotif('Analyse IA réservée à l’administration.');
    return;
  }
  const btn=document.getElementById('habit-ai-run-btn');
  const out=document.getElementById('habit-ai-output');
  const met=document.getElementById('habit-ai-metrics');
  if(appStorage.getItem(HABIT_AI_ENABLED_KEY)!=='1'){
    showDriveNotif('Cochez d’abord « Autoriser l’appel IA » pour lancer une analyse (coût tokens).');
    return;
  }
  if(!currentUser){showDriveNotif('Connectez-vous.');return;}
  const snap=buildUsageHabitContextText();
  const userContent=`Voici un résumé factuel d’utilisation de BenAI. Propose 5 à 8 améliorations concrètes du produit ou des flux (pas de banalités). Réponds en français, titres ## courts, listes à puces, ton professionnel.\n\n---\n${snap}`;
  const systemAdd=`Tu es BenAI côté évolution produit. Tu proposes — l’utilisateur décide. Ne promets pas de délais. Aucune action automatique.`;
  if(btn){btn.disabled=true;btn.textContent='Analyse en cours…';}
  const t0=(typeof performance!=='undefined'&&performance.now)?performance.now():Date.now();
  try{
    const data=await requestAnthropicMessages({
      model:'claude-sonnet-4-20250514',
      max_tokens:1200,
      system:systemAdd+BENAI_CTX,
      messages:[{role:'user',content:userContent}]
    });
    const latency=Math.round(((typeof performance!=='undefined'&&performance.now)?performance.now():Date.now())-t0);
    const usage=data.usage||{};
    const inp=Number(usage.input_tokens||0);
    const out=Number(usage.output_tokens||0);
    trackTokens(currentUser.id,inp,out);
    const costStr=estimateCost(inp,out);
    const costNum=Number(costStr);
    appStorage.setItem(HABIT_AI_LAST_METRICS_KEY,JSON.stringify({
      at:Date.now(),
      latencyMs:latency,
      input:inp,
      output:out,
      costEur:Number.isFinite(costNum)?costNum:costStr
    }));
    if(met)met.textContent=`Dernier appel : ${latency} ms · ${inp} + ${out} tokens · ~${costStr} € (estim. Sonnet — indicatif)`;
    if(out){
      out.style.display='block';
      out.textContent=(data.content&&data.content[0]&&data.content[0].text)?String(data.content[0].text):'(Réponse vide)';
    }
    try{logActivity(`${currentUser.name} a lancé l’analyse « suggestions usage » IA`);}catch{}
  }catch(e){
    if(met)met.textContent='Erreur : '+(e?.message||e||'?');
    showDriveNotif('❌ Analyse IA impossible — vérifiez la clé API.');
  }finally{
    if(btn){btn.disabled=false;btn.textContent='Analyser mon usage et proposer des améliorations';}
  }
}

// ══════════════════════════════════════════
// EVOLUTION
// ══════════════════════════════════════════
const EVOLUTIONS=[
  {id:1,tag:'Productivité',tagColor:'var(--a)',title:'Export SAV mensuel automatique',desc:'Chaque 1er du mois, BenAI prépare un export CSV du SAV du mois écoulé. Vous le recevez dans un rappel.'},
  {id:2,tag:'Communication',tagColor:'var(--bl)',title:'Modèles d\'emails SAV',desc:'Des modèles pré-rédigés pour les emails fournisseurs courants : pièce manquante, délai de livraison, réclamation qualité.'},
  {id:3,tag:'Suivi',tagColor:'var(--g)',title:'Tableau de bord mensuel',desc:'Un récapitulatif mensuel des SAV par société avec les temps de résolution moyens.'},
  {id:4,tag:'Sécurité',tagColor:'var(--r)',title:'Sauvegarde automatique hebdomadaire',desc:'Rappel chaque lundi pour télécharger la sauvegarde BenAI. Simple et fiable.'},
  {id:5,tag:'Productivité',tagColor:'var(--a)',title:'Signatures email personnalisées',desc:'BenAI mémorise vos signatures Nemausus et Lambert pour les intégrer automatiquement dans les emails rédigés.'},
];

function getEvolVotes(){try{return JSON.parse(appStorage.getItem('benai_evol_votes'))||{};}catch{return{};}}
function saveEvolVotes(v){appStorage.setItem('benai_evol_votes',JSON.stringify(v));}

function voteEvol(evolId,vote){
  if(!currentUser||currentUser.id==='benjamin')return;
  const votes=getEvolVotes();
  if(!votes[evolId])votes[evolId]={};
  votes[evolId][currentUser.id]=vote;
  saveEvolVotes(votes);
  renderEvolution();
}

function renderEvolution(){
  hydrateHabitAiPanel();
  const dismissed=JSON.parse(appStorage.getItem('benai_evol_dismissed')||'[]');
  const votes=getEvolVotes();
  const isBenjamin=currentUser?.id==='benjamin';
  const list=document.getElementById('evolution-list');
  const active=EVOLUTIONS.filter(e=>!dismissed.includes(e.id));
  if(!active.length){list.innerHTML='<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">Toutes les suggestions ont été traitées 🎉</div>';return;}
  list.innerHTML=active.map(e=>{
    const eVotes=votes[e.id]||{};
    const yes=Object.values(eVotes).filter(v=>v==='yes').length;
    const no=Object.values(eVotes).filter(v=>v==='no').length;
    const myVote=currentUser?eVotes[currentUser.id]:null;
    const voteSummary=isBenjamin&&(yes+no>0)?`<div style="font-size:11px;color:var(--t2);margin-top:8px;padding:6px 10px;background:var(--s3);border-radius:8px">👍 ${yes} intéressé(s) · 👎 ${no} pas intéressé(s)</div>`:'';
    const yStyle=myVote==='yes'?'border:1px solid var(--g);background:var(--g2);color:var(--g)':'border:1px solid var(--b1);background:var(--s3);color:var(--t2)';
    const nStyle=myVote==='no'?'border:1px solid var(--r);background:var(--r2);color:var(--r)':'border:1px solid var(--b1);background:var(--s3);color:var(--t2)';
    const actions=isBenjamin
      ?`<div class="evol-actions"><button class="evol-btn-ok" onclick="validateEvol(${e.id})">✅ À développer</button><button class="evol-btn-later" onclick="dismissEvol(${e.id})">Plus tard</button></div>`
      :`<div class="evol-actions"><button onclick="voteEvol(${e.id},'yes')" style="padding:6px 14px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;${yStyle}">👍 Intéressé</button><button onclick="voteEvol(${e.id},'no')" style="padding:6px 14px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit;${nStyle}">👎 Pas intéressé</button></div>`;
    return `<div class="evol-card">
      <span class="evol-tag" style="background:${e.tagColor}22;color:${e.tagColor}">${e.tag}</span>
      <div class="evol-title">${e.title}</div>
      <div class="evol-desc">${e.desc}</div>
      ${voteSummary}
      ${actions}
    </div>`;
  }).join('');
}

function validateEvol(id){
  const e=EVOLUTIONS.find(x=>x.id===id);
  if(e)alert(`✅ Noté ! "${e.title}" sera développé dans la prochaine mise à jour BenAI.`);
  dismissEvol(id);
}
function dismissEvol(id){
  const dismissed=JSON.parse(appStorage.getItem('benai_evol_dismissed')||'[]');
  dismissed.push(id);appStorage.setItem('benai_evol_dismissed',JSON.stringify(dismissed));renderEvolution();
}

function getRoleGuideAckKey(role){
  return `benai_role_guide_ack_${GUIDE_REQUIRED_VERSION}_${role||'assistante'}`;
}
function hasAcknowledgedRoleGuide(role){
  if(!currentUser||currentUser.id==='benjamin')return true;
  return appStorage.getItem(getRoleGuideAckKey(role||currentUser.role))==='1';
}
function shouldForceRoleGuide(){
  if(!currentUser||currentUser.id==='benjamin')return false;
  return !hasAcknowledgedRoleGuide(currentUser.role);
}
function acknowledgeRoleGuide(){
  if(!currentUser)return;
  appStorage.setItem(getRoleGuideAckKey(currentUser.role),'1');
  renderGuidePage();
  showDriveNotif('✅ Guide validé');
}

function renderGuidePage(){
  const box=document.getElementById('guide-content');if(!box)return;
  const role=currentUser?.role||'assistante';
  const roleTitle=ROLE_LABELS[role]||role;
  const canInstallMobile=canUseBenAIMobileApp(role);
  const mobileAppOpenUrl='https://expo.dev/@claudemartin/vocal-app?serviceContext=expo&host=expo.dev';
  const qrRemote='https://api.qrserver.com/v1/create-qr-code/?size=220x220&data='+encodeURIComponent(mobileAppOpenUrl);
  let qrImgSrc=qrRemote;
  try{
    qrImgSrc=new URL('benai-guide-qr-mobile.png',document.baseURI||document.URL||window.location.href).href;
  }catch{}
  const qrFallbackEnc=encodeURIComponent(qrRemote);
  const roleGuide={
    admin:[
      'Tu peux consulter le dashboard et traiter les alertes qui te semblent les plus utiles en premier.',
      'Le module SAV permet de suivre les rappels, d’archiver les dossiers clos et d’ajuster les notifications si besoin.',
      'Tu peux attribuer ou réattribuer les leads CRM pour équilibrer la charge.',
      'Tu peux suivre les tickets depuis l’onglet administration et jeter un œil aux connexions.'
    ],
    directeur_co:[
      'Périmètre : l’entreprise et les zones CRM affichées suivent ton compte ; filtres, synthèses dashboard et objectifs restent cohérents.',
      'Menus visibles : Leads CRM, Messages, Absences, Guide — signalement via « Signaler » (pas de liste des tickets).',
      'Leads CRM — « À attribuer » (sans commercial), « Tous les leads », puis « Dashboard ».',
      '« À attribuer » : dossiers en attente d’attribution ; tu peux attribuer depuis la carte ou la fiche.',
      '« Tous les leads » : filtres, recherche, pastilles ; filtre société seulement si ton accès couvre les deux entités.',
      'Fiche lead : tu peux ouvrir l’historique après attribution — timeline avec date et auteur.',
      'Pastille « non ouvert » : indicateur de suivi, pas une sanction.',
      'Dashboard : KPI, secteurs, ventes, objectifs, exports.',
      'Exports : périmètre fichiers = leads et secteurs visibles pour toi.',
      'Notifications : badge Leads surtout sur non attribués et alertes.',
      'Messages : échanges rapides. Absences : planning si l’équipe le renseigne. Signalement : menu « Signaler ».'
    ],
    directeur_general:[
      'Même logique de périmètre et d’onglets CRM que le dir. commercial ; mêmes usages (attribution, statuts, timeline, exports).',
      'Les rappels automatiques ciblent surtout le dir. commercial et les commerciaux ; le CRM reste disponible pour lecture ou action à la demande.',
      'Messages pour les arbitrages ; « Signaler » pour les incidents techniques répétés.'
    ],
    commercial:[
      'Le CRM regroupe tes dossiers en charge : « Mes leads » pour la liste ou le kanban, « Mes ventes » pour les signatures et le suivi d’objectifs.',
      'Filtres, recherche, pastilles et bascule liste / kanban permettent de structurer la journée.',
      'Bouton + Nouveau lead : tu peux créer un dossier ; la source ACTIF t’attribue automatiquement le lead ; sinon l’attribution est gérée côté organisation.',
      'Sur la fiche, Suivi et Commentaire portent l’essentiel pour le terrain ; la direction voit la timeline complète.',
      'Après un contact : tu peux mettre à jour le sous-statut, la date de rappel si utile, et le suivi.',
      'Boutons Appeler et Agenda pour gagner du temps ; « RDV fait » après une visite.',
      'Devis : tu peux renseigner montant HT, date d’envoi et relance ; une alerte peut apparaître si le dossier stagne.',
      'Vendu : le prix vendu HT exact est demandé pour enregistrer. Perdu : un motif détaillé est demandé pour clôturer.',
      'Hors zone : tu peux remplir la justification « hors secteur » pour enregistrer.',
      'Badge et notifications : tu peux réagir ou ajuster la date dans la fiche.',
      'Messages pour les relances ; fiche lead pour ce qui engage le dossier.',
      'Absences : tu peux déclarer les tiennes pour que l’équipe puisse anticiper.',
      'En cas de blocage : menu « Signaler » — décris la page et ce que tu faisais.'
    ],
    assistante:[
      'Menus BenAI : BenAI (IA), Notes, Messages, SAV, Leads CRM, Évolutions, Guide — et « Signaler » en cas de blocage.',
      'Leads CRM : « Mes leads » pour retrouver et compléter tes saisies ; la recherche texte aide à retrouver un client.',
      'Création : + Nouveau lead — champs demandés à l’enregistrement ; le secteur suit le code postal ; tu peux compléter ville et commentaire.',
      'Tu enregistres le contact ; la direction peut être prévenue si un dossier similaire existe.',
      'Attribution et RDV : le dir. commercial pilote une fois le dossier pris en charge.',
      'Sur tes fiches : tu peux modifier les infos de base et le texte ; le tunnel commercial est en général laissé au terrain une fois attribué.',
      'Un commentaire clair aide le terrain ; cite la source réelle.',
      'SAV : menu dédié avec rappel (souvent 5 jours).',
      'Messages : coordination ; cite le client ou l’ID lead.',
      'Notes : perso ; info utile au magasin → fiche ou message.',
      'Évolutions : nouveautés. Signalement : menu « Signaler » avec le contexte.',
      'Pistes de journée : appel → fiche complète → message si relais → SAV si installation.'
    ],
    metreur:[
      'Tu peux utiliser la messagerie interne pour transmettre les infos chantier.',
      'Tu peux ajouter des notes courtes et précises.',
      'Tu peux signaler un problème depuis le menu « Signaler » en cas de blocage.'
    ]
  };
  const common=[
    'BenAI suggère toujours, vous décidez toujours.',
    ...(canInstallMobile?['Sur mobile, installe BenAI via « 📲 Installer » pour un accès direct.']:[]),
    'En cas d’anomalie, tu peux utiliser « Signaler » dans la barre latérale.'
  ];
  const actions=roleGuide[role]||roleGuide.assistante;
  const mustAck=shouldForceRoleGuide();
  box.innerHTML=`
    ${mustAck?`<div class="sav-form" style="margin-bottom:10px;border-color:rgba(232,148,58,.35);background:var(--a3)">
      <div style="font-size:13px;font-weight:700;color:var(--a);margin-bottom:6px">📘 Guide mis à jour (${GUIDE_REQUIRED_VERSION})</div>
      <div style="font-size:12px;color:var(--t2);line-height:1.6;margin-bottom:8px">Ce résumé écrit couvre l’essentiel pour ton rôle. Quand tu l’as parcouru, tu peux valider ci-dessous pour retrouver l’accès complet.</div>
      <button onclick="acknowledgeRoleGuide()" style="padding:8px 12px;background:linear-gradient(135deg,var(--a),var(--a2));color:#fff;border:none;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer;font-family:inherit">✅ J’ai lu et je valide</button>
    </div>`:''}
    ${canInstallMobile?`<div class="sav-form" style="margin-bottom:10px;border-color:rgba(59,130,246,.35);background:var(--bl2)">
      <div style="font-size:14px;font-weight:700;margin-bottom:6px">📲 BenAI sur téléphone</div>
      <div style="font-size:12px;color:var(--t2);line-height:1.65">
        Tu peux installer BenAI comme une appli mobile pour un accès direct.
        <br>1) Tu peux ouvrir BenAI sur ton téléphone.
        <br>2) Tu peux appuyer sur <strong>📲 Installer</strong> (en haut à droite).
        <br>3) iPhone : Safari → Partager → Ajouter à l’écran d’accueil.
        <br>4) Android : tu peux confirmer l’installation proposée.
      </div>
      <div style="margin-top:14px;padding-top:12px;border-top:1px solid var(--b1);text-align:center">
        <div style="font-size:12px;font-weight:700;color:var(--t2);margin-bottom:6px">📱 QR code</div>
        <div style="font-size:11px;color:var(--t3);line-height:1.5;margin-bottom:10px">Scanne ce code avec l’appareil photo pour ouvrir BenAI sur ton téléphone.</div>
        <img id="benai-guide-mobile-qr" src="${qrImgSrc}" data-qr-fallback="${qrFallbackEnc}" width="220" height="220" alt="QR code pour ouvrir BenAI sur le téléphone" style="width:220px;max-width:90%;height:auto;border-radius:14px;border:1px solid var(--b1);background:#fff;padding:10px;box-sizing:border-box;display:inline-block" loading="lazy" onerror="try{var u=decodeURIComponent(this.getAttribute('data-qr-fallback')||'');if(u&amp;&amp;this.src!==u){this.src=u;this.removeAttribute('data-qr-fallback');}}catch(e){}">
      </div>
    </div>`:''}
    <div class="sav-form" style="margin-bottom:10px;border-color:rgba(96,165,250,.35);background:var(--bl2)">
      <div style="font-size:14px;font-weight:700;margin-bottom:6px">🎓 Tutoriel interactif</div>
      <div style="font-size:12px;color:var(--t2);line-height:1.6;margin-bottom:8px">Tu peux le relancer à tout moment pour faire une démonstration ou former un collaborateur.</div>
      <button onclick="startTuto()" style="padding:8px 12px;background:linear-gradient(135deg,var(--a),var(--a2));color:#fff;border:none;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer;font-family:inherit">▶️ Revoir le tutoriel</button>
    </div>
    <div class="sav-form" style="margin-bottom:10px">
      <div style="font-size:14px;font-weight:700;margin-bottom:8px">👤 Guide ${esc(roleTitle)}</div>
      ${actions.map((a,i)=>`<div style="font-size:13px;color:var(--t2);line-height:1.7;margin-bottom:6px">${i+1}. ${esc(a)}</div>`).join('')}
    </div>
    <div class="sav-form">
      <div style="font-size:14px;font-weight:700;margin-bottom:8px">✅ Bonnes pratiques</div>
      ${common.map(t=>`<div style="font-size:13px;color:var(--t2);line-height:1.7;margin-bottom:6px">• ${esc(t)}</div>`).join('')}
    </div>`;
}

// ══════════════════════════════════════════
// ADMIN DASHBOARD
// ══════════════════════════════════════════
function refreshAdmin(){
  refreshSAVBadge();
  const mem=getMem();
  const tot=Object.values(mem.messages).reduce((s,a)=>s+a.length,0);
  const el=document.getElementById('admin-msgs');if(el)el.textContent=tot;
  const notesEl=document.getElementById('admin-notes');if(notesEl)notesEl.textContent=(mem.notes||[]).filter(n=>!n._deleted).length;
  renderTokenStats();renderSAVStatsAdmin();
  // Activité récente
  const actList=document.getElementById('activity-list');
  if(actList&&mem.activity?.length){
    actList.innerHTML=mem.activity.slice(-10).reverse().map(a=>`<div class="act-item"><div class="act-av" style="background:${getUserColor(a.user)}">${getUserInitial(a.user)}</div><div class="act-txt">${esc(a.txt)}</div><div class="act-time">${a.time}</div></div>`).join('');
  }
  // Historique connexions
  renderConnexionsHistory();
  renderMobileUsageSummary();
  // Journal suppressions
  renderDeletionsList();
}

function renderDeletionsList(){
  const el=document.getElementById('deletions-list');if(!el)return;
  const logs=getDeletions().slice(0,30);
  if(!logs.length){el.innerHTML='<div style="color:var(--t3);font-size:12px">Aucune suppression enregistrée</div>';return;}
  el.innerHTML=logs.map(l=>`
    <div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--b1)">
      <span style="font-size:14px">🗑️</span>
      <div style="flex:1">
        <div style="font-size:12px;font-weight:500">${esc(l.nom)}</div>
        <div style="font-size:10px;color:var(--t3)">${esc(l.type)} · Par ${esc(l.user)}</div>
      </div>
      <div style="font-size:10px;color:var(--t3)">${l.date}</div>
    </div>`).join('');
}

function renderTokenStats(){
  const el=document.getElementById('token-stats');if(!el)return;
  const mem=getMem();const tokens=mem.tokens||{};
  const users=[...new Set(['benjamin',...getExtraUsers().map(u=>u.id)])];
  let html='';
  users.forEach(uid=>{
    const u=USERS[uid]||getExtraUserById(uid);if(!u)return;
    const t=tokens[uid]||{input:0,output:0};
    const total=(t.input||0)+(t.output||0);
    const cost=estimateCost(t.input||0,t.output||0);
    html+=`<div class="token-row"><div class="user-av" style="background:${u.color};width:26px;height:26px;border-radius:7px;font-size:11px;font-weight:700;color:#fff;display:flex;align-items:center;justify-content:center">${u.initial}</div><div class="token-name">${u.name}</div><div class="token-val">${total.toLocaleString()} tokens</div><div class="token-cost">~${cost}€</div></div>`;
  });
  el.innerHTML=html||'<div style="color:var(--t3);font-size:12px">Aucune utilisation</div>';
}

function renderSAVStatsAdmin(){
  const el=document.getElementById('sav-stats-admin');if(!el)return;
  const mem=getMem();const savs=mem.sav||[];
  const total=savs.length,regle=savs.filter(s=>s.statut==='regle').length,en_cours=savs.filter(s=>s.statut==='en_cours').length,nouveau=savs.filter(s=>s.statut==='nouveau').length;
  const nem=savs.filter(s=>s.societe==='nemausus').length,lam=savs.filter(s=>s.societe==='lambert').length;
  const urgent=savs.filter(s=>s.urgent).length;
  el.innerHTML=`
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
      <div style="background:var(--s3);border-radius:8px;padding:10px;text-align:center"><div style="font-size:10px;color:var(--t3);margin-bottom:4px">Total SAV</div><div style="font-size:20px;font-weight:700">${total}</div></div>
      <div style="background:var(--s3);border-radius:8px;padding:10px;text-align:center"><div style="font-size:10px;color:var(--t3);margin-bottom:4px">🚨 Urgents</div><div style="font-size:20px;font-weight:700;color:var(--r)">${urgent}</div></div>
    </div>
    <div class="stat-bar-wrap"><div class="stat-bar-label"><span>🔴 Nouveaux</span><span>${nouveau}</span></div><div class="stat-bar"><div class="stat-bar-fill" style="width:${total?nouveau/total*100:0}%;background:var(--r)"></div></div></div>
    <div class="stat-bar-wrap"><div class="stat-bar-label"><span>🟡 En cours</span><span>${en_cours}</span></div><div class="stat-bar"><div class="stat-bar-fill" style="width:${total?en_cours/total*100:0}%;background:var(--y)"></div></div></div>
    <div class="stat-bar-wrap"><div class="stat-bar-label"><span>🟢 Réglés</span><span>${regle}</span></div><div class="stat-bar"><div class="stat-bar-fill" style="width:${total?regle/total*100:0}%;background:var(--g)"></div></div></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:8px">
      <div style="background:rgba(232,148,58,.1);border-radius:8px;padding:8px;text-align:center;font-size:12px"><span style="color:var(--nem);font-weight:600">Nemausus</span><br><span style="font-size:18px;font-weight:700">${nem}</span></div>
      <div style="background:var(--bl2);border-radius:8px;padding:8px;text-align:center;font-size:12px"><span style="color:var(--lam);font-weight:600">Lambert</span><br><span style="font-size:18px;font-weight:700">${lam}</span></div>
    </div>`;
}

// ══════════════════════════════════════════
// GESTION UTILISATEURS
// ══════════════════════════════════════════
const COLORS=['linear-gradient(135deg,#22C55E,#15803D)','linear-gradient(135deg,#60A5FA,#1D4ED8)','linear-gradient(135deg,#F472B6,#DB2777)','linear-gradient(135deg,#A78BFA,#7C3AED)','linear-gradient(135deg,#34D399,#059669)'];

function canManageBenaiUsersAdmin(){
  return currentUser?.role==='admin';
}
/** Attribuer / modifier le pseudo de connexion (app_uid) : Benjamin uniquement. */
function canAssignBenaiLoginPseudo(){
  return currentUser?.role==='admin'&&currentUser?.id==='benjamin';
}
/** Même règle que create-user (app_uid stocké en base). */
function sanitizeAppUidForUid(seed){
  const cleaned=String(seed||'').trim().toLowerCase().replace(/[^a-z0-9_]+/g,'_').replace(/^_+|_+$/g,'');
  return cleaned||'user';
}
function proposedLoginUidFromPseudoField(rawPseudo,fallbackUid){
  const t=(rawPseudo||'').trim();
  if(!t)return sanitizeAppUidForUid(fallbackUid);
  return sanitizeAppUidForUid(normalizeId(t));
}

function migrateMemConversationsForUserRename(oldUid,newUid){
  const mem=getMem();
  if(!mem.messages||!oldUid||!newUid||normalizeId(oldUid)===normalizeId(newUid))return;
  const crowd=new Set(['benjamin','benai']);
  getAllUsers().forEach(u=>{if(u&&u.id)crowd.add(u.id);});
  crowd.forEach(ou=>{
    if(!ou||normalizeId(ou)===normalizeId(oldUid))return;
    const ocid=makeConvId(oldUid,ou);
    const ncid=makeConvId(newUid,ou);
    if(ocid===ncid)return;
    const chunk=mem.messages[ocid];
    if(Array.isArray(chunk)&&chunk.length){
      mem.messages[ncid]=mem.messages[ncid]||[];
      mem.messages[ncid].push(...chunk);
      delete mem.messages[ocid];
    }else if(mem.messages[ocid])delete mem.messages[ocid];
  });
  saveMem(mem);
}

function migrateCommercialIdInLeads(oldId,newId){
  if(normalizeId(oldId)===normalizeId(newId))return;
  const leads=getLeads();
  let ch=false;
  const next=leads.map(l=>{
    if(!l)return l;
    if(normalizeId(l.commercial)===normalizeId(oldId)){
      ch=true;
      return{...l,commercial:newId};
    }
    return l;
  });
  if(ch)saveLeads(next,true);
}

function migrateConnexionLogsUserId(oldUid,newUid){
  try{
    const logs=JSON.parse(appStorage.getItem('benai_connexions')||'[]');
    let ch=false;
    const out=logs.map(l=>{
      if(l&&l.uid===oldUid){ch=true;return{...l,uid:newUid};}
      return l;
    });
    if(ch)appStorage.setItem('benai_connexions',JSON.stringify(out));
  }catch{}
}

function migrateBenaiNameOverrideKey(oldUid,newUid){
  try{
    const o=JSON.parse(appStorage.getItem('benai_name_overrides')||'{}');
    if(!Object.prototype.hasOwnProperty.call(o,oldUid))return;
    const n={...o};
    if(n[oldUid]!==undefined){n[newUid]=n[oldUid];delete n[oldUid];}
    appStorage.setItem('benai_name_overrides',JSON.stringify(n));
  }catch{}
}

function renameExactAppStoragePair(oldUid,newUid){
  const pairs=[
    ['benai_chat_'+oldUid,'benai_chat_'+newUid],
    ['benai_read_'+oldUid,'benai_read_'+newUid],
    ['benai_notifs_'+oldUid,'benai_notifs_'+newUid],
    ['benai_tuto_done_'+oldUid,'benai_tuto_done_'+newUid],
    ['benai_last_motiv_'+oldUid,'benai_last_motiv_'+newUid],
    ['benai_lead_push_state_'+oldUid,'benai_lead_push_state_'+newUid],
    [getAppStorageSupabaseKey(oldUid),getAppStorageSupabaseKey(newUid)]
  ];
  pairs.forEach(([a,b])=>{
    const v=appStorage.getItem(a);
    if(v!==null&&v!==undefined){
      appStorage.setItem(b,v);
      appStorage.removeItem(a);
    }
  });
  const prefix='benai_abs_reminders_'+oldUid+'_';
  Object.keys(appStorageCache).forEach(k=>{
    if(k.startsWith(prefix)){
      const rest=k.slice(prefix.length);
      const nk='benai_abs_reminders_'+newUid+'_'+rest;
      const v=appStorage.getItem(k);
      if(v!==null){appStorage.setItem(nk,v);appStorage.removeItem(k);}
    }
  });
}

/** Après succès cloud : aligner stockage local (mots de passe, accès, messages, leads…). */
function applyBenAiLocalUserIdRename(oldUid,newUid){
  if(!oldUid||!newUid||normalizeId(oldUid)===normalizeId(newUid))return;
  migrateMemConversationsForUserRename(oldUid,newUid);
  migrateCommercialIdInLeads(oldUid,newUid);
  migrateConnexionLogsUserId(oldUid,newUid);
  migrateBenaiNameOverrideKey(oldUid,newUid);
  renameExactAppStoragePair(oldUid,newUid);
  const pwds=getPwds();
  if(Object.prototype.hasOwnProperty.call(pwds,oldUid)){
    pwds[newUid]=pwds[oldUid];
    delete pwds[oldUid];
    savePwds(pwds);
  }
  const access=getAccess();
  if(Object.prototype.hasOwnProperty.call(access,oldUid)){
    access[newUid]=access[oldUid];
    delete access[oldUid];
    saveAccess(access);
  }
  const extras=getExtraUsers();
  let exCh=false;
  const nextE=extras.map(e=>{
    if(normalizeId(e.id)===normalizeId(oldUid)){
      exCh=true;
      return{...e,id:newUid};
    }
    return e;
  });
  if(exCh)saveExtraUsers(nextE);
  if(USERS[oldUid]){
    USERS[newUid]={...USERS[oldUid]};
    delete USERS[oldUid];
  }
}

async function modifierBenaiLoginUid(uid){
  if(!canAssignBenaiLoginPseudo()){alert('Seul Benjamin peut modifier l’identifiant de connexion.');return;}
  const u=findUserById(uid)||findUserById(normalizeId(uid));
  if(!u)return;
  const isSelfBenjamin=normalizeId(uid)==='benjamin';
  if(isSelfBenjamin){
    if(!confirm('Tu définis le pseudo pour te connecter à BenAI (Supabase). Ton compte dans l’app reste « Benjamin » ; seul le mot de passe / email côté Supabase compte pour la connexion. Continuer ?'))return;
  }
  let authRaw=String(u.auth_uid||u.authUid||'').trim();
  if(isSelfBenjamin&&(!authRaw||!isLikelyUuid(authRaw))){
    authRaw=String(currentUser?.auth_uid||currentUser?.authUid||'').trim();
  }
  if(!isLikelyUuid(authRaw)){
    alert('Compte sans identifiant Supabase (auth). Connecte-toi avec email + mot de passe Supabase une fois pour lier la session, puis réessaie.');
    return;
  }
  const actuel=String(u.id||'').trim();
  const np=prompt(`Nouvel identifiant de connexion pour ${u.name}\n${isSelfBenjamin?`(identifiant interne BenAI : ${actuel} — inchangé)\n`:`(actuel dans BenAI : ${actuel})\n`}\nLettres, chiffres et _ (accents et espaces seront normalisés).`,actuel);
  if(np===null||!String(np).trim())return;
  const candidate=proposedLoginUidFromPseudoField(np,actuel);
  if(!candidate||candidate==='user'){alert('Identifiant invalide.');return;}
  const reserved=new Set(['benjamin','benai','admin']);
  if(reserved.has(candidate)){alert('Identifiant réservé.');return;}
  if(normalizeId(candidate)===normalizeId(actuel)){alert('Identique à l’actuel.');return;}
  if(getAllUsers().some(x=>normalizeId(x.id)===normalizeId(candidate)&&normalizeId(x.id)!==normalizeId(actuel))){
    alert('Cet identifiant est déjà utilisé.');
    return;
  }
  const cloud=await updateSupabaseUserAppUidProvisioning({
    target_user_id:authRaw,
    new_app_uid:candidate
  });
  if(!cloud.ok){alert('Échec serveur : '+(cloud.error||'?'));return;}
  const resolved=String(cloud.data?.app_uid||candidate).trim();
  if(!isSelfBenjamin)applyBenAiLocalUserIdRename(actuel,resolved);
  await syncExtraUsersFromSupabaseProfiles();
  scheduleAppStoragePersist();
  renderUsersList();
  renderPwdList();
  logActivity(`Benjamin a défini l’identifiant de connexion ${actuel} → ${resolved} (${u.name})`);
  alert(`Identifiant de connexion mis à jour : ${resolved}`);
}

function getExtraUsers(){try{return JSON.parse(appStorage.getItem('benai_extra_users'))||[];}catch{return [];}}
function saveExtraUsers(u){appStorage.setItem('benai_extra_users',JSON.stringify(u));}
function getExtraUserById(id){return getExtraUsers().find(u=>u.id===id)||null;}
function getHiddenUserIds(){
  try{return JSON.parse(appStorage.getItem('benai_hidden_users')||'[]')||[];}catch{return[];}
}
function saveHiddenUserIds(ids){
  appStorage.setItem('benai_hidden_users',JSON.stringify([...(new Set((ids||[]).map(x=>String(x||'').trim()).filter(Boolean)))]));
}
function hideUserId(uid){
  const normalized=normalizeId(uid);
  if(!normalized)return;
  const hidden=getHiddenUserIds();
  if(!hidden.includes(normalized))hidden.push(normalized);
  saveHiddenUserIds(hidden);
}
function unhideUserId(uid){
  const normalized=normalizeId(uid);
  if(!normalized)return;
  const hidden=getHiddenUserIds().filter(h=>normalizeId(h)!==normalized);
  saveHiddenUserIds(hidden);
}
async function syncExtraUsersFromSupabaseProfiles(){
  if(!currentUser||currentUser.role!=='admin')return false;
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return false;
  const session=await ensureSupabaseSession();
  if(!session?.access_token)return false;
  const headers=getSupabaseHeaders();
  if(!headers)return false;
  try{
    const res=await fetchWithTimeout(`${SUPABASE_CONFIG.url}/rest/v1/profiles?select=id,email,app_uid,full_name,role,company`,{headers},8000);
    if(!res.ok)return false;
    const rows=await res.json();
    const hiddenSet=new Set(getHiddenUserIds().map(normalizeId));
    const old=getExtraUsers().filter(u=>!hiddenSet.has(normalizeId(u.id)));
    const oldMap=new Map(old.map(u=>[u.id,u]));
    const merged=[...old];
    const exists=new Set(old.map(u=>u.id));
    const colorIdx={i:merged.length};
    rows.forEach(p=>{
      const email=(p?.email||'').trim().toLowerCase();
      const fullName=String(p?.full_name||email.split('@')[0]||'Utilisateur').trim();
      if(p?.role==='admin'&&/benjamin/i.test(String(p?.full_name||'')))return;
      const builtinBen=getBuiltinLoginEmails().benjamin;
      if(builtinBen&&email===String(builtinBen).trim().toLowerCase())return;
      let uid=normalizeId(p?.app_uid||email.split('@')[0]||fullName.split(/\s+/)[0]||'');
      if(!uid)return;
      if(uid==='benjamin'||uid==='benai')return;
      if(hiddenSet.has(uid))return;
      const prev=oldMap.get(uid);
      const newU={
        id:uid,
        auth_uid:p?.id||prev?.auth_uid||'',
        name:fullName,
        email,
        role:p?.role||prev?.role||'assistante',
        societe:p?.company||prev?.societe||'nemausus',
        fonction:prev?.fonction||'',
        vehicule:prev?.vehicule||'',
        color:prev?.color||COLORS[(colorIdx.i++)%COLORS.length],
        initial:(fullName[0]||uid[0]||'U').toUpperCase(),
        builtin:false
      };
      if(exists.has(uid)){
        const idx=merged.findIndex(u=>u.id===uid);
        if(idx>-1)merged[idx]={...merged[idx],...newU};
      }else{
        merged.push(newU);
        exists.add(uid);
      }
    });
    saveExtraUsers(merged);
    return true;
  }catch(e){
    return false;
  }
}
function getAllUsers(){
  const hiddenSet=new Set(getHiddenUserIds().map(normalizeId));
  const base=[
    {id:'benjamin',name:'Benjamin',role:'admin',societe:'les-deux',vehicule:'',color:'linear-gradient(135deg,#E8943A,#B45309)',initial:'B',builtin:true}
  ].filter(u=>!hiddenSet.has(normalizeId(u.id)));
  const extras=getExtraUsers()
    .filter(u=>!hiddenSet.has(normalizeId(u.id)))
    .map(u=>({...u,vehicule:u.vehicule||''}));
  return[...base,...extras];
}

function findBenaiAccountLinkedToAnnuaireEmploye(emp){
  if(!emp)return null;
  const full=normalizeId(`${String(emp.prenom||'').trim()} ${String(emp.nom||'').trim()}`.trim());
  const emails=new Set(
    [String(emp.emailPro||'').trim().toLowerCase(),String(emp.email||'').trim().toLowerCase()].filter(Boolean)
  );
  return getAllUsers().find(u=>{
    if(u.builtin||normalizeId(String(u.id||''))==='benjamin')return false;
    if(full){
      const un=normalizeId(String(u.name||''));
      if(un&&un===full)return true;
    }
    const ue=String(u.email||'').trim().toLowerCase();
    return!!(ue&&emails.has(ue));
  })||null;
}

// Trouver un utilisateur par identifiant normalisé
function findUserById(uid){
  const normalized=normalizeId(uid);
  return getAllUsers().find(u=>normalizeId(u.id)===normalized)||null;
}

function generateBenAIPassword(len=14){
  const chars='abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789';
  const arr=new Uint32Array(len);
  crypto.getRandomValues(arr);
  let s='';
  for(let i=0;i<len;i++)s+=chars[arr[i]%chars.length];
  return s;
}
function regenNewUserPwd(){
  const el=document.getElementById('new-user-pwd');
  if(el)el.value=generateBenAIPassword(14);
}

function showAddUser(){
  if(!canManageBenaiUsersAdmin()){alert('Réservé à l’administrateur.');return;}
  const f=document.getElementById('add-user-form');
  const isOpen=f.style.display!=='none';
  f.style.display=isOpen?'none':'block';
  if(!isOpen){
    const pr=document.getElementById('new-user-pseudo-row');
    if(pr)pr.style.display=canAssignBenaiLoginPseudo()?'block':'none';
    const pIn=document.getElementById('new-user-app-uid');
    if(pIn)pIn.value='';
    regenNewUserPwd();
    // Peupler le select avec les salariés de l'annuaire qui n'ont pas encore de compte BenAI
    const ann=getAnnuaireActive();
    const disponibles=ann.filter(e=>!findBenaiAccountLinkedToAnnuaireEmploye(e));
    const sel=document.getElementById('new-user-ann-select');
    if(sel){
      sel.innerHTML='<option value="">Sélectionner...</option>'+
        disponibles.map(e=>`<option value="${esc(e.prenom+'|'+e.nom+'|'+(e.societe||'nemausus')+'|'+(e.fonction||'Autre'))}">${esc(e.prenom+' '+e.nom)} — ${e.fonction||'?'}</option>`).join('');
    }
  }
}

async function creerUtilisateur(){
  if(!canManageBenaiUsersAdmin()){alert('Réservé à l’administrateur.');return;}
  const sel=document.getElementById('new-user-ann-select');
  const val=sel?.value||'';
  let pwd=(document.getElementById('new-user-pwd')?.value||'').trim();
  if(!pwd||pwd.length<10)pwd=generateBenAIPassword(14);
  const role=document.getElementById('new-user-role')?.value||'assistante';
  const status=document.getElementById('add-user-status');
  if(!val){status.style.color='var(--r)';status.textContent='⚠️ Sélectionnez un salarié';return;}
  const [prenom,nom,soc,fonction]=val.split('|');
  const name=`${prenom} ${nom}`.trim();
  let uid=normalizeId(prenom);
  let suffix=2;
  while(getAllUsers().some(u=>normalizeId(u.id)===uid)){
    uid=`${normalizeId(prenom)}_${suffix++}`;
  }
  if(canAssignBenaiLoginPseudo()){
    const custom=(document.getElementById('new-user-app-uid')?.value||'').trim();
    if(custom){
      uid=proposedLoginUidFromPseudoField(custom,uid);
      const reserved=new Set(['benjamin','benai','admin']);
      if(reserved.has(uid)||uid==='user'){
        status.style.color='var(--r)';
        status.textContent='⚠️ Identifiant de connexion réservé ou invalide';
        return;
      }
      if(getAllUsers().some(u=>normalizeId(u.id)===normalizeId(uid))){
        status.style.color='var(--r)';
        status.textContent='⚠️ Cet identifiant est déjà utilisé';
        return;
      }
    }
  }
  const ann=getAnnuaireActive();
  const annEntry=ann.find(e=>normalizeId(`${e.prenom} ${e.nom}`)===normalizeId(name)||normalizeId(e.prenom)===normalizeId(prenom))||null;
  if(annEntry&&findBenaiAccountLinkedToAnnuaireEmploye(annEntry)){
    status.style.color='var(--r)';
    status.textContent='⚠️ Un accès BenAI existe déjà pour ce salarié (même nom ou même e-mail). Supprimez l’ancien compte ou corrigez l’annuaire.';
    return;
  }
  const email=(annEntry?.emailPro||annEntry?.email||'').trim().toLowerCase();
  if(!email||!email.includes('@')){
    status.style.color='var(--r)';
    status.textContent='⚠️ Email introuvable dans l’annuaire pour cet utilisateur';
    return;
  }
  const extras=getExtraUsers();
  const roleLabel={assistante:'Assistante',commercial:'Commercial',directeur_co:'Directeur commercial',directeur_general:'Directeur général'}[role]||'Assistante';
  status.style.color='var(--a)';
  status.textContent='⏳ Création du compte (cloud + local)...';
  const createResult=await createSupabaseUserProvisioning({
    email,
    password:pwd,
    full_name:name,
    role,
    company:soc,
    app_uid:uid
  });
  const useLocalFallback=!createResult.ok&&shouldFallbackLocalUserProvisionAfterCreateError(createResult.error);
  if(!createResult.ok&&!useLocalFallback){
    status.style.color='var(--r)';
    let errShown=String(createResult.error||'Erreur');
    if(/maximum|limit exceeded|user count|too many|quota/i.test(errShown)){
      errShown+=' — Vérifiez le quota utilisateurs du projet Supabase (Authentication) ou supprimez d’anciens comptes.';
    }
    status.textContent='⚠️ '+errShown;
    return;
  }
  if(useLocalFallback){
    enqueuePendingUserCreate({email,password:pwd,full_name:name,role,company:soc,app_uid:uid});
  }
  await finalizeBenAILocalUserAccount({uid,name,email,role,soc,fonction,pwd,extras,roleLabel});
  document.getElementById('new-user-pwd').value=pwd;
  status.style.color='var(--g)';
  if(createResult.ok){
    status.textContent=`✅ Accès ${roleLabel} créé pour ${name}. Identifiant de connexion : ${uid} (ou email ${email}). Mot de passe dans le champ ci-dessus — copié dans le presse-papiers si le navigateur l’autorise.`;
  }else{
    status.style.color='var(--y)';
    status.textContent=`✅ Accès ${roleLabel} créé localement pour ${name}. Cloud : en file d’attente (Edge/Supabase) — reconnecte-toi en admin cloud puis laisse BenAI 30–60 s.`;
  }
  setTimeout(()=>{document.getElementById('add-user-form').style.display='none';status.textContent='';document.getElementById('new-user-pwd').value='';},8000);
  renderUsersList();renderPwdList();logActivity(`Benjamin a créé l'accès BenAI (${roleLabel}) pour ${name}`);
}

async function supprimerUtilisateur(uid, opts){
  opts=opts||{};
  if(!canManageBenaiUsersAdmin()){alert('Réservé à l’administrateur.');return;}
  if(normalizeId(uid)==='benjamin'){alert('Benjamin ne peut pas être supprimé.');return;}
  const u=findUserById(uid)||findUserById(normalizeId(uid));
  if(!u){alert('Utilisateur introuvable.');return;}
  const effectiveId=u.id;
  if(!opts.skipConfirm&&!confirm(`Supprimer ${u.name||effectiveId} définitivement ? Cette action est irréversible.`))return;
  const emailForCloud=(u.email||getEmailCandidateForUid(effectiveId,'')||'').trim().toLowerCase();
  const authUidRaw=String(u.auth_uid||u.authUid||'').trim();
  const userIdCloud=isLikelyUuid(authUidRaw)?authUidRaw:'';
  let remoteDeleted=false;
  const remoteRes=await deleteSupabaseUserProvisioning({
    user_id:userIdCloud,
    app_uid:effectiveId,
    email:emailForCloud
  });
  if(remoteRes.ok){
    remoteDeleted=true;
  }else{
    enqueuePendingUserDelete({uid:effectiveId,user_id:userIdCloud,email:emailForCloud,name:u.name||effectiveId});
    showDriveNotif('⚠️ Suppression cloud en attente — BenAI réessaiera automatiquement.');
  }
  hideUserId(effectiveId);
  const extras=getExtraUsers().filter(e=>normalizeId(e.id)!==normalizeId(effectiveId));
  saveExtraUsers(extras);
  Object.keys(USERS).forEach(k=>{
    if(normalizeId(k)===normalizeId(effectiveId))delete USERS[k];
  });
  const pwds=getPwds();
  Object.keys(pwds).forEach(k=>{
    if(normalizeId(k)===normalizeId(effectiveId))delete pwds[k];
  });
  savePwds(pwds);
  const access=getAccess();
  Object.keys(access).forEach(k=>{
    if(normalizeId(k)===normalizeId(effectiveId))delete access[k];
  });
  saveAccess(access);
  const loginSel=document.getElementById('login-user');
  if(loginSel){
    [...loginSel.querySelectorAll('option')].forEach(opt=>{
      if(String(opt.value||'')===String(effectiveId)||normalizeId(opt.value)===normalizeId(effectiveId))opt.remove();
    });
  }
  scheduleAppStoragePersist();
  renderUsersList();renderPwdList();
  logActivity(`Benjamin a supprimé l'utilisateur ${u.name||effectiveId}${remoteDeleted?' (cloud + local)':' (local)'}`);
  if(remoteDeleted)showDriveNotif(`✅ ${u.name||effectiveId} supprimé définitivement`);
  else showDriveNotif(`✅ ${u.name||effectiveId} retiré de BenAI (cloud en attente si besoin)`);
}

function toggleAccess(uid){
  if(!canManageBenaiUsersAdmin()){alert('Réservé à l’administrateur.');return;}
  const access=getAccess();
  access[uid]=access[uid]===false?true:false;
  saveAccess(access);renderUsersList();
  logActivity(`Benjamin a ${access[uid]===false?'bloqué':'réactivé'} l'accès de ${uid}`);
}

function getLatestConnexionHintsByUid(){
  try{
    const logs=JSON.parse(appStorage.getItem('benai_connexions')||'[]');
    if(!Array.isArray(logs))return{};
    const out={};
    for(let i=0;i<logs.length;i++){
      const l=logs[i];
      if(!l||!l.uid)continue;
      if(out[l.uid])continue;
      out[l.uid]={date:l.date||'',mobile:!!l.mobile,pwa:!!l.pwa};
    }
    return out;
  }catch{
    return{};
  }
}

function renderMobileUsageSummary(){
  const el=document.getElementById('mobile-usage-list');
  if(!el)return;
  const hints=getLatestConnexionHintsByUid();
  const users=getAllUsers().filter(u=>u&&u.id&&u.id!=='benai');
  if(!users.length){
    el.innerHTML='<div style="color:var(--t3);font-size:12px">Aucun utilisateur</div>';
    return;
  }
  const rows=users.map(u=>{
    const h=hints[u.id];
    const mob=h?h.mobile:false;
    const pwa=h?h.pwa:false;
    const when=h&&h.date?h.date:'— pas encore dans l’historique —';
    let label='Ordinateur / inconnu';
    if(mob&&pwa)label='Mobile + mode installé (PWA)';
    else if(mob)label='Navigateur mobile';
    else if(pwa)label='Mode installé (PWA)';
    return`<div style="display:flex;flex-wrap:wrap;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid var(--b1);font-size:12px">
      <span style="font-weight:600;min-width:130px">${esc(u.name)}</span>
      <span style="color:var(--t2);flex:1;min-width:160px">${esc(label)}</span>
      <span style="font-size:10px;color:var(--t3);white-space:nowrap">${esc(when)}</span>
    </div>`;
  }).join('');
  el.innerHTML='<div style="font-size:10px;color:var(--t3);margin-bottom:10px;line-height:1.45">Chaque ligne reprend la <strong>dernière connexion enregistrée</strong> pour cet utilisateur (détection navigateur au login). Ce n’est pas une preuve absolue d’installation sur téléphone.</div>'+rows;
}

function renderConnexionsHistory(){
  const el=document.getElementById('connexions-list');if(!el)return;
  const logs=getConnexions().slice(0,20);
  if(!logs.length){el.innerHTML='<div style="color:var(--t3);font-size:12px">Aucune connexion enregistrée</div>';return;}
  el.innerHTML='';
  logs.forEach(l=>{
    const u=getAllUsers().find(x=>x.id===l.uid);
    const d=document.createElement('div');
    d.style.cssText='display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--b1)';
    const av=document.createElement('div');
    av.style.cssText='width:24px;height:24px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:#fff;flex-shrink:0;background:'+(u?u.color:'#555');
    av.textContent=u?u.initial:'?';
    const nm=document.createElement('div');nm.style.cssText='flex:1;font-size:12px;font-weight:500';nm.textContent=u?u.name:l.uid;
    const rl=document.createElement('div');rl.style.cssText='font-size:10px;color:var(--t3);min-width:100px;text-align:right;line-height:1.25';
    const roleTxt=u?(ROLE_LABELS[u.role]||''):'';
    const dev=(l.mobile?'📱':'')+(l.pwa?'📲':'')+(l.mobile||l.pwa?'':'💻');
    rl.textContent=roleTxt+(roleTxt&&dev?' · ':'')+dev;
    const dt=document.createElement('div');dt.style.cssText='font-size:10px;color:var(--t3)';dt.textContent=l.date;
    d.append(av,nm,rl,dt);el.appendChild(d);
  });
}

function renderUsersList(){
  const list=document.getElementById('users-list');if(!list)return;
  const search=(document.getElementById('user-search')?.value||'').toLowerCase();
  let users=getAllUsers();
  if(search){
    users=users.filter(u=>{
      const id=String(u.id||'').toLowerCase();
      return (u.name||'').toLowerCase().includes(search)||(u.role||'').includes(search)||id.includes(search);
    });
  }
  const access=getAccess();
  list.innerHTML='';
  users.forEach(u=>{
    const isBlocked=access[u.id]===false;
    const isCRM=(u.role==='commercial'||u.role==='directeur_co'||u.role==='directeur_general');
    const derniereConnexion=getConnexions(u.id)[0]?.date||null;
    const div=document.createElement('div');div.className='user-row';
    const av=document.createElement('div');
    av.className='user-av';
    av.style.background=u.color;
    av.textContent=u.initial;
    const info=document.createElement('div');
    info.className='user-info';
    info.innerHTML=`
        <div class="user-name">${esc(u.name)}</div>
        <div style="font-size:10px;color:var(--t3);margin-top:2px">${u.id==='benjamin'?'Identifiant interne':'Connexion'} : <code style="font-size:10px;background:var(--s2);padding:2px 6px;border-radius:4px">${esc(u.id)}</code>${u.id==='benjamin'&&canAssignBenaiLoginPseudo()?' <span style="opacity:.9">· pseudo à la connexion (Supabase) : bouton 🔑</span>':''}</div>
        <div class="user-role">${ROLE_LABELS[u.role]||u.role} · ${u.societe==='les-deux'?'Nemausus & Lambert':u.societe==='nemausus'?'Nemausus':'Lambert'}</div>
        ${(u.role==='commercial'||u.role==='directeur_co'||u.role==='directeur_general')?`<div style="font-size:10px;color:var(--t3)">🚐 Véhicule : ${esc(u.vehicule||'Non renseigné')}</div>`:''}
        <div style="font-size:10px;color:var(--t3)">${derniereConnexion?'Dernière connexion : '+derniereConnexion:'Jamais connecté'}</div>`;
    div.append(av,info);
    if(u.id!=='benjamin'){
      const uid=u.id;
      const btnStyle='padding:4px 8px;border-radius:5px;font-size:11px;cursor:pointer;font-family:inherit';
      const editBtn=document.createElement('button');
      editBtn.type='button';
      editBtn.style.cssText=btnStyle+';background:var(--a3);color:var(--a);border:1px solid var(--a)';
      editBtn.textContent='✏️';
      editBtn.addEventListener('click',()=>editUser(uid));
      const togg=document.createElement('div');
      togg.className='toggle '+(isBlocked?'':'on');
      togg.title=isBlocked?'Bloqué':'Actif';
      togg.addEventListener('click',()=>toggleAccess(uid));
      div.append(editBtn,togg);
      if(isCRM){
        const blockBtn=document.createElement('button');
        blockBtn.type='button';
        blockBtn.style.cssText=btnStyle+';'+(isBlocked?'background:var(--g2);color:var(--g);border:1px solid rgba(34,197,94,.3)':'background:var(--r2);color:var(--r);border:1px solid rgba(248,113,113,.3)');
        blockBtn.textContent=isBlocked?'🔓 Débloquer':'🔒 Bloquer';
        blockBtn.addEventListener('click',()=>toggleBlockCommercial(uid));
        div.appendChild(blockBtn);
      }
      const delBtn=document.createElement('button');
      delBtn.type='button';
      delBtn.style.cssText=btnStyle+';background:var(--r2);color:var(--r);border:1px solid rgba(248,113,113,.3)';
      delBtn.textContent='🗑️';
      delBtn.addEventListener('click',()=>supprimerUtilisateur(uid));
      div.appendChild(delBtn);
      if(canAssignBenaiLoginPseudo()){
        const idBtn=document.createElement('button');
        idBtn.type='button';
        idBtn.style.cssText=btnStyle+';background:var(--s2);color:var(--t1);border:1px solid var(--b1)';
        idBtn.title='Modifier l’identifiant de connexion (pseudo BenAI)';
        idBtn.textContent='🔑';
        idBtn.addEventListener('click',()=>void modifierBenaiLoginUid(uid));
        div.appendChild(idBtn);
      }
    }else{
      const adm=document.createElement('span');
      adm.style.cssText='font-size:10px;color:var(--t3)';
      adm.textContent='Admin';
      div.appendChild(adm);
      if(canAssignBenaiLoginPseudo()){
        const btnStyle='padding:4px 8px;border-radius:5px;font-size:11px;cursor:pointer;font-family:inherit';
        const idBtn=document.createElement('button');
        idBtn.type='button';
        idBtn.style.cssText=btnStyle+';background:var(--s2);color:var(--t1);border:1px solid var(--b1)';
        idBtn.title='Choisir ton pseudo de connexion (email inchangé)';
        idBtn.textContent='🔑';
        idBtn.addEventListener('click',()=>void modifierBenaiLoginUid('benjamin'));
        div.appendChild(idBtn);
      }
    }
    list.appendChild(div);
  });
}

function editUser(uid){
  if(!canManageBenaiUsersAdmin()){alert('Réservé à l’administrateur.');return;}
  const users=getAllUsers();const u=users.find(x=>x.id===uid);if(!u)return;
  const nouveau=prompt(`Modifier ${u.name}\n\nNom affiché (actuel: ${u.name}) :`);
  if(!nouveau||!nouveau.trim())return;
  let vehicule=u.vehicule||'';
  if(u.role==='commercial'||u.role==='directeur_co'||u.role==='directeur_general'){
    const vPrompt=prompt(`Véhicule de société pour ${nouveau.trim()} (optionnel) :`,vehicule);
    if(vPrompt!==null)vehicule=vPrompt.trim();
  }
  // Mettre à jour dans USERS et extraUsers
  if(USERS[uid]){
    USERS[uid].name=nouveau.trim();
    USERS[uid].vehicule=vehicule;
  }
  const extras=getExtraUsers();
  const idx=extras.findIndex(e=>e.id===uid);
  if(idx>-1){extras[idx].name=nouveau.trim();extras[idx].vehicule=vehicule;saveExtraUsers(extras);}
  // Mettre à jour les builtin via stockage session override
  const overrides=JSON.parse(appStorage.getItem('benai_name_overrides')||'{}');
  overrides[uid]=nouveau.trim();
  appStorage.setItem('benai_name_overrides',JSON.stringify(overrides));
  renderUsersList();renderPwdList();
  logActivity(`Benjamin a modifié le nom de ${uid} → ${nouveau.trim()}`);
  alert(`✅ Nom modifié : ${nouveau.trim()}`);
}

function renderPwdList(){
  const list=document.getElementById('pwd-list');if(!list)return;
  list.innerHTML=getAllUsers().map(u=>`
    <div class="user-row">
      <div class="user-av" style="background:${u.color};width:26px;height:26px;border-radius:7px;font-size:11px">${u.initial}</div>
      <div style="flex:1;font-size:13px;font-weight:500">${esc(u.name)}</div>
      <div style="font-size:11px;color:var(--t3);font-family:'JetBrains Mono',monospace;background:var(--bg);padding:4px 10px;border-radius:6px">•••• sécurisé</div>
      <button onclick="changerMdp('${u.id}','${u.name}')" style="padding:5px 10px;background:var(--a3);color:var(--a);border:1px solid var(--a);border-radius:6px;font-size:11px;font-weight:600;cursor:pointer;font-family:inherit">Modifier</button>
    </div>`).join('');
  // Panel settings aussi
  renderTeamPwdSettings();
}

async function changerMdp(uid,name){
  if(currentUser?.role!=='admin'){alert('⚠️ Réservé à l’administrateur');return;}
  const nouveau=prompt(`Nouveau mot de passe pour ${name} :\n\n✅ Minimum 6 caractères\n✅ Au moins 1 majuscule\n✅ Au moins 1 caractère spécial (!@#$%...)`);
  if(!nouveau)return;
  const err=validatePassword(nouveau);
  if(err){alert('⚠️ '+err);return;}
  const u=findUserById(uid)||findUserById(normalizeId(uid));
  const targetId=u?.id||uid;
  const emailCandidate=(u?.email||getEmailCandidateForUid(targetId,'')||'').trim().toLowerCase();
  const pwds=getPwds();
  const hashed=await hashPassword(targetId,nouveau);
  pwds[targetId]=hashed;
  const nk=normalizeId(targetId);
  if(nk&&nk!==targetId)pwds[nk]=hashed;
  savePwds(pwds);
  Object.keys(USERS).forEach(k=>{
    if(normalizeId(k)===nk)USERS[k].pwd=hashed;
  });
  const cloudPayload={password:nouveau};
  const authRaw=u?.auth_uid||u?.authUid||'';
  if(isLikelyUuid(String(authRaw||'')))cloudPayload.user_id=String(authRaw).trim();
  else if(emailCandidate.includes('@'))cloudPayload.email=emailCandidate;
  cloudPayload.app_uid=String(targetId||'').trim();
  const cloud=await updateSupabaseUserPasswordProvisioning(cloudPayload);
  renderPwdList();
  logActivity(`Benjamin a modifié le mot de passe de ${name}`);
  if(cloud.ok){
    alert(`✅ Mot de passe de ${name} modifié (BenAI + cloud).`);
  }else if(SUPABASE_CONFIG.enabled){
    alert(`✅ Mot de passe local enregistré pour ${name}.\n\n⚠️ Cloud : ${cloud.error||'non synchronisé'}\nDéployez la fonction Supabase « update-user-password » (même principe que delete-user), puis réessayez.`);
  }else{
    alert(`✅ Mot de passe de ${name} modifié (stockage local).`);
  }
}

// PARAMÈTRES
async function forcePwdChange(){
  const pwd=document.getElementById('force-pwd-input').value;
  const confirm=document.getElementById('force-pwd-confirm').value;
  const err=document.getElementById('force-pwd-err');
  // Validation
  const validErr=validatePassword(pwd);
  if(validErr){err.textContent='⚠️ '+validErr;return;}
  if(pwd!==confirm){err.textContent='⚠️ Les mots de passe ne correspondent pas';return;}
  // Sauvegarder
  const pwds=getPwds();
  pwds[currentUser.id]=await hashPassword(currentUser.id,pwd);
  savePwds(pwds);
  if(USERS[currentUser.id])USERS[currentUser.id].pwd=pwds[currentUser.id];
  // Masquer l'écran et lancer l'app
  document.getElementById('force-pwd-screen').style.display='none';
  document.getElementById('app').classList.add('visible');
  logActivity(`${currentUser.name} a défini son mot de passe à la première connexion`);
  initApp();
}

function toggleSettings(){
  const panel=document.getElementById('settings-panel');
  const isOpen=panel.style.display!=='none';
  panel.style.display=isOpen?'none':'block';
  if(!isOpen)initSettings();
}

function getStoredTheme(){
  return appStorage.getItem('benai_theme')==='light'?'light':'dark';
}

function applyTheme(theme=getStoredTheme()){
  document.body.classList.toggle('light',theme==='light');
  const btnDark=document.getElementById('btn-dark');
  const btnLight=document.getElementById('btn-light');
  if(btnDark)btnDark.style.borderColor=theme==='dark'?'var(--a)':'var(--b1)';
  if(btnLight)btnLight.style.borderColor=theme==='light'?'var(--a)':'var(--b1)';
}

function initSettings(){
  const isAdmin=currentUser?.role==='admin';
  document.getElementById('api-settings').style.display=isAdmin?'block':'none';
  applyTheme();
  if(isAdmin){
    renderTeamPwdSettings();
    const apiMsg=document.getElementById('api-msg');
    if(apiMsg){
      const hasKey=!!appStorage.getItem('benai_api');
      apiMsg.style.color=hasKey?'var(--g)':'var(--y)';
      apiMsg.textContent=hasKey?'✅ Clé IA partagée active':'⚠️ Renseignez la clé IA partagée pour toute l’équipe';
    }
    // Afficher client ID Google Drive existant
    const clientIdEl=document.getElementById('gdrive-client-id');
    if(clientIdEl)clientIdEl.value=getGDriveClientId()||'';
    const lastBackup=appStorage.getItem('benai_gdrive_last_backup');
    const lastEl=document.getElementById('gdrive-last-backup');
    if(lastEl&&lastBackup)lastEl.textContent='Dernière sauvegarde : '+new Date(lastBackup).toLocaleString('fr-FR');
    const sbUrlInput=document.getElementById('supabase-url-settings');
    const sbKeyInput=document.getElementById('supabase-key-settings');
    const sbMsg=document.getElementById('supabase-msg');
    if(sbUrlInput)sbUrlInput.value=appStorage.getItem(STORAGE_KEYS.sbUrl)||SUPABASE_CONFIG.url||SUPABASE_DEFAULT_URL;
    if(sbKeyInput)sbKeyInput.value=appStorage.getItem(STORAGE_KEYS.sbPublishable)||appStorage.getItem(STORAGE_KEYS.sbAnonLegacy)||SUPABASE_DEFAULT_PUBLISHABLE_KEY;
    if(sbMsg){
      const hasUrl=!!(appStorage.getItem(STORAGE_KEYS.sbUrl)||SUPABASE_DEFAULT_URL||'').trim();
      const hasKey=!!(appStorage.getItem(STORAGE_KEYS.sbPublishable)||appStorage.getItem(STORAGE_KEYS.sbAnonLegacy)||SUPABASE_DEFAULT_PUBLISHABLE_KEY||'').trim();
      sbMsg.style.color=hasUrl&&hasKey?'var(--g)':'var(--y)';
      sbMsg.textContent=hasUrl&&hasKey?'✅ Supabase configuré':'⚠️ Renseignez URL et clé publishable Supabase';
      renderSupabaseRuntimeStatus();
    }
  }
}

function setTheme(theme){
  appStorage.setItem('benai_theme',theme);
  applyTheme(theme);
}

async function changerMonMdp(){
  const pwd=document.getElementById('new-pwd').value.trim();const msg=document.getElementById('pwd-msg');
  const err=validatePassword(pwd);
  if(err){msg.style.color='var(--r)';msg.textContent='⚠️ '+err;return;}
  const client=getSupabaseClient();
  if(client&&currentSupabaseSession?.access_token){
    try{
      const {error}=await client.auth.updateUser({password:pwd});
      if(error)throw error;
    }catch(e){
      msg.style.color='var(--r)';
      msg.textContent='⚠️ Supabase : '+(e?.message||e);
      return;
    }
  }
  const pwds=getPwds();
  const hashed=await hashPassword(currentUser.id,pwd);
  pwds[currentUser.id]=hashed;
  const nk=normalizeId(currentUser.id);
  if(nk&&nk!==currentUser.id)pwds[nk]=hashed;
  savePwds(pwds);
  if(USERS[currentUser.id])USERS[currentUser.id].pwd=hashed;
  document.getElementById('new-pwd').value='';msg.style.color='var(--g)';msg.textContent='✅ Modifié !';
  setTimeout(()=>msg.textContent='',2000);logActivity(`${currentUser.name} a changé son mot de passe`);
}

function renderTeamPwdSettings(){
  const list=document.getElementById('team-pwd-list');if(!list)return;
  list.innerHTML=getAllUsers().map(u=>`
    <div style="display:flex;align-items:center;gap:8px;padding:7px 10px;background:var(--s3);border-radius:8px;margin-bottom:5px">
      <div style="width:22px;height:22px;border-radius:6px;background:${u.color};display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#fff">${u.initial}</div>
      <div style="flex:1;font-size:12px;font-weight:500">${u.name}</div>
      <div style="font-size:11px;color:var(--t3);font-family:'JetBrains Mono',monospace;background:var(--bg);padding:3px 8px;border-radius:5px">•••• sécurisé</div>
      <button onclick="changerMdp('${u.id}','${u.name}')" style="padding:3px 8px;background:var(--a3);color:var(--a);border:1px solid var(--a);border-radius:5px;font-size:10px;cursor:pointer;font-family:inherit">✏️</button>
    </div>`).join('');
}

async function saveApiKeySettings(){
  const key=normalizeAnthropicKey(document.getElementById('api-key-settings').value);const msg=document.getElementById('api-msg');
  if(currentUser?.role!=='admin'){msg.style.color='var(--r)';msg.textContent='⚠️ Réservé à l’administrateur';return;}
  if(!isLikelyAnthropicKey(key)){msg.style.color='var(--r)';msg.textContent='⚠️ Clé Anthropic invalide (format attendu: sk-ant-...)';return;}
  appStorage.setItem('benai_api',key);document.getElementById('api-key-settings').value='';
  const syncResult=await saveSharedApiKeyToSupabase(key);
  if(syncResult.ok){
    pendingSharedApiKey='';
    sharedApiKeyRetryDelayMs=2500;
    msg.style.color='var(--g)';
    msg.textContent='✅ Clé IA enregistrée et synchronisée Supabase';
  }else if(syncResult.reason==='profile_missing'){
    scheduleSharedApiKeySyncRetry(key,'profile-missing');
    msg.style.color='var(--y)';
    msg.textContent='⚠️ Clé locale enregistrée — profil Supabase manquant, nouvelle tentative auto activée';
  }else if(syncResult.reason==='missing_session'){
    scheduleSharedApiKeySyncRetry(key,'missing-session');
    msg.style.color='var(--y)';
    msg.textContent='⚠️ Clé locale enregistrée — session Supabase inactive, sync auto en attente';
  }else{
    scheduleSharedApiKeySyncRetry(key,syncResult.reason||'unknown');
    msg.style.color='var(--y)';
    msg.textContent='⚠️ Clé locale enregistrée — synchro Supabase en retry automatique';
  }
  setTimeout(()=>msg.textContent='',3500);
}

function saveSupabaseSettings(){
  const msg=document.getElementById('supabase-msg');
  if(currentUser?.role!=='admin'){
    setStatusMessage(msg,'var(--r)','⚠️ Réservé à l’administrateur');
    return false;
  }
  const urlInput=document.getElementById('supabase-url-settings');
  const keyInput=document.getElementById('supabase-key-settings');
  const previousUrl=SUPABASE_CONFIG.url||'';
  const previousKey=(SUPABASE_CONFIG.publishableKey||'').trim();
  const url=normalizeSupabaseUrl(urlInput?.value||'');
  const key=(document.getElementById('supabase-key-settings')?.value||'').trim();
  if(!url.startsWith('https://')||!url.includes('.supabase.co')){
    setStatusMessage(msg,'var(--r)','⚠️ URL Supabase invalide');
    return false;
  }
  if(!isLikelySupabasePublicKey(key)){
    setStatusMessage(msg,'var(--r)','⚠️ Clé invalide (utilisez Publishable, pas Secret/service_role)');
    return false;
  }
  if(urlInput)urlInput.value=url;
  if(keyInput)keyInput.value=key;
  appStorage.setItem(STORAGE_KEYS.sbUrl,url);
  appStorage.setItem(STORAGE_KEYS.sbPublishable,key);
  appStorage.setItem(STORAGE_KEYS.sbEnabled,'1');
  const configChanged=normalizeSupabaseUrl(previousUrl)!==url||previousKey!==key;
  if(configChanged){
    stopSupabaseRealtimeSync();
    if(supabaseAuthSubscription){
      try{supabaseAuthSubscription.unsubscribe();}catch(e){}
      supabaseAuthSubscription=null;
    }
    supabaseClient=null;
    currentSupabaseSession=null;
    currentAuthMode='unknown';
    lastSupabaseSyncError='';
    supabaseLastPushOkTs=0;
    supabaseLastPullOkTs=0;
    setStatusMessage(msg,'var(--y)','✅ Configuration enregistrée — reconnectez-vous pour réactiver la session Supabase');
    applySupabaseConfigFromStorage();
    return true;
  }
  applySupabaseConfigFromStorage();
  setStatusMessage(msg,'var(--g)','✅ Configuration Supabase enregistrée');
  renderSupabaseRuntimeStatus();
  return true;
}

async function testSupabaseSettings(){
  const msg=document.getElementById('supabase-msg');
  const isSaved=saveSupabaseSettings();
  if(!isSaved)return;
  setStatusMessage(msg,'var(--a)','⏳ Test de connexion en cours...');
  try{
    const headers=getSupabaseHeaders();
    if(!headers)throw new Error('Clé manquante');
    // Test neutre : endpoint Auth public, sans dépendre des policies SQL.
    const url=`${SUPABASE_CONFIG.url}${SUPABASE_TEST_ENDPOINT}`;
    const res=await fetch(url,{headers:{apikey:headers.apikey}});
    if(!res.ok)throw new Error('Connexion refusée');
    setStatusMessage(msg,'var(--g)','✅ Connexion Supabase OK');
    renderSupabaseRuntimeStatus();
  }catch(e){
    setStatusMessage(msg,'var(--r)','⚠️ Test échoué : vérifiez URL/clé/policies');
  }
}

function saveGDriveClientIdSettings(){
  const id=document.getElementById('gdrive-client-id').value.trim();
  const msg=document.getElementById('gdrive-msg');
  if(!id||!id.includes('googleusercontent.com')){
    msg.style.color='var(--r)';msg.textContent='⚠️ Client ID invalide';
    setTimeout(()=>msg.textContent='',2000);return;
  }
  saveGDriveClientId(id);
  msg.style.color='var(--g)';msg.textContent='✅ Client ID enregistré ! La sauvegarde sera active à la prochaine connexion.';
  setTimeout(()=>msg.textContent='',3000);
  logActivity('Benjamin a configuré la sauvegarde Google Drive');
}

// SAUVEGARDE / RESTAURATION
function exportData(){
  const mem=getMem();const pwds=getPwds();const extras=getExtraUsers();
  const access=getAccess();const annuaire=getAnnuaire();
  const backup={version:BENAI_VERSION,date:new Date().toISOString(),data:mem,pwds,extras,access,annuaire};
  download('BenAI_sauvegarde_'+new Date().toLocaleDateString('fr-FR').replace(/\//g,'-')+'.json',JSON.stringify(backup,null,2),'application/json');
  const msg=document.getElementById('backup-msg');if(msg){msg.style.color='var(--g)';msg.textContent='✅ Sauvegarde téléchargée !';setTimeout(()=>msg.textContent='',3000);}
  logActivity('Benjamin a sauvegardé les données');
}

function importData(input){
  const file=input.files[0];if(!file)return;
  const reader=new FileReader();
  reader.onload=e=>{
    try{
      const backup=JSON.parse(e.target.result);
      if(!backup.data)throw new Error('Format invalide');
      if(!confirm(`Restaurer la sauvegarde du ${new Date(backup.date).toLocaleDateString('fr-FR')} ?\n\nToutes les données actuelles seront remplacées.`))return;
      saveMem(backup.data);
      savePwds(backup.pwds||{});
      saveExtraUsers(backup.extras||[]);
      saveAccess(backup.access||{});
      if(backup.annuaire)saveAnnuaire(backup.annuaire);
      alert('✅ Données restaurées ! La page va se recharger.');
      location.reload();
    }catch(err){alert('❌ Erreur : fichier de sauvegarde invalide');}
  };
  reader.readAsText(file);input.value='';
}

// ══════════════════════════════════════════
// ══════════════════════════════════════════
// 🧠 INTELLIGENCE PROACTIVE
// ══════════════════════════════════════════

// PATTERNS — mémoire longue
function getPatterns(){try{return JSON.parse(appStorage.getItem('benai_patterns'))||{topFournisseurs:{},topClients:{},savCount:0,resolvedCount:0};}catch{return{topFournisseurs:{},topClients:{},savCount:0,resolvedCount:0};}}
function savePatterns(p){appStorage.setItem('benai_patterns',JSON.stringify(p));}

function updatePatterns(fournisseur,client){
  const p=getPatterns();
  if(fournisseur){p.topFournisseurs[fournisseur]=(p.topFournisseurs[fournisseur]||0)+1;}
  if(client){p.topClients[client]=(p.topClients[client]||0)+1;}
  p.savCount=(p.savCount||0)+1;
  savePatterns(p);
}

function getTopPatterns(){
  const p=getPatterns();
  const topF=Object.entries(p.topFournisseurs||{}).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k);
  const topC=Object.entries(p.topClients||{}).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k])=>k);
  return {topFournisseurs:topF,topClients:topC,savCount:p.savCount||0};
}

// PRIORITÉ SAV
function computeSAVPriority(sav){
  let score=0;
  if(sav.urgent)score+=50;
  if(sav.statut==='nouveau')score+=20;
  const days=getSAVAgeDays(sav);
  score+=days*4;
  if(!sav.fournisseur)score+=8;
  if(!sav.actions||sav.actions.length===0)score+=15;
  return score;
}

function getSAVAgeDays(sav){
  if(!sav.date_creation)return 0;
  const d=parseDateFR(sav.date_creation);
  if(!d)return 0;
  return Math.floor((Date.now()-d)/(1000*60*60*24));
}

function parseDateFR(str){
  if(!str)return null;
  const parts=str.split('/');
  if(parts.length===3)return new Date(+parts[2],+parts[1]-1,+parts[0]).getTime();
  return null;
}

function getPriorityBadge(score){
  if(score>=60)return '<span style="background:rgba(248,113,113,.15);color:var(--r);padding:2px 7px;border-radius:10px;font-size:10px;font-weight:700">🔥 Critique</span>';
  if(score>=30)return '<span style="background:rgba(251,191,36,.15);color:var(--y);padding:2px 7px;border-radius:10px;font-size:10px;font-weight:700">⚠️ Important</span>';
  return '';
}

// BRIEFING QUOTIDIEN INTELLIGENT
async function generateDailyBriefing(){
  const todayKey='benai_briefing_'+new Date().toDateString();
  if(appStorage.getItem(todayKey))return; // Déjà fait aujourd'hui
  const apiKey=getApiKey();if(!apiKey)return;
  const ctx=buildBriefingContext();
  const typing=addTyping();
  try{
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({
        model:'claude-sonnet-4-20250514',max_tokens:350,
        messages:[{role:'user',content:`Tu es BenAI, l'assistant de Benjamin. Génère son briefing du matin : 3-4 phrases maximum, direct, utile, proactif. Commence par ce qui est le plus urgent. Tutoie-le. Pas de formule de politesse, va à l'essentiel. Si tout va bien, dis-le en une phrase et propose quelque chose de concret.\n\n${ctx}`}]
      })
    });
    const data=await res.json();
    typing.remove();
    if(data.content&&data.content[0]){
      addAIMsg(data.content[0].text);
      if(data.usage)trackTokens('benjamin',data.usage.input_tokens||0,data.usage.output_tokens||0);
      appStorage.setItem(todayKey,'1');
      // Résumé hebdomadaire le vendredi
      if(new Date().getDay()===5)setTimeout(()=>generateWeeklySummary(),2000);
    }
  }catch(e){
    typing.remove();
    addAIMsg(buildFallbackBriefing());
    appStorage.setItem(todayKey,'1');
    if(new Date().getDay()===5)setTimeout(()=>generateWeeklySummary(),2000);
  }
}

function buildBriefingContext(){
  const mem=getMem();
  const days=['dimanche','lundi','mardi','mercredi','jeudi','vendredi','samedi'];
  const schedule={1:'Nemausus Fermetures',3:'Nemausus Fermetures',5:'Nemausus Fermetures',2:'Lambert SAS',4:'Lambert SAS'};
  const today=new Date().getDay();
  const site=schedule[today]||'chez toi (télétravail)';
  const savs=mem.sav||[];
  const openSAV=savs.filter(s=>s.statut!=='regle');
  const urgentSAV=openSAV.filter(s=>s.urgent);
  const criticalSAV=openSAV.filter(s=>getSAVAgeDays(s)>=5).slice(0,3);
  const convs=getConvsForUser('benjamin');
  const lrBen=getLastRead('benjamin');
  let unread=0;for(const cid of Object.keys(convs))unread+=countUnread('benjamin',cid,mem,lrBen);
  const todayDate=new Date();
  const absToday=(mem.absences||[]).filter(a=>!a._deleted&&new Date(a.debut)<=todayDate&&new Date(a.fin)>=todayDate);
  const absWeek=(mem.absences||[]).filter(a=>{if(a._deleted)return false;const d=new Date(a.debut);const w=new Date();w.setDate(w.getDate()+7);return d>todayDate&&d<=w;});
  const reminders=getSmartReminders();
  const patterns=getTopPatterns();
  const nem=openSAV.filter(s=>s.societe==='nemausus').length;
  const lam=openSAV.filter(s=>s.societe==='lambert').length;
  return `Aujourd'hui: ${days[today]}, Benjamin est à ${site}.
SAV: ${openSAV.length} ouvert(s) — Nemausus: ${nem}, Lambert: ${lam} — Urgents: ${urgentSAV.length}${urgentSAV.length>0?' ('+urgentSAV.slice(0,2).map(s=>s.client+' - '+s.probleme).join(', ')+')':''}
${criticalSAV.length>0?'SAV sans réponse +5j: '+criticalSAV.map(s=>`${s.client} (${getSAVAgeDays(s)}j, fournisseur: ${s.fournisseur||'non renseigné'})`).join(', '):''}
Messages non lus: ${unread}${unread>0?' — vérifier la messagerie':''}
${absToday.length>0?'Absents aujourd\'hui: '+absToday.map(a=>a.employe).join(', '):'Toute l\'équipe est présente'}
${absWeek.length>0?'Absences à venir: '+absWeek.map(a=>a.employe+' le '+formatDate(a.debut)).join(', '):''}
${reminders.length>0?'Rappels notes: '+reminders.slice(0,3).join(' | '):''}
${patterns.topFournisseurs.length>0?'Fournisseurs habituels: '+patterns.topFournisseurs.join(', '):''}`;
}

function buildFallbackBriefing(){
  const mem=getMem();
  const open=(mem.sav||[]).filter(s=>!s._deleted&&s.statut!=='regle').length;
  const urgent=(mem.sav||[]).filter(s=>!s._deleted&&s.urgent&&s.statut!=='regle').length;
  const days=['dimanche','lundi','mardi','mercredi','jeudi','vendredi','samedi'];
  const schedule={1:'Nemausus',3:'Nemausus',5:'Nemausus',2:'Lambert',4:'Lambert'};
  const d=new Date().getDay();
  const site=schedule[d]||'chez toi';
  return `**Briefing du ${days[d]}** — Tu es à ${site}.\n${open>0?`🔧 ${open} SAV ouvert(s)${urgent>0?' dont '+urgent+' urgent(s)':''}.`:'✅ Aucun SAV en attente.'}\nBonne journée Benjamin !`;
}

// CONTEXTE PAR PAGE (sans API — instantané)
let lastPageContext='';
function showPageContext(page){
  if(!currentUser||currentUser.id!=='benjamin')return;
  const mem=getMem();
  let msg='';
  if(page==='sav'){
    const open=(mem.sav||[]).filter(s=>!s._deleted&&s.statut!=='regle');
    const urgent=open.filter(s=>s.urgent);
    const old=open.filter(s=>getSAVAgeDays(s)>=5);
    const parts=[];
    if(urgent.length>0)parts.push(`🚨 **${urgent.length} SAV urgent(s)**`);
    if(old.length>0)parts.push(`⏳ **${old.length} SAV** sans réponse depuis +5 jours`);
    if(parts.length>0)msg=parts.join(' · ');
  }
  if(page==='messages'){
    const convs=getConvsForUser('benjamin');
    const lrBen=getLastRead('benjamin');
    let unread=0;for(const cid of Object.keys(convs))unread+=countUnread('benjamin',cid,mem,lrBen);
    if(unread>0)msg=`💬 **${unread} message(s) non lu(s)**`;
  }
  if(msg&&msg!==lastPageContext){
    lastPageContext=msg;
    setTimeout(()=>addAIMsg(msg),200);
  }
}

// SUGGESTION EMAIL FOURNISSEUR
function suggestFournisseurEmail(savId,client,probleme,fournisseur){
  const prompt=`Rédige un email professionnel et concis à envoyer au fournisseur ${fournisseur} concernant le SAV du client ${client} (problème : ${probleme}). Email de relance ou réclamation selon le contexte. Commence directement par l'objet suggéré puis le corps de l'email.`;
  showPage('benai');
  setTimeout(()=>sendChat(prompt),300);
}

// HELPERS
// ══════════════════════════════════════════
function restoreChatVisual(){
  // Affiche les 6 derniers échanges du chat si l'historique existe
  if(chatHistory.length===0)return;
  const recent=chatHistory.slice(-6);
  recent.forEach(m=>{
    if(m.role==='user')addUserMsg(m.content);
    else if(m.role==='assistant')addAIMsg(m.content);
  });
}

let pollingInterval=null;
let lastAutoBugScanTs=0;
let lastPollingHeartbeat=Date.now();
let lastCloudRefreshTs=0;
let cloudRefreshInFlight=false;
const CLOUD_REFRESH_MIN_INTERVAL_MS=7000;
const POLLING_INTERVAL_MS=10000;

async function refreshCoreDataFromCloudIfNeeded(force=false){
  if(!SUPABASE_CONFIG.enabled||!currentUser)return;
  if(cloudRefreshInFlight)return;
  const now=Date.now();
  if(!force&&now-lastCloudRefreshTs<CLOUD_REFRESH_MIN_INTERVAL_MS)return;
  cloudRefreshInFlight=true;
  lastCloudRefreshTs=now;
  let ok=false;
  let appStorageOk=false;
  try{
    appStorageOk=await hydrateAppStorageFromSupabase(currentUser?.id,true);
    ok=await loadCoreDataFromSupabase();
    if(ok||appStorageOk)refreshVisibleDataAfterSupabaseSync();
  }catch(e){
    // silencieux: la surveillance de sync gère déjà les erreurs.
  }finally{
    cloudRefreshInFlight=false;
  }
  return ok||appStorageOk;
}
function scheduleRealtimeCloudRefresh(reason='realtime'){
  if(supabaseRealtimeDebounceTimer)clearTimeout(supabaseRealtimeDebounceTimer);
  supabaseRealtimeDebounceTimer=setTimeout(()=>{
    supabaseRealtimeDebounceTimer=null;
    void refreshCoreDataFromCloudIfNeeded(true);
  },200);
}
function stopSupabaseRealtimeSync(){
  if(supabaseRealtimeDebounceTimer){
    clearTimeout(supabaseRealtimeDebounceTimer);
    supabaseRealtimeDebounceTimer=null;
  }
  supabaseRealtimeReady=false;
  const client=getSupabaseClient();
  if(client&&supabaseRealtimeChannel){
    try{client.removeChannel(supabaseRealtimeChannel);}catch(e){}
  }
  supabaseRealtimeChannel=null;
  renderSupabaseRuntimeStatus();
}
async function startSupabaseRealtimeSync(){
  stopSupabaseRealtimeSync();
  if(!SUPABASE_CONFIG.enabled||!SUPABASE_CONFIG.url)return false;
  const client=getSupabaseClient();
  if(!client)return false;
  await ensureSupabaseSession();
  const channelName=`benai-rt-${normalizeId(currentUser?.id||'user')}-${Date.now()}`;
  let channel=client.channel(channelName);
  SUPABASE_TABLES.forEach(table=>{
    channel=channel.on('postgres_changes',{
      event:'*',
      schema:'public',
      table
    },payload=>{
      if(!currentUser)return;
      if(table==='app_settings'){
        const changedKey=String(payload?.new?.key||payload?.old?.key||'');
        if(changedKey!==SHARED_CORE_DATA_KEY&&changedKey!==SHARED_AI_SETTING_KEY)return;
      }
      scheduleRealtimeCloudRefresh(`${table}:${payload?.eventType||'change'}`);
    });
  });
  channel.subscribe(status=>{
    supabaseRealtimeReady=(status==='SUBSCRIBED');
    renderSupabaseRuntimeStatus();
  });
  supabaseRealtimeChannel=channel;
  return true;
}

function startPolling(){
  if(pollingInterval)clearInterval(pollingInterval);
  let lastMsgCount=0;
  pollingInterval=setInterval(()=>{
    if(!currentUser)return;
    lastPollingHeartbeat=Date.now();
    const prevBadge=parseInt(document.getElementById('msg-badge')?.textContent||'0');
    refreshMsgBadge();
    const newBadge=parseInt(document.getElementById('msg-badge')?.textContent||'0');
    if(newBadge>prevBadge)playNotifSound();
    refreshSAVBadge();
    refreshLeadsBadge();
    refreshBugsBadge();
    checkRappelsLeads();
    checkLeadSmartNotifications();
    runAutoBugDetectors();
    void refreshCoreDataFromCloudIfNeeded();
    void processPendingUserDeletes();
    void processPendingUserCreates();
    if(supabaseSyncFailStreak>0){
      void flushSupabaseSyncNow();
    }
    if(Date.now()%30000<POLLING_INTERVAL_MS){
      renderSupabaseRuntimeStatus();
    }
    if(currentConv){
      const area=document.getElementById('thread-msgs');
      const mem=getMem();
      const count=(mem.messages[currentConv]||[]).length;
      const readSig=JSON.stringify(mem.msg_read_cursor?.[currentConv]||{});
      if(area&&area.dataset.count&&parseInt(area.dataset.count)!==count){
        renderThread(currentConv);scheduleRenderConvList();
        if(count>parseInt(area.dataset.count||'0'))playNotifSound();
      }else if(area&&document.getElementById('page-messages')?.style.display==='flex'&&area.dataset.readSig!==readSig){
        renderThread(currentConv);
      }
      if(area){
        area.dataset.count=count;
        area.dataset.readSig=readSig;
      }
    }
  },POLLING_INTERVAL_MS);
}

// ══════════════════════════════════════════
// ☁️ SAUVEGARDE GOOGLE DRIVE (Google Identity Services)
// ══════════════════════════════════════════
const GDRIVE_SCOPE='https://www.googleapis.com/auth/drive.file';
const GDRIVE_BACKUP_FILENAME='BenAI_backup.json';
let gdriveTokenClient=null;
let gdriveToken=null;

function getGDriveClientId(){return appStorage.getItem('benai_gdrive_client_id')||'';}
function saveGDriveClientId(id){appStorage.setItem('benai_gdrive_client_id',id);}

function initGDriveClient(){
  const clientId=getGDriveClientId();
  if(!clientId||!window.google?.accounts?.oauth2)return;
  gdriveTokenClient=google.accounts.oauth2.initTokenClient({
    client_id:clientId,
    scope:GDRIVE_SCOPE,
    callback:(resp)=>{
      if(resp.error){showDriveNotif('❌ Erreur Google Drive : '+resp.error);return;}
      gdriveToken=resp.access_token;
      uploadToDrive(gdriveToken).then(ok=>{
        if(ok){
          showDriveNotif('✅ Sauvegarde Google Drive effectuée');
          appStorage.setItem('benai_gdrive_last_backup',new Date().toISOString());
          const el=document.getElementById('gdrive-last-backup');
          if(el)el.textContent='Dernière sauvegarde : '+new Date().toLocaleString('fr-FR');
        } else {
          showDriveNotif('❌ Échec sauvegarde Drive — vérifie le Client ID');
        }
      });
    }
  });
}

async function autoBackupToDrive(){
  const clientId=getGDriveClientId();
  if(!clientId){
    const seen=appStorage.getItem('benai_gdrive_prompt_seen');
    if(!seen){
      appStorage.setItem('benai_gdrive_prompt_seen','1');
      setTimeout(()=>addAIMsg('💾 **Sauvegarde Google Drive non configurée**\n\nVa dans ⚙️ Paramètres → section Google Drive pour entrer ton Client ID et activer la sauvegarde automatique.'),2500);
    }
    return;
  }
  if(!window.google?.accounts?.oauth2){
    // Google Identity Services pas encore chargé — attendre
    setTimeout(()=>autoBackupToDrive(),1500);
    return;
  }
  if(!gdriveTokenClient)initGDriveClient();
  if(!gdriveTokenClient)return;
  // Demander token silencieusement (prompt:'none' = sans popup si déjà autorisé)
  gdriveTokenClient.requestAccessToken({prompt:''});
}

async function manualBackupToDrive(){
  const clientId=getGDriveClientId();
  if(!clientId){alert('⚠️ Configure ton Client ID Google Drive dans les paramètres.');return;}
  if(!window.google?.accounts?.oauth2){alert('⚠️ Google Identity Services non chargé. Vérifie ta connexion internet.');return;}
  if(!gdriveTokenClient)initGDriveClient();
  if(!gdriveTokenClient)return;
  // Forcer la popup d'autorisation
  gdriveTokenClient.requestAccessToken({prompt:'consent'});
}

async function uploadToDrive(token){
  try{
    const mem=getMem();const pwds=getPwds();const extras=getExtraUsers();const access=getAccess();const ann=getAnnuaire();
    const backup={version:BENAI_VERSION,date:new Date().toISOString(),data:mem,pwds,extras,access,annuaire:ann};
    const content=JSON.stringify(backup,null,2);
    const FOLDER_NAME='BenAI Sauvegardes';

    // 1 — Chercher ou créer le dossier
    const folderSearch=await fetch(`https://www.googleapis.com/drive/v3/files?q=name%3D'${encodeURIComponent(FOLDER_NAME)}'+and+mimeType%3D'application%2Fvnd.google-apps.folder'+and+trashed%3Dfalse&fields=files(id,name)`,{
      headers:{Authorization:'Bearer '+token}
    });
    if(!folderSearch.ok)return false;
    const {files:folders=[]}=await folderSearch.json();
    let folderId=folders[0]?.id||null;

    if(!folderId){
      // Créer le dossier
      const folderRes=await fetch('https://www.googleapis.com/drive/v3/files',{
        method:'POST',
        headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},
        body:JSON.stringify({name:FOLDER_NAME,mimeType:'application/vnd.google-apps.folder'})
      });
      if(!folderRes.ok)return false;
      const folderData=await folderRes.json();
      folderId=folderData.id;
    }

    // 2 — Chercher si le fichier existe déjà dans ce dossier
    const fileSearch=await fetch(`https://www.googleapis.com/drive/v3/files?q=name%3D'${GDRIVE_BACKUP_FILENAME}'+and+'${folderId}'+in+parents+and+trashed%3Dfalse&fields=files(id)`,{
      headers:{Authorization:'Bearer '+token}
    });
    if(!fileSearch.ok)return false;
    const {files=[]}=await fileSearch.json();

    if(files[0]?.id){
      // Mettre à jour le fichier existant
      const res=await fetch(`https://www.googleapis.com/upload/drive/v3/files/${files[0].id}?uploadType=media`,{
        method:'PATCH',
        headers:{Authorization:'Bearer '+token,'Content-Type':'application/json'},
        body:content
      });
      return res.ok;
    } else {
      // Créer le fichier dans le dossier
      const meta=new Blob([JSON.stringify({name:GDRIVE_BACKUP_FILENAME,mimeType:'application/json',parents:[folderId]})],{type:'application/json'});
      const blob=new Blob([content],{type:'application/json'});
      const form=new FormData();form.append('metadata',meta);form.append('file',blob);
      const res=await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart',{
        method:'POST',headers:{Authorization:'Bearer '+token},body:form
      });
      return res.ok;
    }
  }catch(e){return false;}
}

function showDriveNotif(msg){
  const notif=document.createElement('div');
  notif.style.cssText='position:fixed;bottom:16px;left:50%;transform:translateX(-50%);background:var(--s2);border:1px solid var(--g);border-radius:10px;padding:10px 18px;font-size:12px;font-weight:600;color:var(--g);z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,.4);transition:opacity .3s';
  notif.textContent=msg;
  document.body.appendChild(notif);
  setTimeout(()=>{notif.style.opacity='0';setTimeout(()=>notif.remove(),300);},3000);
}

function isStandaloneMode(){
  return window.matchMedia('(display-mode: standalone)').matches||window.navigator.standalone===true;
}

/** Indicatif au moment de la connexion (pas une preuve d’installation). */
function detectBenAIClientSurface(){
  const ua=(navigator.userAgent||'').toLowerCase();
  const uaMob=/iphone|ipad|ipod|android|webos|blackberry|iemobile|opera mini|mobile/i.test(ua);
  let narrow=false;
  try{narrow=window.matchMedia('(max-width:768px)').matches;}catch{}
  const mobile=!!(uaMob||narrow);
  const pwa=!!isStandaloneMode();
  return{mobile,pwa};
}

function canUseBenAIMobileApp(role=currentUser?.role){
  return role==='commercial'||role==='directeur_co';
}

function makePngIcon(size){
  const c=document.createElement('canvas');
  c.width=size;c.height=size;
  const ctx=c.getContext('2d');
  ctx.fillStyle='#E8943A';
  const r=Math.round(size*0.17);
  ctx.beginPath();
  ctx.moveTo(r,0);ctx.lineTo(size-r,0);ctx.quadraticCurveTo(size,0,size,r);
  ctx.lineTo(size,size-r);ctx.quadraticCurveTo(size,size,size-r,size);
  ctx.lineTo(r,size);ctx.quadraticCurveTo(0,size,0,size-r);
  ctx.lineTo(0,r);ctx.quadraticCurveTo(0,0,r,0);ctx.fill();
  ctx.fillStyle='#fff';
  ctx.font=`${Math.floor(size*0.62)}px Outfit, Arial, sans-serif`;
  ctx.textAlign='center';ctx.textBaseline='middle';
  ctx.fillText('B',size/2,size/2+size*0.06);
  return c.toDataURL('image/png');
}

function initManifestAndIcons(){
  const icon192=makePngIcon(192);
  const icon512=makePngIcon(512);
  const manifest={
    name:'BenAI',
    short_name:'BenAI',
    start_url:'./',
    scope:'./',
    display:'standalone',
    background_color:'#080808',
    theme_color:'#E8943A',
    icons:[
      {src:icon192,sizes:'192x192',type:'image/png',purpose:'any maskable'},
      {src:icon512,sizes:'512x512',type:'image/png',purpose:'any maskable'}
    ]
  };
  const mfUrl=URL.createObjectURL(new Blob([JSON.stringify(manifest)],{type:'application/manifest+json'}));
  const mf=document.getElementById('benai-manifest');
  if(mf)mf.href=mfUrl;
  let apple=document.querySelector('link[rel="apple-touch-icon"]');
  if(!apple){
    apple=document.createElement('link');
    apple.rel='apple-touch-icon';
    document.head.appendChild(apple);
  }
  apple.href=makePngIcon(180);
}

async function registerBenAISW(){
  if(!('serviceWorker' in navigator))return;
  try{
    const swCode=`
      const CACHE_NAME='benai-shell-v3';
      self.addEventListener('install',e=>{
        e.waitUntil(caches.open(CACHE_NAME).then(cache=>cache.addAll(['./'])));
        self.skipWaiting();
      });
      self.addEventListener('activate',e=>{
        e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE_NAME).map(k=>caches.delete(k)))));
        self.clients.claim();
      });
      self.addEventListener('fetch',e=>{
        if(e.request.method!=='GET')return;
        e.respondWith(
          fetch(e.request).then(res=>{
            const copy=res.clone();
            caches.open(CACHE_NAME).then(cache=>cache.put(e.request,copy)).catch(()=>{});
            return res;
          }).catch(()=>caches.match(e.request).then(r=>r||caches.match('./')))
        );
      });
    `;
    const swUrl=URL.createObjectURL(new Blob([swCode],{type:'application/javascript'}));
    await navigator.serviceWorker.register(swUrl,{scope:'./'});
  }catch(e){}
}

function refreshInstallButton(){
  const btn=document.getElementById('btn-install-app');
  if(!btn)return;
  if(!canUseBenAIMobileApp()){btn.style.display='none';return;}
  if(isStandaloneMode()){btn.style.display='none';return;}
  const isIOS=/iphone|ipad|ipod/i.test(navigator.userAgent);
  if(deferredInstallPrompt||isIOS)btn.style.display='inline-flex';
}

async function installBenAI(){
  if(!canUseBenAIMobileApp()){
    showDriveNotif('ℹ️ Installation mobile réservée aux profils commercial et directeur co.');
    return;
  }
  if(isStandaloneMode())return;
  if(deferredInstallPrompt){
    deferredInstallPrompt.prompt();
    try{await deferredInstallPrompt.userChoice;}catch(e){}
    deferredInstallPrompt=null;
    refreshInstallButton();
    return;
  }
  const isIOS=/iphone|ipad|ipod/i.test(navigator.userAgent);
  if(isIOS){
    showDriveNotif('iPhone: Safari → Partager → Ajouter à l’écran d’accueil');
  }else{
    showDriveNotif('Installation non disponible pour le moment');
  }
}

// ══════════════════════════════════════════
// 📋 CRM LEADS — LOGIQUE COMPLÈTE v2
// ══════════════════════════════════════════

const CRM_ROLES=['directeur_co','directeur_general','commercial'];
const CRM_PAGES_ONLY=['directeur_co','directeur_general','commercial'];
const ROLE_PAGES={
  admin:['benai','notes','messages','sav','leads','absences','annuaire','paie','admin','evolution','guide','bugs'],
  assistante:['benai','notes','messages','sav','leads','absences','evolution','guide'],
  metreur:['benai','notes','messages','absences','evolution','guide'],
  directeur_co:['benai','notes','messages','sav','leads','absences','evolution','guide'],
  directeur_general:['benai','notes','messages','sav','leads','absences','evolution','guide'],
  commercial:['benai','notes','messages','sav','leads','absences','evolution','guide']
};
/** Même périmètre CRM / pilotage (société, filtres, onglets) que le dir. commercial. */
function isCRMScopePilotageRole(role){
  return role==='directeur_co'||role==='directeur_general';
}

function getLeads(){
  if(!Array.isArray(runtimeLeadsState))runtimeLeadsState=[];
  return runtimeLeadsState;
}
function saveLeads(l,sync=true){
  runtimeLeadsState=Array.isArray(l)?l:[];
  // Persistance session (temporaire) pour survivre à un refresh.
  persistRuntimeToSession(currentUser?.id);
  appStorage.removeItem('benai_leads');
  if(sync){
    markSharedDirtyIfChanged('leads',runtimeLeadsState);
    scheduleSupabaseSync(getMem(),getAnnuaire());
  }
}
function getLeadObjectifs(){try{return JSON.parse(appStorage.getItem('benai_lead_obj'))||{nimes:{leads:20,ca:50000},avignon:{leads:15,ca:40000}};}catch{return{nimes:{leads:20,ca:50000},avignon:{leads:15,ca:40000}};}}
function saveLeadObjectifs(o){appStorage.setItem('benai_lead_obj',JSON.stringify(o));}
function canAccessLeadByCompany(lead){
  if(!currentUser||!lead)return false;
  if(currentUser.role==='admin')return true;
  if(isCRMScopePilotageRole(currentUser.role)){
    const leadSoc=lead.societe_crm||'nemausus';
    return currentUser.societe==='les-deux'||currentUser.societe===leadSoc;
  }
  if(currentUser.role==='commercial'){
    return normalizeId(lead.commercial)===normalizeId(currentUser.id);
  }
  if(currentUser.role==='assistante'){
    return lead.cree_par===currentUser.id||lead.cree_par===currentUser.name;
  }
  return false;
}
function getCompanyScopedLeads(list){
  const leads=(list||getLeads()).filter(l=>l&&!l._deleted);
  if(!currentUser)return leads;
  if(!isCRMScopePilotageRole(currentUser.role))return leads;
  return leads.filter(canAccessLeadByCompany);
}
/** Dir. co qui voient ce lead dans le CRM (même règle que canAccessLeadByCompany côté dir.). */
function getDirecteursConcernedByLead(lead){
  if(!lead)return[];
  const leadSoc=lead.societe_crm||'nemausus';
  return getAllUsers().filter(u=>u.role==='directeur_co'&&(u.societe==='les-deux'||u.societe===leadSoc));
}

let currentLeadFilter='tous';
let currentLeadId=null;
let currentLeadStatut='gris';
let currentLeadSource='MAG';
let currentCRMView='list';

function getRdvDayKey(entry){
  if(!entry)return '';
  const raw=String(entry.date||entry.ts||'').trim();
  if(!raw)return '';
  if(/^\d{4}-\d{2}-\d{2}/.test(raw))return raw.slice(0,10);
  const d=new Date(raw);
  if(isNaN(d))return '';
  return d.toISOString().slice(0,10);
}
function getUniqueRdvDays(lead){
  const entries=Array.isArray(lead?.rdv_history)?lead.rdv_history:[];
  const days=new Set();
  entries.forEach(e=>{
    const key=getRdvDayKey(e);
    if(key)days.add(key);
  });
  if(days.size===0&&lead?.date_rdv_fait){
    const fallback=getRdvDayKey({date:lead.date_rdv_fait});
    if(fallback)days.add(fallback);
  }
  return days;
}
function normalizeLeadPhoneForIdentity(value){
  return String(value||'').replace(/\D+/g,'').trim();
}
function getLeadClientIdentityKey(lead){
  if(!lead)return '';
  const strict=getLeadIdentityKey(lead.nom,lead.adresse,lead.cp);
  if(strict)return strict;
  const nameKey=normalizeLeadIdentityText(lead.nom);
  const phoneKey=normalizeLeadPhoneForIdentity(lead.telephone);
  if(nameKey&&phoneKey)return `${nameKey}::tel::${phoneKey}`;
  return '';
}
function getRelatedClientLeads(targetLead,list){
  if(!targetLead)return [];
  const leads=Array.isArray(list)?list:getLeads();
  const key=getLeadClientIdentityKey(targetLead);
  if(!key)return [targetLead];
  return leads.filter(l=>!l._deleted&&getLeadClientIdentityKey(l)===key);
}
function getClientUniqueRdvDays(targetLead,list){
  const days=new Set();
  const related=getRelatedClientLeads(targetLead,list);
  related.forEach(lead=>{
    getUniqueRdvDays(lead).forEach(day=>days.add(day));
  });
  return days;
}
function getClientRdvDoneCount(targetLead,list){
  if(!targetLead)return 0;
  return getClientUniqueRdvDays(targetLead,list).size;
}

function getLeadRdvDoneCount(lead){
  if(!lead)return 0;
  return getUniqueRdvDays(lead).size;
}
function getLeadRdvDoneMonthCount(lead,refDate=new Date()){
  if(!lead)return 0;
  const entries=Array.isArray(lead.rdv_history)?lead.rdv_history:[];
  if(!entries.length&& !lead.date_rdv_fait)return 0;
  const m=refDate.getMonth();
  const y=refDate.getFullYear();
  const uniqueDays=getUniqueRdvDays(lead);
  let count=0;
  uniqueDays.forEach(day=>{
    const d=new Date(day+'T00:00:00');
    if(!isNaN(d)&&d.getMonth()===m&&d.getFullYear()===y)count++;
  });
  return count;
}
function registerLeadRdvDone(lead,source='manuel',dateISO=''){
  if(!lead)return false;
  if(!lead.rdv_history)lead.rdv_history=[];
  const iso=dateISO||new Date().toISOString();
  const keyDate=getRdvDayKey({date:iso});
  const already=getClientUniqueRdvDays(lead).has(keyDate);
  if(already)return false;
  lead.rdv_history.push({date:iso,source,user:currentUser?.name||'BenAI'});
  return true;
}

// INIT PAGE LEADS
function initLeadsPage(){
  const role=currentUser?.role;
  const tabs=document.getElementById('crm-tabs');
  const filters=document.getElementById('crm-filters');
  const btnNew=document.getElementById('btn-new-lead');
  const filterComm=document.getElementById('crm-filter-commercial');
  const filterSecteur=document.getElementById('crm-filter-secteur');
  const filterSociete=document.getElementById('crm-filter-societe');

  // Bouton nouveau lead — assistante et commercial uniquement (pas directeur co)
  btnNew.style.display=(role==='assistante'||role==='admin'||role==='commercial')?'flex':'none';

  // Source ACTIF visible pour commerciaux uniquement
  const srcActif=document.getElementById('src-actif-btn');
  if(srcActif)srcActif.style.display=role==='commercial'?'flex':'none';

  // Filtres globaux réservés aux profils pilotage (admin / dir. co / dir. général).
  const canUseGlobalScopeFilters=(role==='admin'||isCRMScopePilotageRole(role));
  if(filterSecteur){
    filterSecteur.style.display=canUseGlobalScopeFilters?'inline-flex':'none';
    if(!canUseGlobalScopeFilters)filterSecteur.value='';
  }
  if(filterSociete){
    filterSociete.style.display=canUseGlobalScopeFilters?'inline-flex':'none';
    if(!canUseGlobalScopeFilters)filterSociete.value='';
  }
  if(filterComm){
    filterComm.style.display=canUseGlobalScopeFilters?'block':'none';
    if(!canUseGlobalScopeFilters)filterComm.value='';
  }

  tabs.innerHTML='';
  if(role==='admin'||isCRMScopePilotageRole(role)){
    addCRMTab('non-attribues','⚠️ À attribuer',true);
    addCRMTab('mes-leads','📋 Tous les leads',false);
    addCRMTab('dashboard','📊 Dashboard',false);
    fillCommercialFilter();
    filters.style.display='flex';
    // Dir. co / dir. général arrivent sur le dashboard
    showCRMTab(isCRMScopePilotageRole(role)?'dashboard':'non-attribues');
  } else if(role==='assistante'){
    // Assistante : onglet unique — saisie + ses leads
    addCRMTab('mes-leads','📋 Mes leads',true);
    filters.style.display='none'; // Pas de filtres pour l'assistante
    showCRMTab('mes-leads');
  } else {
    addCRMTab('mes-leads','📋 Mes leads',true);
    filters.style.display='flex';
    showCRMTab('mes-leads');
  }
  refreshLeadsBadge();
  checkLeadsAlertes();
  checkRappelsLeads();
}

function addCRMTab(id,label,active){
  const tabs=document.getElementById('crm-tabs');
  const btn=document.createElement('button');
  btn.className='crm-tab'+(active?' active':'');
  btn.textContent=label;
  btn.onclick=()=>showCRMTab(id);
  btn.id='crm-tab-'+id;
  tabs.appendChild(btn);
}

function showCRMTab(id){
  document.querySelectorAll('.crm-tab').forEach(t=>t.classList.remove('active'));
  const tab=document.getElementById('crm-tab-'+id);
  if(tab)tab.classList.add('active');
  const filters=document.getElementById('crm-filters');
  const list=document.getElementById('leads-list');
  const kb=document.getElementById('leads-kanban');
  if(id==='dashboard'){
    filters.style.display='none';
    if(list)list.style.display='none';
    if(kb)kb.style.display='none';
    renderLeadsDashboard();
  } else if(id==='non-attribues'){
    filters.style.display='none';
    if(list){list.style.display='flex';list.style.flexDirection='column';}
    if(kb)kb.style.display='none';
    renderNonAttribues();
  } else {
    const role=currentUser?.role;
    // Assistante ne voit pas les filtres
    filters.style.display=role==='assistante'?'none':'flex';
    setCRMView(currentCRMView);
    renderLeads();
  }
}

function isLeadInNonAttribQueue(l){
  return !!l&&!l.archive&&!l.commercial&&l.statut==='gris';
}

function renderNonAttribues(){
  const list=document.getElementById('leads-list');if(!list)return;
  const leads=getCompanyScopedLeads(getLeads()).filter(isLeadInNonAttribQueue);
  if(!leads.length){
    list.innerHTML='<div style="color:var(--g);font-size:14px;padding:32px;text-align:center;font-weight:600">✅ Tous les leads sont attribués !</div>';
    return;
  }
  list.innerHTML='<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--r);margin-bottom:10px;padding:0 2px">'+leads.length+' lead(s) en attente d\'attribution</div>'+leads.map(l=>renderDispatchCard(l)).join('');
}

function setCRMView(view){
  currentCRMView=view;
  const list=document.getElementById('leads-list');
  const kanban=document.getElementById('leads-kanban');
  const btnL=document.getElementById('btn-view-list');
  const btnK=document.getElementById('btn-view-kanban');
  if(view==='list'){
    if(list)list.style.display='flex';
    if(kanban)kanban.style.display='none';
    if(btnL){btnL.style.background='var(--a3)';btnL.style.borderColor='var(--a)';}
    if(btnK){btnK.style.background='var(--s2)';btnK.style.borderColor='var(--b1)';}
  } else {
    if(list)list.style.display='none';
    if(kanban){kanban.style.display='flex';}
    if(btnL){btnL.style.background='var(--s2)';btnL.style.borderColor='var(--b1)';}
    if(btnK){btnK.style.background='var(--a3)';btnK.style.borderColor='var(--a)';}
  }
  renderLeads();
}

function filterLeads(f,btn){
  currentLeadFilter=f;
  document.querySelectorAll('.crm-filter').forEach(b=>b.classList.remove('active'));
  if(btn)btn.classList.add('active');
  renderLeads();
}

// Statuts renommés
const STATUT_LABELS={gris:'🔵 Non traité',rdv:'📞 RDV pris',jaune:'🟡 Devis envoyé',vert:'🟢 Vendu',rouge:'🔴 Perdu'};
const STATUT_CLS={gris:'ls-gris',rdv:'ls-rdv',jaune:'ls-jaune',vert:'ls-vert',rouge:'ls-rouge'};

function getFilteredLeads(){
  const role=currentUser?.role;
  const search=(document.getElementById('crm-search')?.value||'').toLowerCase();
  const hasGlobalScopeFilters=(role==='admin'||isCRMScopePilotageRole(role));
  const secteurFilter=hasGlobalScopeFilters?(document.getElementById('crm-filter-secteur')?.value||''):'';
  const societeFilter=hasGlobalScopeFilters?(document.getElementById('crm-filter-societe')?.value||''):'';
  const commFilter=hasGlobalScopeFilters?(document.getElementById('crm-filter-commercial')?.value||''):'';
  let leads=getLeads().filter(l=>!l._deleted);

  // RÈGLES STRICTES PAR RÔLE
  if(role==='commercial'){
    // Commercial voit UNIQUEMENT ses leads assignés
    leads=leads.filter(l=>l.commercial===currentUser.id);
  } else if(role==='assistante'){
    // Assistante voit uniquement les leads qu'elle a créés
    leads=leads.filter(l=>l.cree_par===currentUser.id||l.cree_par===currentUser.name);
  } else if(isCRMScopePilotageRole(role)){
    leads=getCompanyScopedLeads(leads);
  }
  // Admin et pilotage CRM voient tout le périmètre société — aucun filtre « mes leads »

  // Archive
  if(currentLeadFilter!=='archive')leads=leads.filter(l=>!l.archive);
  // Filtres UI
  if(secteurFilter)leads=leads.filter(l=>l.secteur===secteurFilter);
  if(societeFilter)leads=leads.filter(l=>(l.societe_crm||'nemausus')===societeFilter);
  if(commFilter)leads=leads.filter(l=>l.commercial===commFilter);
  if(currentLeadFilter==='gris')leads=leads.filter(l=>l.statut==='gris');
  else if(currentLeadFilter==='rdv')leads=leads.filter(l=>l.statut==='rdv');
  else if(currentLeadFilter==='jaune')leads=leads.filter(l=>l.statut==='jaune');
  else if(currentLeadFilter==='vert')leads=leads.filter(l=>l.statut==='vert');
  else if(currentLeadFilter==='rouge')leads=leads.filter(l=>l.statut==='rouge');
  else if(currentLeadFilter==='alerte')leads=leads.filter(l=>isLeadAlerte(l));
  else if(currentLeadFilter==='archive')leads=leads.filter(l=>l.archive);
  // Recherche
  if(search)leads=leads.filter(l=>
    (l.nom||'').toLowerCase().includes(search)||
    (l.telephone||'').includes(search)||
    (l.ville||'').toLowerCase().includes(search)||
    (l.type_projet||'').toLowerCase().includes(search)
  );
  leads.sort((a,b)=>{
    const aa=isLeadAlerte(a)?1:0,bb=isLeadAlerte(b)?1:0;
    if(aa!==bb)return bb-aa;
    return new Date(b.date_creation||0)-new Date(a.date_creation||0);
  });
  return leads;
}

function renderLeads(){
  if(currentCRMView==='kanban'){renderKanban();return;}
  const role=currentUser?.role;
  const list=document.getElementById('leads-list');if(!list)return;

  // VUE DIRECTEUR CO — deux sections
  if(isCRMScopePilotageRole(role)||role==='admin'){
    const allLeads=getCompanyScopedLeads(getLeads()).filter(l=>!l.archive);
    const nonAttrib=allLeads.filter(isLeadInNonAttribQueue);
    const attrib=getFilteredLeads().filter(l=>l.commercial||l.statut!=='gris');
    let html='';
    // Section À ATTRIBUER
    if(nonAttrib.length>0){
      html+=`<div style="margin-bottom:14px">
        <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--r);margin-bottom:8px;display:flex;align-items:center;gap:6px">
          ⚠️ NON ATTRIBUÉS <span style="background:var(--r);color:#fff;border-radius:10px;padding:1px 7px;font-size:10px">${nonAttrib.length}</span>
        </div>`;
      html+=nonAttrib.map(l=>renderDispatchCard(l)).join('');
      html+='</div><div style="border-top:1px solid var(--b1);margin-bottom:12px"></div>';
    }
    // Section LEADS EN COURS
    html+=`<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--t3);margin-bottom:8px">📋 Leads (${attrib.length})</div>`;
    html+=attrib.map(l=>renderLeadCard(l,role)).join('');
    list.innerHTML=html||'<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">Aucun lead</div>';
    return;
  }

  // VUE ASSISTANTE — ses leads uniquement, sans statuts, sans cheminement
  if(role==='assistante'){
    const leads=getFilteredLeads();
    if(!leads.length){list.innerHTML='<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">Aucun lead saisi pour le moment.<br><br>Cliquez sur <strong>+ Nouveau lead</strong> pour en créer un.</div>';return;}
    list.innerHTML=leads.map(l=>renderLeadCardAssistante(l)).join('');
    return;
  }

  // VUE COMMERCIAL — ses leads assignés
  const leads=getFilteredLeads();
  if(!leads.length){list.innerHTML='<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">Aucun lead assigné</div>';return;}
  list.innerHTML=leads.map(l=>renderLeadCard(l,role)).join('');
}

// CARTE DISPATCH (directeur co — attribution rapide)
function renderDispatchCard(l){
  const srcIcon=LEAD_SOURCE_ICONS[l.source]||'📋';
  const leadSoc=l.societe_crm||'nemausus';
  const commerciaux=getAllUsers().filter(u=>{
    if(!(u.role==='commercial'||u.role==='directeur_co'))return false;
    if(currentUser?.role==='admin')return true;
    return u.societe===leadSoc||u.societe==='les-deux';
  });
  const selectOptions=commerciaux.map(c=>`<option value="${c.id}">${esc(c.name)}</option>`).join('');
  return `<div class="lead-card" style="border-left:4px solid var(--r);background:rgba(248,113,113,.05)">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
      <span>${srcIcon}</span>
      <div class="lead-nom">${esc(l.nom)}</div>
      <span style="margin-left:auto;font-size:10px;color:var(--t3)">${getLeadAge(l)}</span>
    </div>
    <div class="lead-info">${esc(l.ville||'')}${l.cp?' ('+l.cp+')':''} · ${esc(l.type_projet||'')}</div>
    <div style="margin-top:8px;display:flex;gap:6px;align-items:center;flex-wrap:wrap">
      ${makeLeadCallLink(l.id,l.telephone)}
      ${makeGPSLink(l.adresse,l.ville,l.cp)}
      <select id="dispatch-${l.id}" class="form-input" style="flex:1;padding:6px 8px;font-size:12px;min-width:120px">
        <option value="">Choisir un commercial...</option>
        ${selectOptions}
      </select>
      <button onclick="dispatchLead(${l.id})" style="padding:6px 12px;background:var(--a);color:#fff;border:none;border-radius:7px;font-size:12px;font-weight:700;cursor:pointer;font-family:inherit;white-space:nowrap">✓ Attribuer</button>
      <button onclick="openLead(${l.id})" style="padding:6px 10px;background:var(--s3);border:1px solid var(--b1);border-radius:7px;font-size:12px;cursor:pointer;font-family:inherit">👁️</button>
    </div>
  </div>`;
}

// ATTRIBUTION RAPIDE
function dispatchLead(id){
  const sel=document.getElementById('dispatch-'+id);
  const commId=sel?.value;
  if(!commId){showDriveNotif('⚠️ Choisissez un commercial');return;}
  const leads=getLeads();
  const idx=leads.findIndex(l=>l.id===id);if(idx===-1)return;
  const l=leads[idx];
  if(!canAccessLeadByCompany(l)){showDriveNotif('🔒 Accès refusé pour cette société');return;}
  const comm=getAllUsers().find(u=>u.id===commId);
  if(!comm){showDriveNotif('⚠️ Commercial introuvable');return;}
  const leadSoc=l.societe_crm||'nemausus';
  if(currentUser?.role!=='admin'&&!(comm.societe===leadSoc||comm.societe==='les-deux')){
    showDriveNotif('⚠️ Ce commercial n’appartient pas à cette société');
    return;
  }
  const dateStr=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  leads[idx].commercial=commId;
  addLeadTimelineEntry(leads[idx],`Attribué à ${comm?.name||commId}`,currentUser.name);
  saveLeads(leads);
  renderLeads();
  refreshLeadsBadge();
  pushNotif('📋 Lead assigné',`${l.nom} — ${l.type_projet}`,LEAD_SOURCE_ICONS[l.source]||'📋',commId);
  logActivity(`${currentUser.name} a attribué ${l.nom} à ${comm?.name}`);
  showDriveNotif(`✅ ${l.nom} attribué à ${comm?.name}`);
}

// CARTE ASSISTANTE (sans statuts)
function renderLeadCardAssistante(l){
  const srcIcon=LEAD_SOURCE_ICONS[l.source]||'📋';
  const age=getLeadAge(l);
  const comm=getAllUsers().find(u=>u.id===l.commercial);
  // Infos RDV/rappel
  let rdvInfo='';
  if(l.rappel&&l.sous_statut==='rdv_programme'){
    const d=new Date(l.rappel);
    rdvInfo=`<div style="font-size:11px;color:var(--bl);font-weight:600;margin-top:4px;padding:4px 8px;background:var(--bl2);border-radius:6px">📅 RDV le ${d.toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'})}</div>`;
  } else if(l.rappel){
    const d=new Date(l.rappel);
    rdvInfo=`<div style="font-size:11px;color:var(--y);font-weight:600;margin-top:4px;padding:4px 8px;background:var(--y2);border-radius:6px">📞 Rappel le ${d.toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'})}</div>`;
  }
  return `<div class="lead-card statut-${l.statut||'gris'}" onclick="openLead(${l.id})">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px">
      <span>${srcIcon}</span>
      <div class="lead-nom">${esc(l.nom)}</div>
      <span style="margin-left:auto;font-size:10px;color:var(--t3)">${age}</span>
    </div>
    <div style="font-size:13px;font-weight:700;color:var(--a);margin-bottom:3px">${esc(l.type_projet||'—')}</div>
    <div class="lead-info">${esc(l.ville||'')}</div>
    <div class="lead-meta" style="margin-top:6px;gap:8px">
      ${comm
        ?`<span style="background:var(--a3);color:var(--a);padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600">👤 ${esc(comm.name)}</span>`
        :'<span style="background:var(--y2);color:var(--y);padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600">⏳ En attente d\'attribution</span>'}
      ${makeLeadCallLink(l.id,l.telephone)}
    </div>
    ${rdvInfo}
    ${l.commentaire?`<div style="font-size:11px;color:var(--t2);margin-top:4px;padding:4px 8px;background:var(--s3);border-radius:6px">💬 ${esc(l.commentaire)}</div>`:''}
  </div>`;
}

function getLeadSecteurLabel(secteur){
  if(secteur==='avignon')return 'Avignon';
  if(secteur==='bagnoles')return 'Bagnols-sur-Cèze';
  if(secteur==='zone_blanche')return 'Zone blanche';
  return 'Nîmes';
}

function renderLeadCard(l,role){
  const alerte=isLeadAlerte(l);
  const age=getLeadAge(l);
  const comm=getAllUsers().find(u=>u.id===l.commercial);
  const secteurLabel=getLeadSecteurLabel(l.secteur);
  const statLabel=STATUT_LABELS[l.statut]||'🔵 Non traité';
  const statCls=STATUT_CLS[l.statut]||'ls-gris';
  const srcIcon=LEAD_SOURCE_ICONS[l.source]||'📋';
  const montant=l.statut==='vert'&&l.prix_vendu?` · 💰 ${Number(l.prix_vendu).toLocaleString('fr-FR')} €`:l.statut==='jaune'&&l.montant_devis?` · 📋 ${Number(l.montant_devis).toLocaleString('fr-FR')} €`:'';
  const vuBy=(role==='admin'||isCRMScopePilotageRole(role));
  const vuInfo=vuBy?(l.vu_date?`<span class="lead-vu">👁️ ${l.vu_date}</span>`:`<span class="lead-non-vu">👁️ Jamais ouvert</span>`):'';
  const dernAction=l.timeline?.length?l.timeline[l.timeline.length-1]:null;
  const rdvDone=getLeadRdvDoneCount(l);
  const rdvClientDone=getClientRdvDoneCount(l);
  const dernContact=dernAction?`<div style="font-size:10px;color:var(--t2);margin-top:3px">🕐 ${dernAction.date} — ${esc(dernAction.action)}</div>`:'';
  return `<div class="lead-card statut-${l.statut}${alerte?' alerte24':''}${l.archive?';opacity:.6':''}" onclick="openLead(${l.id})" style="position:relative">
    ${alerte?'<div class="lead-alerte" style="position:absolute;top:10px;right:10px">⏰ Alerte</div>':''}
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;flex-wrap:wrap">
      <span style="font-size:14px">${srcIcon}</span>
      <div class="lead-nom">${esc(l.nom)}</div>
      <span style="margin-left:auto;font-size:10px;color:var(--t3)">${age}</span>
    </div>
    <div style="font-size:13px;font-weight:700;color:var(--a);margin-bottom:3px">${esc(l.type_projet||'—')}</div>
    <div class="lead-info">${esc(l.ville||'')}${l.cp?' ('+l.cp+')':''} ${montant}</div>
    <div class="lead-meta" style="margin-top:5px">
      <span class="lead-statut ${statCls}">${statLabel}</span>
      <span class="lead-secteur">${secteurLabel}</span>
      ${comm?`<span class="lead-commercial">${esc(comm.name)}</span>`:''}
      ${rdvClientDone>0?`<span style="background:var(--g2);color:var(--g);padding:2px 7px;border-radius:10px;font-size:10px;font-weight:700">📅 ${rdvClientDone} RDV client</span>`:''}
      ${rdvDone>0&&rdvDone!==rdvClientDone?`<span style="background:var(--s3);color:var(--t2);padding:2px 7px;border-radius:10px;font-size:10px">fiche: ${rdvDone}</span>`:''}
      ${vuInfo}
      <span style="margin-left:auto;display:flex;gap:4px" onclick="event.stopPropagation()">${makeLeadCallLink(l.id,l.telephone)}${makeGPSLink(l.adresse,l.ville,l.cp)}</span>
    </div>
    ${dernContact}
    ${l.suivi?`<div style="font-size:11px;color:var(--t2);margin-top:4px;padding:4px 8px;background:var(--s3);border-radius:6px">📌 ${esc(l.suivi)}</div>`:''}
  </div>`;
}

function renderKanban(){
  const kanban=document.getElementById('leads-kanban');if(!kanban)return;
  kanban.style.display='flex';
  const role=currentUser?.role;
  const leads=getFilteredLeads();
  const cols=[
    {id:'gris',label:'🔵 Non traité',cls:'ls-gris'},
    {id:'rdv',label:'📞 RDV pris',cls:'ls-rdv'},
    {id:'jaune',label:'🟡 Devis envoyé',cls:'ls-jaune'},
    {id:'vert',label:'🟢 Vendu',cls:'ls-vert'},
    {id:'rouge',label:'🔴 Perdu',cls:'ls-rouge'},
  ];
  kanban.innerHTML=cols.map(col=>{
    const colLeads=leads.filter(l=>l.statut===col.id);
    return `<div class="kanban-col">
      <div class="kanban-col-header">
        <span>${col.label}</span>
        <span style="background:var(--s3);border-radius:10px;padding:1px 7px;font-size:11px;font-weight:700">${colLeads.length}</span>
      </div>
      <div class="kanban-col-body">
        ${colLeads.map(l=>`
          <div class="kanban-card" onclick="openLead(${l.id})">
            <div class="kanban-card-nom">${esc(l.nom)}</div>
            <div class="kanban-card-info">${esc(l.ville||'')} · ${esc(l.type_projet||'')}</div>
            ${l.montant_devis||l.prix_vendu?`<div style="font-size:11px;color:var(--g);font-weight:600;margin-top:2px">${Number(l.prix_vendu||l.montant_devis||0).toLocaleString('fr-FR')} €</div>`:''}
          </div>`).join('')||'<div style="color:var(--t3);font-size:11px;padding:8px;text-align:center">Vide</div>'}
      </div>
    </div>`;
  }).join('');
}

function getLeadLostAnalytics(leads){
  const labels={
    prix:'Prix trop élevé',
    delai:'Délai trop long',
    concurrent:'Concurrent choisi',
    qualite_percue:'Qualité perçue insuffisante',
    injoignable_definitif:'Injoignable définitif',
    annule:'Projet annulé',
    autre:'Autre'
  };
  const lost=(leads||[]).filter(l=>l.statut==='rouge');
  const byReason={};
  lost.forEach(l=>{
    const key=l.raison_mort||'autre';
    byReason[key]=(byReason[key]||0)+1;
  });
  const rows=Object.entries(byReason).sort((a,b)=>b[1]-a[1]).map(([k,v])=>({key:k,label:labels[k]||k,count:v}));
  return{total:lost.length,rows};
}

function getCommercialRankingMonth(leads){
  const now=new Date();
  const month=now.getMonth(),year=now.getFullYear();
  const sales=(leads||[]).filter(l=>{
    if(l.statut!=='vert'||!l.commercial)return false;
    const d=new Date(l.date_creation||0);
    return d.getMonth()===month&&d.getFullYear()===year;
  });
  const map={};
  sales.forEach(l=>{
    if(!map[l.commercial])map[l.commercial]={uid:l.commercial,ventes:0,ca:0};
    map[l.commercial].ventes++;
    map[l.commercial].ca+=Number(l.prix_vendu||0);
  });
  return Object.values(map).sort((a,b)=>b.ca-a.ca||b.ventes-a.ventes);
}

function getArchivedLeadsStats(leads){
  const arch=(leads||[]).filter(l=>l.archive);
  const byStatut={vert:0,rouge:0,autre:0};
  arch.forEach(l=>{
    if(l.statut==='vert')byStatut.vert++;
    else if(l.statut==='rouge')byStatut.rouge++;
    else byStatut.autre++;
  });
  return{total:arch.length,byStatut};
}

// DASHBOARD
function renderLeadsDashboard(){
  const container=document.getElementById('leads-container');if(!container)return;
  const list=document.getElementById('leads-list');
  if(list){list.style.display='flex';}
  const leads=getCompanyScopedLeads(getLeads()).filter(l=>!l.archive);
  const obj=getLeadObjectifs();
  const now=new Date();
  const month=now.getMonth();const year=now.getFullYear();
  const leadsMonth=leads.filter(l=>{const d=new Date(l.date_creation||0);return d.getMonth()===month&&d.getFullYear()===year;});
  const secteurs=[
    {id:'nimes',label:'Nîmes',color:'var(--a)'},
    {id:'avignon',label:'Avignon',color:'var(--bl)'},
    {id:'bagnoles',label:'Bagnols-sur-Cèze',color:'#22c55e'},
    {id:'zone_blanche',label:'Zone blanche',color:'var(--r)'}
  ];
  let html='';
  // Stats globales
  const totalLeads=leads.length;
  const nonAttrib=leads.filter(l=>!l.commercial&&l.statut==='gris').length;
  const alertes=leads.filter(l=>isLeadAlerte(l)).length;
  const rdvMonthTotal=leads.reduce((sum,l)=>sum+getLeadRdvDoneMonthCount(l,now),0);
  html+=`<div class="secteur-card" style="padding:10px 14px">
    <div style="font-size:11px;color:var(--t3);font-weight:600;letter-spacing:.3px">STRUCTURE DASHBOARD</div>
    <div style="font-size:12px;color:var(--t2);margin-top:4px">1) Pilotage global · 2) Analyse ventes · 3) Performance commerciale · 4) Exports et suivi</div>
  </div>`;
  html+=`<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px">
    <div class="stat-box"><div class="stat-box-val">${totalLeads}</div><div class="stat-box-lbl">Total leads</div></div>
    <div class="stat-box"><div class="stat-box-val" style="color:var(--y)">${nonAttrib}</div><div class="stat-box-lbl">À attribuer</div></div>
    <div class="stat-box"><div class="stat-box-val" style="color:var(--r)">${alertes}</div><div class="stat-box-lbl">Alertes</div></div>
  </div>`;
  const secteursSynthese=secteurs.map(s=>{
    const all=leads.filter(l=>l.secteur===s.id);
    const ventes=all.filter(l=>l.statut==='vert');
    return{
      ...s,
      leads:all.length,
      ventes:ventes.length,
      ca:ventes.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0)
    };
  });
  html+=`<div style="font-size:11px;color:var(--t3);font-weight:700;margin:6px 0">1) PILOTAGE GLOBAL</div>`;
  html+=`<div class="secteur-card" id="crm-dashboard-secteurs-export">
    <div class="secteur-title" style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
      <span>🧭 Synthèse par secteur</span>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:0 0 10px">
      <label style="font-size:11px;color:var(--t3);display:flex;align-items:center;gap:6px;flex:1;min-width:200px">
        <span style="white-space:nowrap">Export :</span>
        <select id="crm-dash-export-secteur" class="form-input" style="flex:1;min-width:160px;padding:6px 10px;font-size:12px;border-radius:8px">
          <option value="all">Global — les 4 secteurs</option>
          <option value="core3">Global — les 3 secteurs (hors zone blanche)</option>
          <option value="nimes">Nîmes uniquement</option>
          <option value="avignon">Avignon uniquement</option>
          <option value="bagnoles">Bagnols-sur-Cèze uniquement</option>
          <option value="zone_blanche">Zone blanche uniquement</option>
        </select>
      </label>
      <button type="button" onclick="exportSecteurSyntheseFromDashboard()" title="CSV : même colonnes que l’export Performance secteurs" style="padding:8px 14px;background:var(--g2);color:var(--g);border:1px solid rgba(34,197,94,.4);border-radius:10px;font-family:inherit;font-size:12px;font-weight:700;cursor:pointer;white-space:nowrap">📥 Export CSV</button>
    </div>
    <div style="font-size:11px;color:var(--t3);margin:-4px 0 10px;line-height:1.45">Leads <strong>actifs non archivés</strong> du périmètre visible (société / rôle). Colonnes CSV : secteur, leads, ventes, CA signé, devis en cours, pipeline devis.</div>
    ${secteursSynthese.map(s=>`
      <div class="perf-row">
        <div class="perf-name" style="color:${s.color}">${s.label}</div>
        <div class="perf-stats">${s.leads} lead(s) · ${s.ventes} vente(s)</div>
        <div class="perf-ca">${s.ca.toLocaleString('fr-FR')} €</div>
      </div>
    `).join('')}
  </div>`;
  const salesTotal=leads.filter(l=>l.statut==='vert');
  const caTotal=salesTotal.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
  const salesMonth=leadsMonth.filter(l=>l.statut==='vert');
  const caMonth=salesMonth.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
  const panierMoyen=salesTotal.length?Math.round(caTotal/salesTotal.length):0;
  const txConv=totalLeads?Math.round((salesTotal.length/totalLeads)*100):0;
  html+=`<div style="font-size:11px;color:var(--t3);font-weight:700;margin:6px 0">2) ANALYSE VENTES</div>`;
  html+=`<div class="secteur-card">
    <div class="secteur-title">💼 Analyse ventes complète</div>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">
      <div class="stat-box"><div class="stat-box-val" style="color:var(--g)">${salesTotal.length}</div><div class="stat-box-lbl">Ventes signées</div></div>
      <div class="stat-box"><div class="stat-box-val" style="color:var(--g)">${caTotal.toLocaleString('fr-FR')} €</div><div class="stat-box-lbl">CA total signé</div></div>
      <div class="stat-box"><div class="stat-box-val">${panierMoyen.toLocaleString('fr-FR')} €</div><div class="stat-box-lbl">Panier moyen</div></div>
      <div class="stat-box"><div class="stat-box-val" style="color:var(--a)">${txConv}%</div><div class="stat-box-lbl">Taux conversion global</div></div>
    </div>
    <div style="margin-top:8px;font-size:11px;color:var(--t2)">Ce mois: ${salesMonth.length} vente(s) · ${caMonth.toLocaleString('fr-FR')} € signé(s).</div>
  </div>`;
  const rdvTop=leads.map(l=>({lead:l,count:getLeadRdvDoneMonthCount(l,now)})).filter(x=>x.count>0).sort((a,b)=>b.count-a.count).slice(0,5);
  html+=`<div class="secteur-card">
    <div class="secteur-title">📅 Pont Google Agenda — RDV réalisés (ce mois)</div>
    <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-bottom:8px">
      <div class="stat-box"><div class="stat-box-val" style="color:var(--bl)">${rdvMonthTotal}</div><div class="stat-box-lbl">RDV faits ce mois</div></div>
      <div class="stat-box"><div class="stat-box-val">${rdvTop.length}</div><div class="stat-box-lbl">Leads avec RDV</div></div>
    </div>
    ${rdvTop.length?rdvTop.map(x=>`<div class="perf-row"><div class="perf-name">${esc(x.lead.nom)}</div><div class="perf-stats">${x.count} RDV</div><div class="perf-ca">${esc(x.lead.type_projet||'')}</div></div>`).join(''):`<div style="font-size:12px;color:var(--t3)">Aucun RDV réalisé enregistré ce mois.</div>`}
  </div>`;
  // Par secteur
  html+=`<div style="font-size:11px;color:var(--t3);font-weight:700;margin:6px 0">3) PERFORMANCE COMMERCIALE</div>`;
  secteurs.forEach(s=>{
    const sl=leadsMonth.filter(l=>l.secteur===s.id);
    const total=sl.length,ventes=sl.filter(l=>l.statut==='vert');
    const ca=ventes.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
    const pipeline=sl.filter(l=>l.statut==='jaune').reduce((sum,l)=>sum+Number(l.montant_devis||0),0);
    const taux=total>0?Math.round(ventes.length/total*100):0;
    const oL=obj[s.id]?.leads||0,oCA=obj[s.id]?.ca||0;
    const pL=oL>0?Math.min(total/oL*100,100):0,pCA=oCA>0?Math.min(ca/oCA*100,100):0;
    html+=`<div class="secteur-card">
      <div class="secteur-title" style="color:${s.color}">📍 ${s.label}
        ${currentUser.role==='admin'?`<button onclick="editObjectif('${s.id}')" style="margin-left:auto;background:var(--s3);border:1px solid var(--b1);border-radius:6px;padding:3px 8px;font-size:10px;cursor:pointer;font-family:inherit;color:var(--t2)">⚙️</button>`:''}
      </div>
      <div class="secteur-stats">
        <div class="stat-box"><div class="stat-box-val" style="color:${s.color}">${total}</div><div class="stat-box-lbl">Leads</div><div class="stat-box-obj">Obj: ${oL}</div><div class="objectif-bar"><div class="objectif-bar-fill" style="width:${pL}%;background:${s.color}"></div></div></div>
        <div class="stat-box"><div class="stat-box-val" style="color:var(--g)">${ventes.length}</div><div class="stat-box-lbl">Ventes · ${taux}%</div></div>
        <div class="stat-box"><div class="stat-box-val" style="color:var(--g);font-size:15px">${ca.toLocaleString('fr-FR')} €</div><div class="stat-box-lbl">CA réalisé</div><div class="stat-box-obj">Obj: ${oCA.toLocaleString('fr-FR')} €</div><div class="objectif-bar"><div class="objectif-bar-fill" style="width:${pCA}%;background:var(--g)"></div></div></div>
        <div class="stat-box"><div class="stat-box-val" style="color:var(--y);font-size:15px">${pipeline.toLocaleString('fr-FR')} €</div><div class="stat-box-lbl">Pipeline</div></div>
      </div>
    </div>`;
  });
  // Stats par source
  const sources=LEAD_SOURCE_CODES;
  const srcIcons=LEAD_SOURCE_ICONS;
  html+=`<div class="secteur-card"><div class="secteur-title">📊 Par source</div><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(92px,1fr));gap:6px">`;
  sources.forEach(src=>{
    const sl=leadsMonth.filter(l=>l.source===src);
    const v=sl.filter(l=>l.statut==='vert').length;
    html+=`<div class="stat-box"><div style="font-size:16px">${srcIcons[src]}</div><div class="stat-box-val" style="font-size:16px">${sl.length}</div><div class="stat-box-lbl">${LEAD_SOURCE_LABELS[src]||src}</div><div style="font-size:10px;color:var(--g)">${v} ventes</div></div>`;
  });
  html+='</div></div>';
  // Classement mensuel commerciaux
  const ranking=getCommercialRankingMonth(leads);
  html+=`<div class="secteur-card"><div class="secteur-title">🏆 Classement mensuel commerciaux</div>`;
  if(!ranking.length){
    html+=`<div style="font-size:12px;color:var(--t3)">Aucune vente ce mois.</div>`;
  } else {
    html+=ranking.map((r,i)=>{
      const u=getAllUsers().find(x=>x.id===r.uid);
      return `<div class="perf-row">
        <div style="width:24px;text-align:center;font-weight:800;color:${i===0?'var(--g)':'var(--t2)'}">${i+1}</div>
        <div class="perf-name">${esc(u?.name||r.uid)}</div>
        <div class="perf-stats">${r.ventes} vente(s)</div>
        <div class="perf-ca">${r.ca.toLocaleString('fr-FR')}€</div>
      </div>`;
    }).join('');
  }
  html+='</div>';
  // Analyse leads perdus
  const lost=getLeadLostAnalytics(leadsMonth);
  html+=`<div class="secteur-card"><div class="secteur-title">📉 Analyse des leads perdus</div>`;
  if(!lost.total){
    html+=`<div style="font-size:12px;color:var(--t3)">Aucun lead perdu ce mois.</div>`;
  } else {
    html+=lost.rows.map(r=>`
      <div class="perf-row">
        <div class="perf-name">${esc(r.label)}</div>
        <div class="perf-stats">${r.count} lead(s)</div>
      </div>
    `).join('');
  }
  html+='</div>';
  // Historique archivé
  const arch=getArchivedLeadsStats(getCompanyScopedLeads(getLeads()));
  html+=`<div class="secteur-card"><div class="secteur-title">🗃️ Historique archivé</div>
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px">
      <div class="stat-box"><div class="stat-box-val">${arch.total}</div><div class="stat-box-lbl">Total archivés</div></div>
      <div class="stat-box"><div class="stat-box-val" style="color:var(--g)">${arch.byStatut.vert}</div><div class="stat-box-lbl">Vendus</div></div>
      <div class="stat-box"><div class="stat-box-val" style="color:var(--r)">${arch.byStatut.rouge}</div><div class="stat-box-lbl">Perdus</div></div>
    </div>
    <div style="margin-top:8px;font-size:11px;color:var(--t3)">Consultez l’onglet filtre “📁 Archives” dans Leads pour le détail.</div>
  </div>`;
  // Perf commerciaux
  const role=currentUser?.role||'assistante';
  const commerciaux=getAllUsers().filter(u=>u.role==='commercial');
  if(role==='admin'||isCRMScopePilotageRole(role)){
    html+=`<div style="font-size:11px;color:var(--t3);font-weight:700;margin:6px 0">4) EXPORTS ET ACTIONS</div>`;
    html+=`<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap">
      <button onclick="exportLeadsExcel()" style="flex:1;min-width:220px;padding:8px;background:var(--g2);color:var(--g);border:1px solid rgba(34,197,94,.3);border-radius:8px;font-family:inherit;font-size:12px;font-weight:600;cursor:pointer">📥 Exporter tous les leads</button>
      <button onclick="exportAllCRMTables()" style="flex:1;min-width:220px;padding:8px;background:var(--a3);color:var(--a);border:1px solid rgba(232,148,58,.35);border-radius:8px;font-family:inherit;font-size:12px;font-weight:600;cursor:pointer">📦 Télécharger tous les tableaux CRM</button>
      <button onclick="ouvrirFusion()" style="flex:1;min-width:220px;padding:8px;background:var(--bl2);color:var(--bl);border:1px solid rgba(96,165,250,.3);border-radius:8px;font-family:inherit;font-size:12px;font-weight:600;cursor:pointer">🔀 Fusionner leads en double</button>
    </div>`;
    html+=`<div class="secteur-card">
      <div class="secteur-title">📤 Exports individuels</div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px">
        <button onclick="exportCRMTable('leads')" style="padding:8px;background:var(--s3);color:var(--t1);border:1px solid var(--b1);border-radius:8px;font-family:inherit;font-size:12px;cursor:pointer">Leads détaillés</button>
        <button onclick="exportCRMTable('ventes')" style="padding:8px;background:var(--s3);color:var(--t1);border:1px solid var(--b1);border-radius:8px;font-family:inherit;font-size:12px;cursor:pointer">Ventes</button>
        <button onclick="exportCRMTable('secteurs')" style="padding:8px;background:var(--s3);color:var(--t1);border:1px solid var(--b1);border-radius:8px;font-family:inherit;font-size:12px;cursor:pointer">Performance secteurs</button>
        <button onclick="exportCRMTable('sources')" style="padding:8px;background:var(--s3);color:var(--t1);border:1px solid var(--b1);border-radius:8px;font-family:inherit;font-size:12px;cursor:pointer">Performance sources</button>
        <button onclick="exportCRMTable('classement')" style="padding:8px;background:var(--s3);color:var(--t1);border:1px solid var(--b1);border-radius:8px;font-family:inherit;font-size:12px;cursor:pointer">Classement commerciaux</button>
        <button onclick="exportCRMTable('pertes')" style="padding:8px;background:var(--s3);color:var(--t1);border:1px solid var(--b1);border-radius:8px;font-family:inherit;font-size:12px;cursor:pointer">Analyse pertes</button>
      </div>
    </div>`;
  }

  // RAPPORT ALERTES
  const rapport=genererRapportAlertes();
  if(rapport.total>0){
    html+=`<div class="secteur-card" style="border-color:var(--r)">
      <div class="secteur-title" style="color:var(--r)">🚨 Rapport alertes (${rapport.total})</div>
      ${rapport.nonAttrib.length?`<div style="margin-bottom:6px"><span style="font-size:11px;font-weight:700;color:var(--y)">⚠️ ${rapport.nonAttrib.length} lead(s) non attribué(s)</span>${rapport.nonAttrib.map(l=>`<div style="font-size:11px;color:var(--t2);padding:2px 0 2px 12px">→ ${esc(l.nom)} (${getLeadAge(l)})</div>`).join('')}</div>`:''}
      ${rapport.nonOuverts.length?`<div style="margin-bottom:6px"><span style="font-size:11px;font-weight:700;color:var(--r)">👁️ ${rapport.nonOuverts.length} lead(s) non ouvert(s) par le commercial</span>${rapport.nonOuverts.map(l=>{const c=getAllUsers().find(u=>u.id===l.commercial);return`<div style="font-size:11px;color:var(--t2);padding:2px 0 2px 12px">→ ${esc(l.nom)} → ${esc(c?.name||'?')} (${getLeadAge(l)})</div>`;}).join('')}</div>`:''}
      ${rapport.enRetard.length?`<div><span style="font-size:11px;font-weight:700;color:var(--r)">⏰ ${rapport.enRetard.length} lead(s) sans action +24h</span>${rapport.enRetard.map(l=>{const c=getAllUsers().find(u=>u.id===l.commercial);return`<div style="font-size:11px;color:var(--t2);padding:2px 0 2px 12px">→ ${esc(l.nom)} → ${esc(c?.name||'?')}</div>`;}).join('')}</div>`:''}
    </div>`;
  }

  // OBJECTIFS COMMERCIAUX
  if(commerciaux.length>0){
    html+=`<div class="secteur-card"><div class="secteur-title">🎯 Objectifs commerciaux</div>`;
    commerciaux.forEach(c=>{
      const prog=getProgressionCommercial(c.id);
      const pctMois=prog.obj.mensuel>0?Math.min(prog.ca_mois/prog.obj.mensuel*100,110):0;
      const pctHebdo=prog.obj.hebdo>0?Math.min(prog.ca_semaine/prog.obj.hebdo*100,110):0;
      const contacts=getContactsParCommercial()[c.id]||{ce_mois:0,cette_semaine:0};
      html+=`<div style="background:var(--s3);border-radius:10px;padding:12px;margin-bottom:8px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
          <div style="width:28px;height:28px;border-radius:8px;background:${c.color};display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;color:#fff">${c.initial}</div>
          <div style="flex:1"><div style="font-size:13px;font-weight:600">${esc(c.name)}</div><div style="font-size:10px;color:var(--t3)">${contacts.ce_mois} contacts ce mois · ${contacts.cette_semaine} cette semaine${c.vehicule?` · 🚐 ${esc(c.vehicule)}`:''}</div></div>
          ${(currentUser.role==='admin'||isCRMScopePilotageRole(currentUser.role))?`<button onclick="editObjectifCommercial('${c.id}','${esc(c.name)}')" style="background:var(--s2);border:1px solid var(--b1);border-radius:6px;padding:3px 8px;font-size:10px;cursor:pointer;font-family:inherit;color:var(--t2)">⚙️</button>`:''}
        </div>
        <div style="font-size:10px;color:var(--t3);margin-bottom:3px">Mensuel — ${prog.ca_mois.toLocaleString('fr-FR')}€ / ${prog.obj.mensuel.toLocaleString('fr-FR')}€ · ${prog.ventes_mois} ventes</div>
        <div class="objectif-bar" style="margin-bottom:6px"><div class="objectif-bar-fill" style="width:${pctMois}%;background:${pctMois>=100?'var(--g)':pctMois>=75?'var(--a)':'var(--r)'}"></div></div>
        <div style="font-size:10px;color:var(--t3);margin-bottom:3px">Hebdo — ${prog.ca_semaine.toLocaleString('fr-FR')}€ / ${prog.obj.hebdo.toLocaleString('fr-FR')}€ · ${prog.ventes_semaine} ventes</div>
        <div class="objectif-bar"><div class="objectif-bar-fill" style="width:${pctHebdo}%;background:${pctHebdo>=100?'var(--g)':pctHebdo>=75?'var(--a)':'var(--r)'}"></div></div>
        ${pctMois>=100?`<div style="margin-top:6px;font-size:11px;color:var(--g);font-weight:600">🏆 Objectif mensuel dépassé !</div>`:''}
      </div>`;
    });
    html+=`</div>`;
  }

  // TABLEAU DE VENTES
  const tvDiv=document.createElement('div');tvDiv.className='secteur-card';
  if(list)list.innerHTML=html;
  renderTableauVentes(tvDiv);
  if(list)list.appendChild(tvDiv);
}

function editObjectifCommercial(uid,name){
  const obj=getObjectifsCommerciaux();
  const cur=obj[uid]||{mensuel:0,hebdo:0};
  const mensuel=prompt(`Objectif CA mensuel (€) pour ${name} :`,cur.mensuel);
  if(mensuel===null)return;
  const hebdo=prompt(`Objectif CA hebdo (€) pour ${name} :`,cur.hebdo);
  if(hebdo===null)return;
  setObjectifCommercial(uid,mensuel,hebdo);
  renderLeadsDashboard();
  logActivity(`${currentUser.name} a fixé les objectifs de ${name}`);
}

function editObjectif(secteur){
  const obj=getLeadObjectifs();const s=obj[secteur]||{leads:0,ca:0};
  const label=getLeadSecteurLabel(secteur);
  const newLeads=prompt(`Objectif leads/mois — ${label} :`,s.leads);if(newLeads===null)return;
  const newCA=prompt(`Objectif CA/mois (€ HT) — ${label} :`,s.ca);if(newCA===null)return;
  obj[secteur]={leads:parseInt(newLeads)||0,ca:parseInt(newCA)||0};
  saveLeadObjectifs(obj);renderLeadsDashboard();
}

// OUVRIR NOUVEAU LEAD
function openNewLead(){
  currentLeadId=null;currentLeadStatut='gris';currentLeadSource='MAG';
  document.getElementById('modal-lead-nom').textContent='Nouveau lead';
  document.getElementById('modal-lead-sub').innerHTML='';
  resetLeadForm();
  const role=currentUser?.role;
  // Statut visible seulement si attribué (commercial) ou dir co / admin
  document.getElementById('modal-statut-section').style.display='none';
  const commWrap=document.getElementById('lead-commercial-wrap');
  if(commWrap)commWrap.style.display=(role==='admin'||isCRMScopePilotageRole(role))?'block':'none';
  fillCommercialAssign();fillProjetsSuggestions();
  document.getElementById('btn-delete-lead').style.display='none';
  document.getElementById('lead-timeline').style.display='none';
  updateLeadSectorSignals();
  document.getElementById('lead-modal').classList.add('open');
}

// OUVRIR FICHE LEAD EXISTANT
function openLead(id){
  const leads=getLeads();const l=leads.find(x=>x.id===id);if(!l)return;
  if(!canAccessLeadByCompany(l)){showDriveNotif('🔒 Accès refusé pour cette société');return;}
  currentLeadId=id;currentLeadStatut=l.statut||'gris';currentLeadSource=l.source||'MAG';
  // Marquer comme vu
  const role=currentUser?.role;
  if(role!=='admin'&&!isCRMScopePilotageRole(role)&&!l.vu_date){
    const idx=leads.findIndex(x=>x.id===id);
    const dateStr=new Date().toLocaleDateString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
    leads[idx].vu_date=dateStr;
    leads[idx].vu_par=currentUser.name;
    addLeadTimelineEntry(leads[idx],`Lead ouvert pour la première fois`,currentUser.name);
    saveLeads(leads);
  }
  document.getElementById('modal-lead-nom').textContent=l.nom;
  const rdvLeadCount=getLeadRdvDoneCount(l);
  const rdvClientCount=getClientRdvDoneCount(l);
  document.getElementById('modal-lead-sub').innerHTML=`
    <span class="lead-secteur">${getLeadSecteurLabel(l.secteur)}</span>
    <span>${getLeadAge(l)}</span>
    <span style="color:var(--g)">📅 ${rdvClientCount} RDV chez ce client</span>
    ${rdvLeadCount!==rdvClientCount?`<span style="color:var(--t2)">Fiche actuelle: ${rdvLeadCount}</span>`:''}
    ${l.vu_date?`<span class="lead-vu">👁️ Vu le ${l.vu_date}</span>`:(role==='admin'||isCRMScopePilotageRole(role))?'<span class="lead-non-vu">👁️ Non ouvert</span>':''}
  `;
  resetLeadForm();
  // Remplir champs
  document.getElementById('lead-nom').value=l.nom||'';
  document.getElementById('lead-tel').value=l.telephone||'';
  document.getElementById('lead-adresse').value=l.adresse||'';
  document.getElementById('lead-ville').value=l.ville||'';
  document.getElementById('lead-cp').value=l.cp||'';
  document.getElementById('lead-projet').value=l.type_projet||'';
  document.getElementById('lead-secteur').value=l.secteur||'nimes';
  const leadHorsSecteurJustif=document.getElementById('lead-hors-secteur-justif');
  if(leadHorsSecteurJustif)leadHorsSecteurJustif.value=l.justification_hors_secteur||'';
  document.getElementById('lead-suivi').value=l.suivi||'';
  document.getElementById('lead-commentaire').value=l.commentaire||'';
  // Source
  selectSourceByVal(l.source||'MAG');
  // Statut — visible si lead attribué OU si admin / pilotage CRM
  const canChangeStatut=(l.commercial&&(role==='commercial'||role==='admin'||isCRMScopePilotageRole(role)))||(role==='admin'||isCRMScopePilotageRole(role));
  document.getElementById('modal-statut-section').style.display=canChangeStatut?'block':'none';
  if(canChangeStatut){
    selectStatut(l.statut||'gris',null,true);
    if(l.sous_statut)document.getElementById('lead-sous-statut').value=l.sous_statut;
    if(l.rappel)document.getElementById('lead-rappel').value=l.rappel;
    toggleRdvDate();
    if(l.date_rdv_fait){const el=document.getElementById('lead-date-rdv-fait');if(el)el.value=l.date_rdv_fait;}
    if(l.montant_rdv){const el=document.getElementById('lead-montant-rdv');if(el)el.value=l.montant_rdv;}
    if(l.montant_devis)document.getElementById('lead-montant-devis').value=l.montant_devis;
    if(l.date_devis)document.getElementById('lead-date-devis').value=l.date_devis;
    if(l.rappel_devis)document.getElementById('lead-rappel-devis').value=l.rappel_devis;
    if(l.prix_vendu)document.getElementById('lead-prix-vendu').value=l.prix_vendu;
    if(l.date_signature)document.getElementById('lead-date-signature').value=l.date_signature;
    if(l.produit_vendu)document.getElementById('lead-produit-vendu').value=l.produit_vendu;
    if(l.raison_mort)document.getElementById('lead-raison-mort').value=l.raison_mort;
  }
  // Attribution
  const commWrap=document.getElementById('lead-commercial-wrap');
  if(commWrap)commWrap.style.display=(role==='admin'||isCRMScopePilotageRole(role))?'block':'none';
  fillCommercialAssign(l.commercial);
  fillProjetsSuggestions();
  // Suppression — Benjamin uniquement
  document.getElementById('btn-delete-lead').style.display=currentUser.id==='benjamin'?'block':'none';
  // Lead orphelin (commercial supprimé) → remettre en attente d'attribution
  if(l.commercial&&!getAllUsers().find(u=>u.id===l.commercial)){
    const idx2=leads.findIndex(x=>x.id===id);
    if(idx2!==-1){leads[idx2].commercial=null;saveLeads(leads);}
    showDriveNotif('⚠️ Commercial introuvable — lead remis en attente');
  }
  // Bouton réouvrir si archivé
  const btnSave=document.getElementById('btn-save-lead');
  if(l.archive&&btnSave){
    btnSave.textContent='🔄 Réouvrir ce lead';
    btnSave.onclick=()=>rouvrirLead(id);
  } else if(btnSave){
    btnSave.textContent='Enregistrer';
    btnSave.onclick=()=>saveLead();
  }
  // Timeline
  if(l.timeline&&l.timeline.length>0){
    document.getElementById('lead-timeline').style.display='block';
    document.getElementById('timeline-list').innerHTML=l.timeline.slice().reverse().map(t=>`
      <div class="timeline-item">
        <div class="timeline-dot"></div>
        <div class="timeline-content">
          <div class="timeline-action">${esc(t.action)}</div>
          <div class="timeline-meta">${t.date} · ${esc(t.user)}</div>
        </div>
      </div>`).join('');
  } else {
    document.getElementById('lead-timeline').style.display='none';
  }
  document.getElementById('lead-modal').classList.add('open');
  // Boutons contextuels
  const btnAgenda=document.getElementById('btn-agenda-lead');
  const btnRdvDone=document.getElementById('btn-rdv-done-lead');
  const btnEmail=document.getElementById('btn-export-lead');
  if(btnAgenda)btnAgenda.style.display=(l.rappel||l.date_rdv)?'inline-block':'none';
  if(btnRdvDone)btnRdvDone.style.display='inline-block';
  if(btnEmail)btnEmail.style.display='inline-block';
  updateLeadSectorSignals();
}

function closeLeadModal(){
  document.getElementById('lead-modal').classList.remove('open');
  currentLeadId=null;
  document.getElementById('adresse-suggestions').style.display='none';
}

function resetLeadForm(){
  ['lead-nom','lead-tel','lead-adresse','lead-ville','lead-cp','lead-projet','lead-suivi','lead-commentaire','lead-action',
   'lead-rappel','lead-montant-devis','lead-date-devis','lead-rappel-devis',
   'lead-prix-vendu','lead-date-signature','lead-produit-vendu'].forEach(id=>{
    const el=document.getElementById(id);if(el)el.value='';
  });
  document.getElementById('lead-secteur').value='nimes';
  const leadHorsSecteurJustif=document.getElementById('lead-hors-secteur-justif');
  if(leadHorsSecteurJustif)leadHorsSecteurJustif.value='';
  const leadSectorAlert=document.getElementById('lead-sector-alert');
  if(leadSectorAlert){leadSectorAlert.style.display='none';leadSectorAlert.textContent='';}
  const leadHorsSecteurWrap=document.getElementById('lead-hors-secteur-wrap');
  if(leadHorsSecteurWrap)leadHorsSecteurWrap.style.display='none';
  document.getElementById('lead-sous-statut').value='a_rappeler';
  const dateRdvFait=document.getElementById('lead-date-rdv-fait');if(dateRdvFait)dateRdvFait.value='';
  const montantRdv=document.getElementById('lead-montant-rdv');if(montantRdv)montantRdv.value='';
  document.getElementById('lead-raison-mort').value='';
  const rdvWrap=document.getElementById('rdv-date-wrap');
  if(rdvWrap)rdvWrap.style.display='none';
  // Reset bordure modale
  const box=document.querySelector('.lead-modal-box');
  if(box){box.style.background='';box.style.borderColor='var(--b1)';}
  selectStatut('gris',null,true);
  selectSourceByVal('MAG');
}

function selectSource(btn){
  document.querySelectorAll('.src-btn').forEach(b=>b.classList.remove('selected'));
  btn.classList.add('selected');
  currentLeadSource=btn.dataset.src;
}
function selectSourceByVal(val){
  document.querySelectorAll('.src-btn').forEach(b=>{
    b.classList.toggle('selected',b.dataset.src===val);
  });
  currentLeadSource=val;
}

function selectStatut(statut,btn,silent=false){
  currentLeadStatut=statut;
  document.querySelectorAll('.statut-btn').forEach(b=>b.classList.remove('selected'));
  if(btn)btn.classList.add('selected');
  else document.querySelectorAll('.statut-btn').forEach(b=>{if(b.classList.contains('sb-'+statut))b.classList.add('selected');});
  ['gris','rdv','jaune','vert','rouge'].forEach(s=>{
    const p=document.getElementById('panel-'+s);if(p)p.style.display=s===statut?'block':'none';
  });
  const box=document.querySelector('.lead-modal-box');
  if(box){box.style.background='';const borders={gris:'var(--b1)',rdv:'rgba(96,165,250,.6)',jaune:'rgba(251,191,36,.6)',vert:'rgba(34,197,94,.6)',rouge:'rgba(248,113,113,.6)'};box.style.borderColor=borders[statut]||'var(--b1)';box.style.borderWidth='2px';}
}

function toggleRdvDate(){
  const sel=document.getElementById('lead-sous-statut');
  const wrap=document.getElementById('rdv-date-wrap');
  const isRdv=sel&&sel.value==='rdv_programme';
  if(wrap)wrap.style.display=isRdv?'block':'none';
  if(isRdv){
    // Marquer comme RDV pris mais garder panel-gris ouvert pour la date
    currentLeadStatut='rdv';
    document.querySelectorAll('.statut-btn').forEach(b=>{
      b.classList.toggle('selected',b.classList.contains('sb-rdv'));
    });
    const box=document.querySelector('.lead-modal-box');
    if(box){box.style.background='';box.style.borderColor='rgba(96,165,250,.6)';box.style.borderWidth='2px';}
  }
}

function formatTel(input){
  let v=input.value.replace(/\D/g,'').substring(0,10);
  let formatted='';
  for(let i=0;i<v.length;i++){
    if(i>0&&i%2===0)formatted+=' ';
    formatted+=v[i];
  }
  input.value=formatted;
}

// AJOUTER ACTION MANUELLE
function addLeadAction(){
  const input=document.getElementById('lead-action');
  const txt=input?.value.trim();if(!txt||!currentLeadId)return;
  const leads=getLeads();
  const l=leads.find(x=>x.id===currentLeadId);if(!l)return;
  addLeadTimelineEntry(l,txt,currentUser.name);
  saveLeads(leads);
  input.value='';
  // Refresh timeline
  document.getElementById('timeline-list').innerHTML=(l.timeline||[]).slice().reverse().map(t=>`
    <div class="timeline-item">
      <div class="timeline-dot"></div>
      <div class="timeline-content">
        <div class="timeline-action">${esc(t.action)}</div>
        <div class="timeline-meta">${t.date} · ${esc(t.user)}</div>
      </div>
    </div>`).join('');
  document.getElementById('lead-timeline').style.display='block';
  showDriveNotif('Action enregistrée');
}

function addLeadTimelineEntry(lead,action,user){
  if(!lead.timeline)lead.timeline=[];
  const dateStr=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  lead.timeline.push({date:dateStr,user,action});
}

function shouldAutoAgendaForLead(lead){
  if(!lead)return false;
  const hasRdvDate=!!(lead.rappel||lead.date_rdv);
  const isRdv=lead.statut==='rdv'||lead.sous_statut==='rdv_programme';
  return hasRdvDate&&isRdv;
}
function autoPushLeadToGoogleAgenda(lead,force=false){
  if(!shouldAutoAgendaForLead(lead))return false;
  const rdvKey=String(lead.rappel||lead.date_rdv||'');
  if(!force&&lead.agenda_last_sync===rdvKey)return false;
  const url=makeGoogleAgendaLink(lead);
  if(!url)return false;
  window.open(url,'_blank');
  lead.agenda_last_sync=rdvKey;
  addLeadTimelineEntry(lead,'RDV envoyé vers Google Agenda (auto)',currentUser.name);
  return true;
}

// SAUVEGARDER LEAD
function saveLead(){
  const nom=document.getElementById('lead-nom').value.trim();
  const tel=document.getElementById('lead-tel').value.trim();
  const adresse=document.getElementById('lead-adresse').value.trim();
  // Ville en lecture seule — permettre saisie manuelle si autocomplétion non utilisée
  const villeEl=document.getElementById('lead-ville');
  const cpEl=document.getElementById('lead-cp');
  if(villeEl)villeEl.removeAttribute('readonly');
  if(cpEl)cpEl.removeAttribute('readonly');
  const ville=villeEl?.value.trim()||adresse.split(/\d{5}/)?.[1]?.trim()||'';
  const cp2=cpEl?.value.trim()||'';
  const projet=document.getElementById('lead-projet').value.trim();
  const sectorState=updateLeadSectorSignals()||computeLeadSectorState(cp2);
  const justifHorsSecteur=(document.getElementById('lead-hors-secteur-justif')?.value||'').trim();
  const historicalLead=findHistoricalLeadByIdentity(nom,adresse,cp2,currentLeadId);
  const historicalProposal=getCommercialHistoryProposal(historicalLead);
  const effectiveSource=historicalLead?'ANCIEN_CLIENT':currentLeadSource;
  const missing=[];
  if(!nom)missing.push('Nom');
  if(!tel)missing.push('Téléphone');
  if(!projet)missing.push('Projet');
  if(!cp2)missing.push('Code postal');
  if(currentUser?.role==='commercial'&&sectorState.outOfSector&&!justifHorsSecteur){
    missing.push('Justification hors secteur');
  }
  const raisonMort=document.getElementById('lead-raison-mort').value;
  const prixVenduRaw=(document.getElementById('lead-prix-vendu')?.value||'').trim();
  const prixVenduValue=Number(prixVenduRaw);
  if(currentLeadStatut==='rouge'&&!raisonMort)missing.push('Raison du refus');
  if(currentLeadStatut==='vert'){
    if(!prixVenduRaw)missing.push('Prix final vendu HT');
    else if(!Number.isFinite(prixVenduValue)||prixVenduValue<=0)missing.push('Prix final vendu HT (> 0)');
  }
  if(missing.length){
    alert('Champs vides : '+missing.join(', '));
    return;
  }
  const leads=getLeads();
  let agendaAutoOpened=false;
  let movedOutOfNonAttrib=false;
  const activeCRMTabId=(document.querySelector('.crm-tab.active')?.id||'').replace('crm-tab-','');
  const now=new Date().toISOString();
  const dateStr=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  const selectedCommercial=document.getElementById('lead-commercial-assign')?.value||null;
  const defaultCommercial=currentUser?.role==='commercial'?currentUser.id:null;
  const assignedCommercial=currentLeadSource==='ACTIF'?currentUser.id:(selectedCommercial||defaultCommercial||null);
  const data={
    nom,telephone:tel,adresse,ville,cp:cp2,type_projet:projet,
    secteur:sectorState.secteur,
    source:effectiveSource,
    statut:currentLeadStatut,
    sous_statut:document.getElementById('lead-sous-statut')?.value||'a_rappeler',
    rappel:document.getElementById('lead-rappel')?.value||null,
    date_rdv_fait:document.getElementById('lead-date-rdv-fait')?.value||null,
    montant_rdv:document.getElementById('lead-montant-rdv')?.value||null,
    montant_devis:document.getElementById('lead-montant-devis')?.value||null,
    date_devis:document.getElementById('lead-date-devis')?.value||null,
    rappel_devis:document.getElementById('lead-rappel-devis')?.value||null,
    prix_vendu:prixVenduRaw||null,
    date_signature:document.getElementById('lead-date-signature')?.value||null,
    produit_vendu:document.getElementById('lead-produit-vendu')?.value||null,
    raison_mort:document.getElementById('lead-raison-mort')?.value||null,
    commercial:assignedCommercial,
    commercial_user_id:resolveAuthUidForUserId(assignedCommercial),
    suivi:document.getElementById('lead-suivi')?.value.trim()||'',
    commentaire:document.getElementById('lead-commentaire')?.value.trim()||'',
    hors_secteur:!!sectorState.outOfSector,
    zone_blanche:!!sectorState.isZoneBlanche,
    justification_hors_secteur:justifHorsSecteur||null,
    societe_secteur:CRM_SECTOR_OWNERS[sectorState.secteur]||null,
    ancien_client:!!historicalLead,
    commercial_historique:historicalProposal?.id||historicalLead?.commercial||null,
    commercial_historique_nom:historicalProposal?.name||null,
    proposition_reaffectation:!!(historicalProposal&&historicalProposal.isActive),
  };
  if(currentLeadId){
    const idx=leads.findIndex(l=>l.id===currentLeadId);
    if(idx!==-1){
      const old=leads[idx];
      // Archive auto si vert ou rouge
      if((data.statut==='vert'||data.statut==='rouge')&&old.statut!==data.statut){
        data.archive=false;// On garde actif, archive manuelle
      }
      if(old.statut!==data.statut){
        addLeadTimelineEntry(old,`Statut : ${old.statut} → ${data.statut}`,currentUser.name);
      }
      if(data.date_rdv_fait&&data.date_rdv_fait!==old.date_rdv_fait){
        const doneDate=new Date(data.date_rdv_fait+'T09:00:00');
        if(registerLeadRdvDone(old,'fiche',doneDate.toISOString())){
          addLeadTimelineEntry(old,'RDV effectué (fiche lead)',currentUser.name);
        }
      }
      if(old.commercial!==data.commercial&&data.commercial){
        const newComm=getAllUsers().find(u=>u.id===data.commercial);
        addLeadTimelineEntry(old,`Attribué à ${newComm?.name||data.commercial}`,currentUser.name);
        // Notifier le commercial
        pushNotif('Lead attribué',`${nom} — ${projet}`,LEAD_SOURCE_ICONS[data.source]||'📋',data.commercial);
      }
      const updatedLead={
        ...old,
        ...data,
        id:currentLeadId,
        date_creation:old.date_creation,
        date_modification:now,
        commercial_user_id:data.commercial_user_id||old.commercial_user_id||null,
        societe_crm:resolveLeadSocieteBySecteur(data.secteur,old.societe_crm||getSocieteFromUser(currentUser.id))
      };
      leads[idx]=updatedLead;
      if(activeCRMTabId==='non-attribues'&&!isLeadInNonAttribQueue(updatedLead)){
        movedOutOfNonAttrib=true;
      }
      agendaAutoOpened=autoPushLeadToGoogleAgenda(updatedLead)||agendaAutoOpened;
    }
    logActivity(`${currentUser.name} a mis à jour : ${nom}`);
  } else {
    // Nouveau lead
    const societe=resolveLeadSocieteBySecteur(data.secteur,getSocieteFromUser(currentUser.id));
    let autoCommercial=data.commercial||null;
    if(!autoCommercial&&!hasDirecteurCommercial()){
      autoCommercial=getRoundRobinCommercial(societe);
    }
    if(data.ancien_client&&historicalProposal&&historicalProposal.isActive&&data.hors_secteur&&currentUser?.role==='directeur_co'&&!selectedCommercial){
      showDriveNotif(`🔁 Ancien client détecté : proposition de réattribution à ${historicalProposal.name}`);
    }
    const newLead={...data,id:Date.now(),date_creation:now,timeline:[],rdv_history:[],cree_par:currentUser.id,societe_crm:societe,commercial:autoCommercial};
    if(newLead.date_rdv_fait){
      const doneDate=new Date(newLead.date_rdv_fait+'T09:00:00');
      registerLeadRdvDone(newLead,'creation',doneDate.toISOString());
    }
    addLeadTimelineEntry(newLead,'Lead créé',currentUser.name);
    if(newLead.zone_blanche){
      addLeadTimelineEntry(newLead,'Lead tagué en zone blanche',currentUser.name);
    }
    if(newLead.hors_secteur){
      addLeadTimelineEntry(newLead,`Lead hors secteur (${getLeadSecteurLabel(newLead.secteur)})`,currentUser.name);
    }
    if(newLead.ancien_client){
      const histTxt=newLead.commercial_historique_nom?` — commercial historique: ${newLead.commercial_historique_nom}`:'';
      addLeadTimelineEntry(newLead,`Ancien client détecté${histTxt}`,currentUser.name);
    }
    if(newLead.hors_secteur){
      const proposalTxt=newLead.commercial_historique_nom&&newLead.proposition_reaffectation
        ?` · Proposition: réattribuer à ${newLead.commercial_historique_nom}`
        :'';
      getDirecteursConcernedByLead(newLead).forEach(u=>{
        pushNotif('Vous êtes hors secteur',`${nom} (${newLead.cp||'CP inconnu'})${proposalTxt}`,'⚠️',u.id);
      });
    }
    if(newLead.commercial){
      const c=getAllUsers().find(u=>u.id===newLead.commercial);
      addLeadTimelineEntry(newLead,`Attribué à ${c?.name||newLead.commercial}`,currentUser.name);
    }
    const dirsLead=getDirecteursConcernedByLead(newLead);
    const leadSrcIcon=LEAD_SOURCE_ICONS[effectiveSource]||LEAD_SOURCE_ICONS[data.source]||'📋';
    const srcHuman=LEAD_SOURCE_LABELS[effectiveSource]||LEAD_SOURCE_LABELS[data.source]||String(effectiveSource||data.source||'');
    if(currentUser?.role==='commercial'&&dirsLead.length){
      dirsLead.forEach(u=>{
        pushNotif('Nouveau lead équipe',`${nom} — ${projet} · ${srcHuman} · Par ${currentUser.name}`,leadSrcIcon,u.id);
      });
    }else if(currentLeadSource==='ACTIF'&&dirsLead.length){
      dirsLead.forEach(u=>{
        pushNotif('Lead ACTIF créé',`${nom} par ${currentUser.name}`,leadSrcIcon,u.id);
      });
    }else if(!newLead.commercial&&dirsLead.length){
      dirsLead.forEach(u=>{
        pushNotif('Nouveau lead à attribuer',`${nom} — ${projet}`,leadSrcIcon,u.id);
      });
    }
    agendaAutoOpened=autoPushLeadToGoogleAgenda(newLead)||agendaAutoOpened;
    leads.unshift(newLead);
    updateProjetSuggestion(projet);
    logActivity(`${currentUser.name} a créé un lead : ${nom}`);
    // Rafraîchir badge notifs
    refreshNotifBadge();
  }
  saveLeads(leads);
  if(agendaAutoOpened){
    showDriveNotif('📅 RDV prérempli dans Google Agenda');
  }
  closeLeadModal();
  if(movedOutOfNonAttrib){
    showCRMTab('mes-leads');
    showDriveNotif('ℹ️ Lead déplacé dans "Tous les leads" après mise à jour.');
  }
  renderLeads();
  refreshLeadsBadge();
}

function srcIcons(){return LEAD_SOURCE_ICONS;}

function rouvrirLead(id){
  if(!confirm('Réouvrir ce lead ?'))return;
  const leads=getLeads();
  const idx=leads.findIndex(l=>l.id===id);if(idx===-1)return;
  leads[idx].archive=false;
  leads[idx].statut='gris';
  const dateStr=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  addLeadTimelineEntry(leads[idx],'Lead réouvert',currentUser.name);
  saveLeads(leads);
  closeLeadModal();renderLeads();refreshLeadsBadge();
  showDriveNotif('✅ Lead réouvert');
  logActivity(`${currentUser.name} a réouvert le lead : ${leads[idx].nom}`);
}

function deleteLead(){
  if(currentUser.id!=='benjamin'){alert('Suppression réservée à Benjamin');return;}
  if(!confirm('Supprimer ce lead définitivement ?'))return;
  const leads=getLeads();
  const l=leads.find(x=>x.id===currentLeadId);
  if(!l)return;
  logDeletion('Lead',l?.nom||'?');
  l._deleted=true;
  l.archive=true;
  l.date_modification=new Date().toISOString();
  saveLeads(leads);
  closeLeadModal();renderLeads();refreshLeadsBadge();
  logActivity('Benjamin a supprimé le lead : '+(l?.nom||'?'));
}

// AUTOCOMPLÉTION ADRESSE (API adresse.data.gouv.fr)
let adresseTimeout=null;
async function searchAdresse(val){
  clearTimeout(adresseTimeout);
  const sugg=document.getElementById('adresse-suggestions');
  if(!val||val.length<4){sugg.style.display='none';return;}
  adresseTimeout=setTimeout(async()=>{
    try{
      const res=await fetch(`https://api-adresse.data.gouv.fr/search/?q=${encodeURIComponent(val)}&limit=6&autocomplete=1`);
      const data=await res.json();
      const features=data.features||[];
      if(!features.length){sugg.style.display='none';return;}
      sugg.innerHTML=features.map(f=>{
        const p=f.properties;
        return `<div class="addr-suggestion" onclick="selectAdresse('${escAttr(p.label)}','${escAttr(p.city||p.municipality||'')}','${escAttr(p.postcode||'')}','${escAttr(p.context?.split(',')[0]||'')}')">
          <strong>${esc(p.name||'')}</strong> ${esc(p.postcode||'')} ${esc(p.city||'')}
        </div>`;
      }).join('');
      sugg.style.display='block';
    }catch(e){sugg.style.display='none';}
  },350);
}

function selectAdresse(label,ville,cp,context){
  document.getElementById('lead-adresse').value=label;
  document.getElementById('lead-ville').value=ville;
  document.getElementById('lead-cp').value=cp;
  document.getElementById('adresse-suggestions').style.display='none';
  // Détection secteur auto par code postal
  detectSecteurByCP(cp);
}

const CRM_SECTOR_CP_MAP={
  nimes:['30000','30100','30110','30111','30114','30121','30127','30128','30129','30132','30140','30170','30190','30210','30220','30230','30240','30250','30260','30270','30300','30310','30320','30340','30350','30360','30380','30410','30420','30460','30470','30480','30490','30500','30510','30520','30540','30560','30580','30600','30610','30620','30640','30660','30670','30700','30720','30730','30740','30800','30820','30840','30870','30920','30960','30980'],
  avignon:['13103','13150','13160','13440','13550','13570','13630','13690','13870','13910','30126','30131','30133','30150','30290','30300','30390','30400','30650','84000','84130','84170','84200','84210','84230','84250','84270','84310','84320','84370','84450','84470','84510','84700','84740'],
  bagnoles:['07150','07460','07700','26790','30130','30200','30290','30330','30430','30500','30580','30630','30760','84100','84110','84150','84190','84260','84290','84330','84340','84350','84420','84430','84500','84550','84810','84830','84840','84850','84860','84870']
};
const CRM_SECTOR_SETS=Object.fromEntries(Object.entries(CRM_SECTOR_CP_MAP).map(([k,v])=>[k,new Set(v)]));
const CRM_SECTOR_LABELS={nimes:'Nîmes',avignon:'Avignon',bagnoles:'Bagnols-sur-Cèze',zone_blanche:'Zone blanche'};
const CRM_SECTOR_OWNERS={nimes:'nemausus',avignon:'nemausus',bagnoles:'lambert',zone_blanche:'zone_blanche'};
function resolveLeadSocieteBySecteur(secteur,fallbackSociete='nemausus'){
  if(secteur==='bagnoles')return 'lambert';
  if(secteur==='nimes'||secteur==='avignon')return 'nemausus';
  if(fallbackSociete==='lambert'||fallbackSociete==='nemausus')return fallbackSociete;
  return 'nemausus';
}

function normalizeLeadCP(raw){
  const digits=String(raw||'').replace(/\D/g,'');
  if(digits.length>=5)return digits.slice(0,5);
  return '';
}
function resolveLeadSectorByCP(cpRaw){
  const cp=normalizeLeadCP(cpRaw);
  if(!cp)return {cp:'',secteur:'nimes',isKnown:false,isZoneBlanche:false,ownerSociete:'nemausus',label:CRM_SECTOR_LABELS.nimes};
  if(CRM_SECTOR_SETS.avignon.has(cp))return {cp,secteur:'avignon',isKnown:true,isZoneBlanche:false,ownerSociete:'nemausus',label:CRM_SECTOR_LABELS.avignon};
  if(CRM_SECTOR_SETS.bagnoles.has(cp))return {cp,secteur:'bagnoles',isKnown:true,isZoneBlanche:false,ownerSociete:'lambert',label:CRM_SECTOR_LABELS.bagnoles};
  if(CRM_SECTOR_SETS.nimes.has(cp))return {cp,secteur:'nimes',isKnown:true,isZoneBlanche:false,ownerSociete:'nemausus',label:CRM_SECTOR_LABELS.nimes};
  return {cp,secteur:'zone_blanche',isKnown:false,isZoneBlanche:true,ownerSociete:'zone_blanche',label:CRM_SECTOR_LABELS.zone_blanche};
}
function getAllowedCommercialSectors(user){
  if(!user||user.role!=='commercial')return new Set();
  if(user.societe==='lambert')return new Set(['bagnoles']);
  if(user.societe==='les-deux')return new Set(['nimes','avignon','bagnoles']);
  return new Set(['nimes','avignon']);
}
function computeLeadSectorState(cpRaw){
  const selectedSecteur=document.getElementById('lead-secteur')?.value||'nimes';
  const detected=resolveLeadSectorByCP(cpRaw);
  const allowedSectors=getAllowedCommercialSectors(currentUser);
  const outOfSectorByRole=currentUser?.role==='commercial'&&(!allowedSectors.size||!allowedSectors.has(detected.secteur));
  const selectedMismatch=detected.isKnown&&selectedSecteur!==detected.secteur;
  return {...detected,selectedSecteur,outOfSectorByRole,selectedMismatch,outOfSector:detected.isZoneBlanche||outOfSectorByRole};
}
function updateLeadSectorSignals(){
  const cpInput=document.getElementById('lead-cp');
  const secteurSel=document.getElementById('lead-secteur');
  if(!cpInput||!secteurSel)return null;
  const state=computeLeadSectorState(cpInput.value||'');
  if(state.isKnown&&secteurSel.value!==state.secteur)secteurSel.value=state.secteur;
  const alertEl=document.getElementById('lead-sector-alert');
  const justifWrap=document.getElementById('lead-hors-secteur-wrap');
  if(alertEl){
    if(state.outOfSector){
      const scopeMsg=state.isZoneBlanche
        ?'Zone blanche: hors secteur connu.'
        :`Vous êtes hors secteur (${CRM_SECTOR_LABELS[state.secteur]||state.secteur}).`;
      alertEl.textContent=`⚠️ ${scopeMsg}`;
      alertEl.style.display='block';
    }else{
      alertEl.style.display='none';
      alertEl.textContent='';
    }
  }
  if(justifWrap)justifWrap.style.display=state.outOfSector?'block':'none';
  return state;
}
function normalizeLeadIdentityText(value){
  return String(value||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9 ]+/g,' ').replace(/\s+/g,' ').trim();
}
function getLeadIdentityKey(nom,adresse,cp){
  const n=normalizeLeadIdentityText(nom);
  const a=normalizeLeadIdentityText(adresse);
  const p=normalizeLeadCP(cp);
  if(!n||!p)return '';
  return `${n}::${p}::${a}`;
}
function findHistoricalLeadByIdentity(nom,adresse,cp,currentId){
  const key=getLeadIdentityKey(nom,adresse,cp);
  if(!key)return null;
  const leads=getLeads();
  return leads
    .filter(l=>String(l.id)!==String(currentId||'')&&getLeadIdentityKey(l.nom,l.adresse,l.cp)===key)
    .sort((a,b)=>new Date(b.date_modification||b.date_creation||0)-new Date(a.date_modification||a.date_creation||0))[0]||null;
}
function getCommercialHistoryProposal(lead){
  if(!lead?.commercial)return null;
  const user=getAllUsers().find(u=>u.id===lead.commercial);
  if(!user)return null;
  const access=getAccess();
  const isActive=access[lead.commercial]!==false;
  return {id:user.id,name:user.name,isActive};
}

function detectSecteurByCP(cp){
  const state=resolveLeadSectorByCP(cp);
  const secteurSel=document.getElementById('lead-secteur');
  if(secteurSel)secteurSel.value=state.secteur;
  updateLeadSectorSignals();
}

// Fermer suggestions si clic ailleurs
document.addEventListener('click',e=>{
  const sugg=document.getElementById('adresse-suggestions');
  const input=document.getElementById('lead-adresse');
  if(sugg&&input&&!sugg.contains(e.target)&&e.target!==input)sugg.style.display='none';
});

// SUGGESTIONS PROJETS
function getProjetsSuggestions(){try{return JSON.parse(appStorage.getItem('benai_projets_sugg'))||[];}catch{return[];}}
function updateProjetSuggestion(terme){
  const sugg=getProjetsSuggestions();
  const ex=sugg.find(s=>s.t===terme);
  if(ex)ex.n++;else sugg.push({t:terme,n:1});
  sugg.sort((a,b)=>b.n-a.n);
  appStorage.setItem('benai_projets_sugg',JSON.stringify(sugg.slice(0,30)));
}
function fillProjetsSuggestions(){
  const dl=document.getElementById('projets-list');if(!dl)return;
  dl.innerHTML=getProjetsSuggestions().map(s=>`<option value="${esc(s.t)}">`).join('');
}

// FILTRE COMMERCIAL
function fillCommercialFilter(){
  const sel=document.getElementById('crm-filter-commercial');if(!sel)return;
  const commerciaux=getAllUsers().filter(u=>{
    if(u.role!=='commercial')return false;
    if(currentUser?.role==='admin')return true;
    if(isCRMScopePilotageRole(currentUser?.role)){
      return currentUser.societe==='les-deux'||u.societe===currentUser.societe||u.societe==='les-deux';
    }
    return true;
  });
  sel.innerHTML='<option value="">Tous commerciaux</option>'+commerciaux.map(c=>`<option value="${c.id}">${esc(c.name)}</option>`).join('');
}
function fillCommercialAssign(selected=''){
  const sel=document.getElementById('lead-commercial-assign');if(!sel)return;
  const users=getAllUsers().filter(u=>{
    if(!(u.role==='commercial'||u.role==='directeur_co'))return false;
    if(currentUser?.role==='admin')return true;
    if(isCRMScopePilotageRole(currentUser?.role)){
      return currentUser.societe==='les-deux'||u.societe===currentUser.societe||u.societe==='les-deux';
    }
    return true;
  });
  sel.innerHTML='<option value="">⏳ Non attribué</option>'+users.map(c=>`<option value="${c.id}"${c.id===selected?' selected':''}>${esc(c.name)} (${c.role==='directeur_co'?'Dir.co':c.role==='directeur_general'?'Dir. général':'Commercial'})</option>`).join('');
}

// ALERTES & RAPPELS
function isLeadAlerte(l){
  if(l.statut==='vert'||l.statut==='rouge'||l.archive)return false;
  if(l.statut==='rdv')return false;// RDV pris = traité
  if(!l.date_creation)return false;
  const h=getHeuresOuvrees(new Date(l.date_creation));
  if(!l.commercial)return h>2;
  if(!l.vu_date)return h>4;
  return h>24;
}

function getLeadAge(l){
  if(!l.date_creation)return '';
  const h=(Date.now()-new Date(l.date_creation).getTime())/3600000;
  if(h<1)return 'À l\'instant';if(h<24)return Math.floor(h)+'h';
  return Math.floor(h/24)+'j';
}

function getLeadNotifScopeForUser(){
  if(!currentUser)return [];
  let scoped=getLeads().filter(l=>!l._deleted&&!l.archive&&l.statut!=='vert'&&l.statut!=='rouge');
  if(currentUser.role==='commercial'){
    scoped=scoped.filter(l=>normalizeId(l.commercial)===normalizeId(currentUser.id));
  }else if(currentUser.role==='assistante'){
    scoped=scoped.filter(l=>l.cree_par===currentUser.id||l.cree_par===currentUser.name);
  }else if(currentUser.role==='directeur_co'){
    scoped=getCompanyScopedLeads(scoped);
  }else if(currentUser.role==='directeur_general'||currentUser.role==='metreur'){
    scoped=[];
  }else if(currentUser.role==='admin'&&currentUser.id!=='benjamin'){
    scoped=[];
  }else{
    scoped=[];
  }
  return scoped;
}

function checkLeadSmartNotifications(){
  if(!currentUser)return;
  if(currentUser.role==='admin')return;
  if(currentUser.role==='assistante')return;
  if(currentUser.role==='directeur_general'||currentUser.role==='metreur')return;
  const scoped=getLeadNotifScopeForUser();
  if(!scoped.length)return;
  const nonAttribues=scoped.filter(l=>!l.commercial&&l.statut==='gris').length;
  const alertes=scoped.filter(l=>isLeadAlerte(l)).length;
  const aRappeler=scoped.filter(l=>l.statut==='gris'||l.statut==='jaune').length;
  const totalAction=Math.max(nonAttribues,0)+Math.max(alertes,0);
  if(totalAction<=0&&aRappeler<=0)return;

  const key=`benai_lead_push_state_${currentUser.id}`;
  const now=Date.now();
  let state={lastTs:0,lastSig:''};
  try{
    const raw=appStorage.getItem(key);
    if(raw){
      const parsed=JSON.parse(raw);
      if(parsed&&typeof parsed==='object')state={...state,...parsed};
    }
  }catch(e){}
  const sig=`${nonAttribues}|${alertes}|${aRappeler}`;
  const changed=sig!==state.lastSig;
  const cooldownPassed=(now-Number(state.lastTs||0))>20*60*1000; // 20 min
  if(!changed&&!cooldownPassed)return;

  let msg='';
  if(currentUser.id==='benjamin'){
    if(nonAttribues>0)msg=`Benjamin, ${nonAttribues} lead(s) t’attendent pour attribution.`;
    else if(alertes>0)msg=`Benjamin, ${alertes} lead(s) attendent une action commerciale.`;
    else msg=`Benjamin, ${aRappeler} lead(s) à suivre aujourd’hui.`;
  }else{
    if(alertes>0)msg=`${alertes} lead(s) urgents t’attendent.`;
    else msg=`${aRappeler} lead(s) à suivre maintenant.`;
  }
  pushBenAINotif('📋 Leads en attente',msg,'⏰',currentUser.id);
  appStorage.setItem(key,JSON.stringify({lastTs:now,lastSig:sig}));
}

// Compat legacy: certains appels historiques utilisent encore ce nom.
function checkLeadsAlertes(){
  checkLeadSmartNotifications();
}

function checkRappelsLeads(){
  if(!currentUser)return;
  if(currentUser.role==='directeur_general'||currentUser.role==='metreur')return;
  const now=new Date();
  const leads=getLeads().filter(l=>!l._deleted&&l.rappel&&(l.commercial===currentUser.id||currentUser.role==='directeur_co'||currentUser.id==='benjamin'));
  leads.forEach(l=>{
    const rappel=new Date(l.rappel);
    const diff=Math.abs(rappel-now)/60000;
    if(diff<=1&&!appStorage.getItem('benai_rappel_done_'+l.id+'_'+l.rappel)){
      pushBenAINotif('📞 Rappel lead',`${l.nom} — ${l.type_projet}`,l.rappel_devis?'📋':'📞',currentUser.id);
      appStorage.setItem('benai_rappel_done_'+l.id+'_'+l.rappel,'1');
    }
    if(l.statut==='jaune'&&l.date_devis){
      const devisDate=new Date(l.date_devis);devisDate.setHours(0,0,0,0);
      const today=new Date();today.setHours(0,0,0,0);
      const diffDays=Math.floor((today-devisDate)/(1000*60*60*24));
      const k='benai_devis_relance_'+l.id+'_'+l.date_devis;
      if(diffDays>=7&&!appStorage.getItem(k)){
        pushBenAINotif('📋 Devis sans réponse',`${l.nom} — relance recommandée (J+${diffDays})`,'⏰',currentUser.id);
        appStorage.setItem(k,'1');
      }
    }
  });
}

function refreshLeadsBadge(){
  const badge=document.getElementById('leads-badge');if(!badge)return;
  const role=currentUser?.role;
  if(role==='admin'){badge.style.display='none';return;}
  if(role==='directeur_general'){badge.style.display='none';return;}
  let leads=getCompanyScopedLeads(getLeads()).filter(l=>!l.archive);
  // Directeur co : badge = non attribués
  if(role==='directeur_co'){
    const n=leads.filter(l=>!l.commercial&&l.statut==='gris').length;
    if(n>0){badge.style.display='flex';badge.textContent=n;badge.style.background='var(--r)';}
    else badge.style.display='none';
    return;
  }
  if(role==='commercial')leads=leads.filter(l=>l.commercial===currentUser.id);
  const alertes=leads.filter(l=>isLeadAlerte(l)).length;
  if(alertes>0){badge.style.display='flex';badge.textContent=alertes;badge.style.background='var(--r)';}
  else{const n=leads.filter(l=>l.statut==='gris').length;
    if(n>0){badge.style.display='flex';badge.textContent=n;badge.style.background='var(--a)';}
    else badge.style.display='none';}
}

// EXPORT EXCEL
function exportLeadsExcel(){
  const leads=getCompanyScopedLeads(getLeads());
  const header='Date,Source,Nom,Téléphone,Ville,CP,Projet,Statut,Commercial,Secteur,Devis (€),CA (€),Suivi,Commentaire\n';
  const rows=leads.map(l=>{
    const comm=getAllUsers().find(u=>u.id===l.commercial);
    return[
      new Date(l.date_creation||0).toLocaleDateString('fr-FR'),
      l.source||'',csv(l.nom),csv(l.telephone),csv(l.ville),l.cp||'',
      csv(l.type_projet),l.statut,comm?.name||'',
      getLeadSecteurLabel(l.secteur),
      l.montant_devis||'',l.prix_vendu||'',
      csv(l.suivi),csv(l.commentaire)
    ].join(',');
  }).join('\n');
  download('Leads_BenAI_'+new Date().toLocaleDateString('fr-FR').replace(/\//g,'-')+'.csv',header+rows,'text/csv;charset=utf-8');
  logActivity('Export leads Excel');
}

const CRM_SECTEUR_IDS_ALL=['nimes','avignon','bagnoles','zone_blanche'];
const CRM_SECTEUR_IDS_CORE3=['nimes','avignon','bagnoles'];

/** Lignes CSV (sans en-tête) pour synthèse secteurs — ids = codes secteur (ex. nimes). */
function buildSecteurSynthDataRows(actifs,ids){
  return ids.map(s=>{
    const sl=actifs.filter(l=>l.secteur===s);
    const ventes=sl.filter(l=>l.statut==='vert');
    const devis=sl.filter(l=>l.statut==='jaune');
    const ca=ventes.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
    const pipeline=devis.reduce((sum,l)=>sum+Number(l.montant_devis||0),0);
    return [csv(getLeadSecteurLabel(s)),sl.length,ventes.length,ca,devis.length,pipeline].join(',');
  }).join('\n');
}

/** CSV complet (en-tête + lignes + ligne TOTAL optionnelle pour regroupement multi-secteurs). */
function buildSecteurSynthCSVString(actifs,ids,withTotalRow){
  const header='Secteur,Leads actifs,Ventes,CA signé (€),Devis en cours,Pipeline devis (€)\n';
  const body=buildSecteurSynthDataRows(actifs,ids);
  if(!withTotalRow||!ids.length)return header+body;
  const allSl=actifs.filter(l=>ids.includes(l.secteur));
  const ventes=allSl.filter(l=>l.statut==='vert');
  const devis=allSl.filter(l=>l.statut==='jaune');
  const ca=ventes.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
  const pipeline=devis.reduce((sum,l)=>sum+Number(l.montant_devis||0),0);
  const totalLine=[csv('TOTAL (sélection)'),allSl.length,ventes.length,ca,devis.length,pipeline].join(',');
  return header+body+'\n'+totalLine;
}

function exportSecteurSyntheseFromDashboard(){
  const sel=(document.getElementById('crm-dash-export-secteur')?.value)||'all';
  if(sel==='all'){
    exportCRMTable('secteurs');
    return;
  }
  const actifs=getCompanyScopedLeads(getLeads()).filter(l=>!l.archive);
  const stamp=new Date().toISOString().slice(0,10);
  const scopeLabel=isCRMScopePilotageRole(currentUser?.role)?`scope_${currentUser.societe||'societe'}`:'scope_global';
  let fname,content;
  if(sel==='core3'){
    fname=`CRM_Secteurs_3horsZB_${scopeLabel}_${stamp}.csv`;
    content=buildSecteurSynthCSVString(actifs,CRM_SECTEUR_IDS_CORE3,true);
  }else{
    fname=`CRM_Secteur_${sel}_${scopeLabel}_${stamp}.csv`;
    content=buildSecteurSynthCSVString(actifs,[sel],false);
  }
  download(fname,content,'text/csv;charset=utf-8');
  showDriveNotif('📥 Export secteur prêt');
  logActivity(`${currentUser?.name||'?'} a exporté synthèse secteurs (${sel})`);
}

function buildCRMExportBundles(){
  const scopedLeads=getCompanyScopedLeads(getLeads());
  const actifs=scopedLeads.filter(l=>!l.archive);
  const now=new Date();
  const currentMonth=appStorage.getItem('benai_ventes_mois')||`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  const [yr,mo]=currentMonth.split('-').map(Number);
  const leadsMonth=actifs.filter(l=>{const d=new Date(l.date_creation||0);return d.getFullYear()===yr&&d.getMonth()+1===mo;});
  const salesMonth=leadsMonth.filter(l=>l.statut==='vert');
  const stamp=new Date().toISOString().slice(0,10);
  const scopeLabel=isCRMScopePilotageRole(currentUser?.role)?`scope_${currentUser.societe||'societe'}`:'scope_global';

  const leadsHeader='Date,Source,Nom,Téléphone,Ville,CP,Projet,Statut,Commercial,Secteur,Hors secteur,Zone blanche,Devis (€),CA (€),Suivi,Commentaire\n';
  const leadsRows=actifs.map(l=>{
    const comm=getAllUsers().find(u=>u.id===l.commercial);
    return [
      new Date(l.date_creation||0).toLocaleDateString('fr-FR'),
      l.source||'',
      csv(l.nom),csv(l.telephone),csv(l.ville),l.cp||'',
      csv(l.type_projet),l.statut,csv(comm?.name||''),
      csv(getLeadSecteurLabel(l.secteur)),
      l.hors_secteur?'Oui':'Non',
      (l.zone_blanche||l.secteur==='zone_blanche')?'Oui':'Non',
      l.montant_devis||'',l.prix_vendu||'',
      csv(l.suivi),csv(l.commentaire)
    ].join(',');
  }).join('\n');

  const ventesHeader='Mois,Source,Commercial,Client,Téléphone,Ville,Produit,HT (€),Date signature\n';
  const ventesRows=salesMonth.map(l=>{
    const comm=getAllUsers().find(u=>u.id===l.commercial);
    return [currentMonth,l.source||'',csv(comm?.name||''),csv(l.nom),csv(l.telephone),csv(l.ville),csv(l.produit_vendu||l.type_projet),l.prix_vendu||'',l.date_signature||''].join(',');
  }).join('\n');

  const secteurHeader='Secteur,Leads actifs,Ventes,CA signé (€),Devis en cours,Pipeline devis (€)\n';
  const secteurRows=buildSecteurSynthDataRows(actifs,CRM_SECTEUR_IDS_ALL);

  const srcHeader='Mois,Source,Leads,Ventes,CA signé (€)\n';
  const srcRows=LEAD_SOURCE_CODES.map(src=>{
    const sl=leadsMonth.filter(l=>l.source===src);
    const ventes=sl.filter(l=>l.statut==='vert');
    const ca=ventes.reduce((sum,l)=>sum+Number(l.prix_vendu||0),0);
    return [currentMonth,csv(LEAD_SOURCE_LABELS[src]||src),sl.length,ventes.length,ca].join(',');
  }).join('\n');

  const ranking=getCommercialRankingMonth(actifs);
  const rankHeader='Mois,Rang,Commercial,Ventes,CA signé (€)\n';
  const rankRows=ranking.map((r,i)=>{
    const u=getAllUsers().find(x=>x.id===r.uid);
    return [currentMonth,i+1,csv(u?.name||r.uid),r.ventes,r.ca].join(',');
  }).join('\n');

  const lost=getLeadLostAnalytics(leadsMonth);
  const lostHeader='Mois,Motif,Leads perdus\n';
  const lostRows=(lost.rows||[]).map(r=>[currentMonth,csv(r.label),r.count].join(',')).join('\n');

  return {
    currentMonth,
    bundles:{
      leads:{filename:`CRM_Leads_${scopeLabel}_${stamp}.csv`,content:leadsHeader+leadsRows},
      ventes:{filename:`CRM_Ventes_${currentMonth}_${scopeLabel}_${stamp}.csv`,content:ventesHeader+ventesRows},
      secteurs:{filename:`CRM_Secteurs_${scopeLabel}_${stamp}.csv`,content:secteurHeader+secteurRows},
      sources:{filename:`CRM_Sources_${currentMonth}_${scopeLabel}_${stamp}.csv`,content:srcHeader+srcRows},
      classement:{filename:`CRM_Classement_${currentMonth}_${scopeLabel}_${stamp}.csv`,content:rankHeader+rankRows},
      pertes:{filename:`CRM_Pertes_${currentMonth}_${scopeLabel}_${stamp}.csv`,content:lostHeader+lostRows}
    }
  };
}

function exportCRMTable(key){
  const payload=buildCRMExportBundles();
  const item=payload.bundles[key];
  if(!item){
    showDriveNotif('⚠️ Tableau inconnu');
    return;
  }
  download(item.filename,item.content,'text/csv;charset=utf-8');
  showDriveNotif(`📥 Export ${key} prêt`);
  logActivity(`${currentUser?.name||'Utilisateur'} a exporté le tableau ${key}`);
}

function exportAllCRMTables(){
  const payload=buildCRMExportBundles();
  Object.values(payload.bundles).forEach(item=>{
    download(item.filename,item.content,'text/csv;charset=utf-8');
  });
  showDriveNotif(`📦 Tableaux CRM exportés (${formatMois(payload.currentMonth)})`);
  logActivity(`${currentUser?.name||'Utilisateur'} a exporté tous les tableaux CRM`);
}

// ══════════════════════════════════════════
// 🐛 BUGS
// ══════════════════════════════════════════
function getBugs(){try{return JSON.parse(appStorage.getItem('benai_bugs'))||[];}catch{return[];}}
function saveBugs(b){appStorage.setItem('benai_bugs',JSON.stringify(b));}
function makeAutoBugFingerprint(page,desc){
  return `${page}::${String(desc||'').toLowerCase().replace(/\s+/g,' ').trim().slice(0,180)}`;
}
/** Rapport texte prêt à coller pour un dev / une IA (plus de notifications cloche sur les bugs). */
function buildDevIncidentReport(opts){
  const page=String(opts?.page||'?');
  const desc=String(opts?.desc||'').trim();
  const gravite=String(opts?.gravite||'important');
  const extra=opts?.extra&&typeof opts.extra==='object'?opts.extra:null;
  const auto=!!opts?.auto;
  const source=String(opts?.source||page);
  const ver=(typeof BENAI_VERSION!=='undefined'&&BENAI_VERSION)?String(BENAI_VERSION):'?';
  const uid=currentUser?.id||'?';
  const uname=currentUser?.name||'?';
  const role=currentUser?.role||'?';
  let href='',ua='';
  try{href=String(location?.href||'');}catch{}
  try{ua=String(navigator?.userAgent||'').slice(0,240);}catch{}
  const lines=[
    '## Rapport pour correction (BenAI)',
    '',
    '| Champ | Valeur |',
    '|---|---|',
    `| Version app | ${ver} |`,
    `| Origine | ${auto?'Détection automatique':'Signalement utilisateur'} — ${source} |`,
    `| Utilisateur | ${uname} (\`${uid}\`) |`,
    `| Rôle | ${role} |`,
    `| Page / module | ${page} |`,
    `| Gravité | ${gravite} |`,
    '',
    '### Description',
    desc||'—',
    '',
    '### Environnement',
    `- **URL** : ${href||'—'}`,
    `- **User-Agent** : ${ua||'—'}`,
    ''
  ];
  if(extra&&Object.keys(extra).length){
    lines.push('### Données techniques (JSON)');
    lines.push('```json');
    try{lines.push(JSON.stringify(extra,null,2).slice(0,2000));}catch{lines.push(String(extra));}
    lines.push('```');
    lines.push('');
  }
  lines.push('### Reproduction (à compléter / valider par le dev)');
  lines.push('1. Ouvrir la page ou le module indiqué (compte similaire si besoin).');
  lines.push('2. Reproduire le scénario ou surveiller la console (F12 → Console).');
  lines.push('3. Vérifier la synchro Supabase si données partagées.');
  return lines.join('\n');
}
function reportAutoBug(page,desc,gravite='important',extra={}){
  if(!desc||!page)return false;
  const bugs=getBugs();
  const now=Date.now();
  const fingerprint=makeAutoBugFingerprint(page,desc);
  const already=bugs.find(b=>b.auto&&b.fingerprint===fingerprint&&b.statut==='ouvert'&&(now-(b.ts||0))<6*60*60*1000);
  if(already)return false;
  const actorName=currentUser?.name||'BenAI';
  const actorId=currentUser?.id||'benjamin';
  const row={
    id:now,
    page,
    gravite,
    desc:String(desc).slice(0,500),
    user:`${actorName} (auto)`,
    userId:actorId,
    date:new Date(now).toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}),
    statut:'ouvert',
    auto:true,
    fingerprint,
    ts:now,
    details:extra&&typeof extra==='object'?extra:null,
    rapport_dev:buildDevIncidentReport({page,desc,gravite,extra,auto:true,source:page})
  };
  bugs.unshift(row);
  saveBugs(bugs);
  refreshBugsBadge();
  return true;
}
function resolveAutoBug(page,desc,response='Auto-résolu'){
  const fingerprint=makeAutoBugFingerprint(page,desc);
  const bugs=getBugs();
  const nowStr=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  let changed=false;
  bugs.forEach(b=>{
    if(b?.auto&&b?.statut==='ouvert'&&b?.fingerprint===fingerprint){
      b.statut='resolu';
      b.reponse=response;
      b.date_resolution=nowStr;
      changed=true;
    }
  });
  if(!changed)return false;
  saveBugs(bugs);
  if(currentUser?.role==='admin')renderBugsList();
  refreshBugsBadge();
  return true;
}
function attemptAutoHealCRMLeads(){
  const leads=getLeads();
  if(!Array.isArray(leads)||!leads.length)return {changed:false,fixes:0,summary:''};
  let changed=false;
  let fixes=0;
  let fixedDuplicates=0,fixedRdv=0,fixedStatut=0,fixedShape=0;
  const validStatuts=['gris','rdv','jaune','vert','rouge'];
  const usedIds={};
  const nowIso=new Date().toISOString();
  leads.forEach((l,idx)=>{
    if(!l||typeof l!=='object')return;
    // Réparer forme minimale
    if(!Array.isArray(l.timeline)){l.timeline=[];changed=true;fixes++;fixedShape++;}
    if(!Array.isArray(l.rdv_history)){l.rdv_history=[];changed=true;fixes++;fixedShape++;}
    if(!l.date_creation){l.date_creation=nowIso;changed=true;fixes++;fixedShape++;}
    if(!l.date_modification){l.date_modification=nowIso;changed=true;fixes++;fixedShape++;}
    if(!l.source||!LEAD_SOURCE_CODES.includes(l.source)){l.source='MAG';changed=true;fixes++;fixedShape++;}
    if(!l.secteur||!['nimes','avignon','bagnoles','zone_blanche'].includes(l.secteur)){l.secteur='nimes';changed=true;fixes++;fixedShape++;}
    // Statut invalide
    if(!validStatuts.includes(l.statut)){l.statut='gris';changed=true;fixes++;fixedStatut++;}
    // RDV incohérent: pas de date -> retour en à rappeler
    if((l.statut==='rdv'||l.sous_statut==='rdv_programme')&&!l.rappel&&!l.date_rdv_fait){
      l.statut='gris';
      l.sous_statut='a_rappeler';
      l.date_modification=nowIso;
      changed=true;fixes++;fixedRdv++;
      addLeadTimelineEntry(l,'Auto-correction: RDV sans date rebasculé en "à rappeler"','BenAI Auto-fix');
    }
    // Doublons ID
    const baseId=(l.id===undefined||l.id===null||l.id==='')?Date.now()+idx:l.id;
    const key=String(baseId);
    if(usedIds[key]){
      l.id=Date.now()+idx+Math.floor(Math.random()*1000);
      changed=true;fixes++;fixedDuplicates++;
    }
    usedIds[String(l.id)]=1;
  });
  if(changed){
    saveLeads(leads);
  }
  const summaryParts=[];
  if(fixedShape>0)summaryParts.push(`${fixedShape} forme`);
  if(fixedStatut>0)summaryParts.push(`${fixedStatut} statut`);
  if(fixedRdv>0)summaryParts.push(`${fixedRdv} RDV`);
  if(fixedDuplicates>0)summaryParts.push(`${fixedDuplicates} doublon ID`);
  return {changed,fixes,summary:summaryParts.join(', ')};
}
function runAutoBugDetectors(){
  const now=Date.now();
  if(now-lastAutoBugScanTs<2*60*1000)return;
  lastAutoBugScanTs=now;
  // Auto-correction safe CRM
  try{
    const heal=attemptAutoHealCRMLeads();
    if(heal.changed){
      reportAutoBug('CRM Leads',`Auto-correction appliquée (${heal.summary||`${heal.fixes} correction(s)`}).`,'mineur',{autoFixed:true,fixes:heal.fixes,summary:heal.summary});
      logActivity(`BenAI auto-correction CRM: ${heal.summary||heal.fixes+' correction(s)'}`);
    }
  }catch(e){
    reportAutoBug('CRM Leads',`Auto-correction CRM en erreur: ${e?.message||'erreur inconnue'}`,'mineur');
  }
  // Détecteur CRM: incohérences de données leads.
  try{
    const leads=getLeads();
    if(!Array.isArray(leads)||!leads.length)return;
    let missingRequired=0,rdvWithoutDate=0,duplicateId=0;
    const idMap={};
    leads.forEach(l=>{
      if(!l||typeof l!=='object')return;
      if(!l.nom||!l.telephone||!l.type_projet)missingRequired++;
      if((l.statut==='rdv'||l.sous_statut==='rdv_programme')&&!l.rappel&&!l.date_rdv_fait)rdvWithoutDate++;
      if(l.id!==undefined&&l.id!==null){
        idMap[l.id]=(idMap[l.id]||0)+1;
      }
    });
    duplicateId=Object.values(idMap).filter(v=>v>1).length;
    if(missingRequired>0){
      reportAutoBug('CRM Leads',`${missingRequired} lead(s) sans infos obligatoires (nom/téléphone/projet).`,'important',{missingRequired});
    }
    if(rdvWithoutDate>0){
      reportAutoBug('CRM Leads',`${rdvWithoutDate} lead(s) en statut RDV sans date planifiée/effectuée.`,'important',{rdvWithoutDate});
    }
    if(duplicateId>0){
      reportAutoBug('CRM Leads',`${duplicateId} identifiant(s) de lead en doublon détecté(s).`,'bloquant',{duplicateId});
    }
  }catch(e){
    reportAutoBug('CRM Leads',`Détecteur CRM en erreur: ${e?.message||'erreur inconnue'}`,'mineur');
  }
}
function resolveNotificationConstructorAutoBugs(){
  const bugs=getBugs();
  let changed=false;
  const nowStr=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  bugs.forEach(b=>{
    const txt=String(b?.desc||'').toLowerCase();
    if(b?.statut==='ouvert'&&/failed to construct ['"]?notification['"]?|illegal constructor|serviceworkerregistration\.shownotification/.test(txt)){
      b.statut='resolu';
      b.reponse='Erreur navigateur connue. Notifications OS désactivées automatiquement pour éviter le spam.';
      b.date_resolution=nowStr;
      changed=true;
    }
  });
  if(!changed)return false;
  saveBugs(bugs);
  refreshBugsBadge();
  return true;
}
function setupAutomaticBugHooks(){
  if(window.__benaiAutoBugHooksReady)return;
  window.__benaiAutoBugHooksReady=true;
  resolveNotificationConstructorAutoBugs();
  window.addEventListener('error',evt=>{
    const msg=String(evt?.message||'Erreur JS inconnue');
    // Filtrer quelques bruits navigateur fréquents.
    if(/resizeobserver loop limit exceeded/i.test(msg))return;
    if(/failed to construct ['"]?notification['"]?|illegal constructor|serviceworkerregistration\.shownotification/i.test(msg)){
      disableBrowserNotifications(msg);
      resolveNotificationConstructorAutoBugs();
      return;
    }
    const src=evt?.filename?` (${evt.filename.split('/').pop()}:${evt.lineno||0})`:'';
    reportAutoBug('BenAI IA',`Erreur JavaScript: ${msg}${src}`,'bloquant');
  });
  window.addEventListener('unhandledrejection',evt=>{
    const reason=evt?.reason;
    const msg=typeof reason==='string'?reason:(reason?.message||JSON.stringify(reason)||'Promise rejetée sans détail');
    reportAutoBug('BenAI IA',`Promise rejetée: ${String(msg).slice(0,300)}`,'important');
  });
}
setupAutomaticBugHooks();
function setupSafeSelfHealing(){
  if(window.__benaiSelfHealingReady)return;
  window.__benaiSelfHealingReady=true;
  setInterval(async()=>{
    if(!currentUser)return;
    // Si le polling est tombé, on le relance automatiquement.
    const staleMs=Date.now()-lastPollingHeartbeat;
    if(!pollingInterval||staleMs>90000){
      startPolling();
      reportAutoBug('BenAI IA','Auto-correction: polling relancé automatiquement.','mineur',{autoFixed:true,staleMs});
      if(Date.now()-lastSelfHealToastTs>15*60*1000){
        showDriveNotif('🩹 Auto-correction BenAI: surveillance relancée');
        lastSelfHealToastTs=Date.now();
      }
    }
    // Si Supabase est activé mais session manquante, tenter une récupération douce.
    if(SUPABASE_CONFIG.enabled){
      if(shouldMonitorSupabaseSession()){
        const sess=await ensureSupabaseSession();
        if(!sess?.access_token){
          reportAutoBug('BenAI IA','Session Supabase absente malgré auto-récupération.','important',{autoFixed:false});
        }else{
          resolveAutoBug('BenAI IA','Session Supabase absente malgré auto-récupération.','Session Supabase revenue, alerte fermée automatiquement.');
        }
      }else{
        // Mode local ou preview: cette alerte n'est pas pertinente.
        resolveAutoBug('BenAI IA','Session Supabase absente malgré auto-récupération.','Mode local/preview détecté, alerte fermée automatiquement.');
      }
    }
  },60000);
}
setupSafeSelfHealing();

let currentBugFilter='tous';

function openBugReportOverlay(){
  const ov=document.getElementById('bug-rpt-overlay');
  if(!ov)return;
  const d=document.getElementById('rpt-desc');
  if(d)d.value='';
  ov.style.display='flex';
}
function closeBugReportOverlay(){
  const ov=document.getElementById('bug-rpt-overlay');
  if(ov)ov.style.display='none';
}
async function submitBugReportOverlay(){
  const desc=(document.getElementById('rpt-desc')?.value||'').trim();
  if(!desc){alert('Décris le problème avant d’envoyer.');return;}
  const pageVal=document.getElementById('rpt-page')?.value||'Autre';
  const gravVal=document.getElementById('rpt-gravite')?.value||'important';
  const bugs=getBugs();
  const now=new Date();
  bugs.unshift({
    id:Date.now(),
    page:pageVal,
    gravite:gravVal,
    desc,
    user:currentUser.name,
    userId:currentUser.id,
    date:now.toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}),
    statut:'ouvert',
    rapport_dev:buildDevIncidentReport({page:pageVal,desc,gravite:gravVal,extra:{canal:'overlay_signalement'},auto:false,source:'Signalement utilisateur'})
  });
  saveBugs(bugs);
  closeBugReportOverlay();
  if(currentUser?.role==='admin')renderBugsList();
  refreshBugsBadge();
  logActivity(`${currentUser.name} a signalé un problème`);
  showDriveNotif('✅ Signalement envoyé — merci !');
}

function openNewBug(){
  document.getElementById('bug-form-wrap').style.display='block';
  document.getElementById('bug-desc').value='';
}

function saveBug(){
  const desc=document.getElementById('bug-desc').value.trim();
  if(!desc){alert('Décrivez le bug');return;}
  const bugs=getBugs();
  const now=new Date();
  const pageVal=document.getElementById('bug-page').value;
  const gravVal=document.getElementById('bug-gravite').value;
  bugs.unshift({
    id:Date.now(),
    page:pageVal,
    gravite:gravVal,
    desc,
    user:currentUser.name,
    userId:currentUser.id,
    date:now.toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'}),
    statut:'ouvert',
    rapport_dev:buildDevIncidentReport({page:pageVal,desc,gravite:gravVal,extra:{canal:'formulaire_bugs'},auto:false,source:'Signalement manuel'})
  });
  saveBugs(bugs);
  document.getElementById('bug-form-wrap').style.display='none';
  document.getElementById('bug-desc').value='';
  if(currentUser?.role==='admin')renderBugsList();
  refreshBugsBadge();
  logActivity(`${currentUser.name} a signalé un bug`);
  showDriveNotif('✅ Bug signalé — merci !');
}

function filterBugs(f,btn){
  currentBugFilter=f;
  document.querySelectorAll('#bug-filter-bar .crm-filter').forEach(b=>b.classList.remove('active'));
  if(btn)btn.classList.add('active');
  renderBugsList();
}

function initBugsPage(){
  if(currentUser?.role!=='admin'){
    const allowed=ROLE_PAGES[currentUser?.role]||ROLE_PAGES['assistante'];
    showPage(allowed[0]||'guide');
    return;
  }
  const filterBar=document.getElementById('bug-filter-bar');
  if(filterBar)filterBar.style.display='flex';
  renderBugsList();
  refreshBugsBadge();
}

function renderBugsList(){
  const list=document.getElementById('bugs-list');if(!list)return;
  const role=currentUser?.role;
  if(role!=='admin'){list.innerHTML='';return;}
  let bugs=getBugs();
  // Filtres admin
  if(currentBugFilter==='ouvert')bugs=bugs.filter(b=>b.statut==='ouvert');
  else if(currentBugFilter==='resolu')bugs=bugs.filter(b=>b.statut==='resolu');
  else if(currentBugFilter==='bloquant')bugs=bugs.filter(b=>b.gravite==='bloquant');
  if(!bugs.length){list.innerHTML='<div style="color:var(--t3);font-size:13px;padding:20px;text-align:center">Aucun bug signalé 🎉</div>';return;}
  const graviteIcon={bloquant:'🔴',important:'🟡',mineur:'🟢'};
  list.innerHTML=bugs.map(b=>`
    <div style="background:var(--s2);border:1px solid ${b.statut==='resolu'?'var(--g)':'var(--b1)'};border-radius:12px;padding:13px 15px;margin-bottom:8px;${b.statut==='resolu'?'opacity:.7':''}">
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap">
        <span>${graviteIcon[b.gravite]||'🟡'}</span>
        <span style="font-size:12px;font-weight:700">${esc(b.page)}</span>
        <span style="font-size:10px;background:var(--s3);padding:2px 8px;border-radius:10px;color:var(--t2)">${b.gravite}</span>
        ${b.statut==='resolu'?'<span style="background:var(--g2);color:var(--g);padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700">✅ Résolu</span>':''}
        <span style="margin-left:auto;font-size:10px;color:var(--t3)">${b.date}</span>
      </div>
      <div style="font-size:13px;color:var(--t1);margin-bottom:6px">${esc(b.desc)}</div>
      <div style="font-size:10px;color:var(--t3)">Signalé par ${esc(b.user)}</div>
      ${b.rapport_dev?`<details style="margin-top:10px;font-size:11px"><summary style="cursor:pointer;color:var(--a);font-weight:600">📋 Rapport pour correction (copier / IA)</summary><pre style="white-space:pre-wrap;max-height:220px;overflow:auto;background:var(--s3);padding:10px;border-radius:8px;margin-top:6px;border:1px solid var(--b1);color:var(--t2);font-family:ui-monospace,monospace;font-size:10px">${esc(b.rapport_dev)}</pre><button type="button" onclick="copyBugRapport(${b.id})" style="margin-top:6px;padding:6px 12px;background:var(--a3);border:1px solid var(--a);border-radius:8px;color:var(--t1);font-size:11px;font-weight:600;cursor:pointer;font-family:inherit">Copier le rapport</button></details>`:''}
      ${role==='admin'&&b.statut==='ouvert'?`
        <div style="margin-top:8px;display:flex;gap:6px">
          <input type="text" id="bug-rep-${b.id}" placeholder="Réponse / solution..." style="flex:1;padding:7px 10px;background:var(--s3);border:1px solid var(--b1);border-radius:7px;color:var(--t1);font-family:inherit;font-size:12px;outline:none">
          <button onclick="resolveBug(${b.id})" style="padding:7px 12px;background:var(--g);color:#fff;border:none;border-radius:7px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit">✅ Résoudre</button>
        </div>`:
      b.reponse?`<div style="margin-top:6px;padding:6px 10px;background:var(--g2);border-radius:6px;font-size:11px;color:var(--g)">✅ ${esc(b.reponse)}</div>`:''}
    </div>`).join('');
}

function resolveBug(id){
  const bugs=getBugs();
  const b=bugs.find(x=>x.id===id);if(!b)return;
  const rep=document.getElementById('bug-rep-'+id)?.value.trim()||'Corrigé';
  b.statut='resolu';b.reponse=rep;b.date_resolution=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  saveBugs(bugs);
  if(currentUser?.role==='admin')renderBugsList();
  refreshBugsBadge();
  logActivity(`Benjamin a résolu un bug : ${b.page}`);
}

function copyBugRapport(id){
  const b=getBugs().find(x=>x.id===id);
  const txt=String(b?.rapport_dev||'').trim();
  if(!txt){showDriveNotif('Aucun rapport pour ce ticket');return;}
  const runCopy=()=>{
    showDriveNotif('Rapport copié dans le presse-papiers');
    try{logActivity(`Rapport bug #${id} copié`);}catch{}
  };
  if(navigator.clipboard&&navigator.clipboard.writeText){
    navigator.clipboard.writeText(txt).then(runCopy).catch(()=>{
      try{
        const ta=document.createElement('textarea');
        ta.value=txt;ta.style.position='fixed';ta.style.left='-9999px';
        document.body.appendChild(ta);ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        runCopy();
      }catch{
        showDriveNotif('Copie impossible — sélectionnez le texte dans le rapport');
      }
    });
  }else{
    showDriveNotif('Copie non disponible sur ce navigateur');
  }
}

function refreshBugsBadge(){
  const badge=document.getElementById('bugs-badge');if(!badge)return;
  if(currentUser?.role!=='admin'){badge.style.display='none';return;}
  const bugs=getBugs().filter(b=>b.statut==='ouvert');
  if(bugs.length>0){badge.style.display='flex';badge.textContent=bugs.length;}
  else badge.style.display='none';
}

// ══════════════════════════════════════════
// 📅 JOURS FÉRIÉS FRANCE (hors weekend)
// ══════════════════════════════════════════
function getJoursFeries(year){
  const y=year||new Date().getFullYear();
  // Calcul Pâques (algorithme de Meeus)
  const a=y%19,b=Math.floor(y/100),c=y%100;
  const d=Math.floor(b/4),e=b%4,f=Math.floor((b+8)/25),g=Math.floor((b-f+1)/3);
  const h=(19*a+b-d-g+15)%30,i=Math.floor(c/4),k=c%4;
  const l=(32+2*e+2*i-h-k)%7,m=Math.floor((a+11*h+22*l)/451);
  const month=Math.floor((h+l-7*m+114)/31),day=((h+l-7*m+114)%31)+1;
  const paques=new Date(y,month-1,day);
  const jf=[
    new Date(y,0,1),   // 1er janvier
    new Date(y,4,1),   // Fête du travail
    new Date(y,4,8),   // Victoire 1945
    new Date(y,6,14),  // Bastille
    new Date(y,7,15),  // Assomption
    new Date(y,10,1),  // Toussaint
    new Date(y,10,11), // Armistice
    new Date(y,11,25), // Noël
  ];
  // Jours liés à Pâques
  const lundi_paques=new Date(paques);lundi_paques.setDate(paques.getDate()+1);
  const ascension=new Date(paques);ascension.setDate(paques.getDate()+39);
  const pentecote=new Date(paques);pentecote.setDate(paques.getDate()+50);
  jf.push(lundi_paques,ascension,pentecote);
  return jf.map(d=>d.toDateString());
}

function isJourOuvre(date){
  const d=date||new Date();
  const day=d.getDay();
  if(day===0||day===6)return false; // weekend
  const feries=getJoursFeries(d.getFullYear());
  return !feries.includes(d.toDateString());
}

function addJoursOuvres(date,jours){
  const d=new Date(date);let added=0;
  while(added<jours){
    d.setDate(d.getDate()+1);
    if(isJourOuvre(d))added++;
  }
  return d;
}

function getHeuresOuvrees(since){
  // Calcul rapide sans boucle — math direct
  const now=new Date();
  const start=new Date(since);
  if(start>=now)return 0;
  const feries=new Set(getJoursFeries(start.getFullYear()).concat(getJoursFeries(now.getFullYear())));
  let heures=0;
  let d=new Date(start);
  // Aller au prochain créneau ouvré
  let iterations=0;
  while(d<now&&iterations<10000){
    iterations++;
    const day=d.getDay();
    const h=d.getHours();
    const dateStr=d.toDateString();
    if(day!==0&&day!==6&&!feries.has(dateStr)&&h>=8&&h<19){
      heures++;
    }
    d.setHours(d.getHours()+1);
  }
  return heures;
}

// ══════════════════════════════════════════
// 🎯 OBJECTIFS COMMERCIAUX
// ══════════════════════════════════════════
function getObjectifsCommerciaux(){
  try{return JSON.parse(appStorage.getItem('benai_obj_comm'))||{};}catch{return{};}
}
function saveObjectifsCommerciaux(o){appStorage.setItem('benai_obj_comm',JSON.stringify(o));}

function setObjectifCommercial(uid,mensuel,hebdo){
  const obj=getObjectifsCommerciaux();
  obj[uid]={mensuel:Number(mensuel)||0,hebdo:Number(hebdo)||0};
  saveObjectifsCommerciaux(obj);
}

function getProgressionCommercial(uid){
  const leads=getCompanyScopedLeads(getLeads()).filter(l=>l.statut==='vert'&&l.commercial===uid);
  const now=new Date();
  const month=now.getMonth(),year=now.getFullYear();
  const week=getWeekNumber(now);
  const mensuel=leads.filter(l=>{const d=new Date(l.date_creation||0);return d.getMonth()===month&&d.getFullYear()===year;});
  const hebdo=leads.filter(l=>{const d=new Date(l.date_creation||0);return getWeekNumber(d)===week&&d.getFullYear()===year;});
  const caMensuel=mensuel.reduce((s,l)=>s+Number(l.prix_vendu||0),0);
  const caHebdo=hebdo.reduce((s,l)=>s+Number(l.prix_vendu||0),0);
  const obj=getObjectifsCommerciaux()[uid]||{mensuel:0,hebdo:0};
  return{ventes_mois:mensuel.length,ventes_semaine:hebdo.length,ca_mois:caMensuel,ca_semaine:caHebdo,obj};
}

function getWeekNumber(d){
  const date=new Date(d);date.setHours(0,0,0,0);
  date.setDate(date.getDate()+3-(date.getDay()+6)%7);
  const week1=new Date(date.getFullYear(),0,4);
  return 1+Math.round(((date.getTime()-week1.getTime())/86400000-3+(week1.getDay()+6)%7)/7);
}

// Message motivation BenAI automatique
async function genererMessageMotivation(uid,contexte){
  const apiKey=getApiKey();if(!apiKey)return;
  const prog=getProgressionCommercial(uid);
  const user=getAllUsers().find(u=>u.id===uid);if(!user)return;
  const pctMois=prog.obj.mensuel>0?Math.round(prog.ca_mois/prog.obj.mensuel*100):0;
  const prompt=`Tu es BenAI, assistant du directeur commercial. Génère un message de motivation court (2-3 phrases max) et TRÈS personnalisé pour ${user.name}. 
Contexte : ${contexte}
Données réelles : ${prog.ventes_mois} ventes ce mois, CA ${prog.ca_mois.toLocaleString('fr-FR')}€, objectif ${prog.obj.mensuel.toLocaleString('fr-FR')}€ (${pctMois}% atteint), ${prog.ventes_semaine} ventes cette semaine.
Le message doit mentionner ses vrais chiffres. Tutoie-le. Sois enthousiaste mais authentique. Pas de formule générique.`;
  try{
    const res=await fetch('https://api.anthropic.com/v1/messages',{
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':apiKey,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:'claude-sonnet-4-20250514',max_tokens:200,messages:[{role:'user',content:prompt}]})
    });
    const data=await res.json();
    if(data.content?.[0]?.text){
      const msg=data.content[0].text.trim();
      appendBenAIMotivationInternalMessage(uid,msg);
    }
  }catch(e){}
}

// Vérifier motivations automatiques
function checkMotivationsAuto(){
  if(!currentUser||currentUser.role!=='commercial')return;
  const uid=currentUser.id;
  const prog=getProgressionCommercial(uid);
  const pct=prog.obj.mensuel>0?prog.ca_mois/prog.obj.mensuel:0;
  const lastMotiv=appStorage.getItem('benai_last_motiv_'+uid)||'';
  const today=new Date().toDateString();
  if(lastMotiv===today)return;
  let contexte='';
  if(pct>=1)contexte='Objectif mensuel dépassé ! Encourage-le à se surpasser encore.';
  else if(pct>=0.75)contexte='Presque à l\'objectif mensuel, dans la dernière ligne droite.';
  else if(new Date().getDay()===1)contexte='C\'est lundi, début de semaine, donne-lui de l\'élan.';
  else if(prog.ventes_semaine===0&&new Date().getDay()>=3)contexte='Pas encore de vente cette semaine, encourage-le à relancer.';
  else return;
  appStorage.setItem('benai_last_motiv_'+uid,today);
  setTimeout(()=>genererMessageMotivation(uid,contexte),3000);
}

// ══════════════════════════════════════════
// 📊 TABLEAU DE VENTES MENSUEL
// ══════════════════════════════════════════
function renderTableauVentes(container){
  const leads=getCompanyScopedLeads(getLeads()).filter(l=>l.statut==='vert');
  const now=new Date();
  // Sélecteur mois
  const moisDispos=[...new Set(leads.map(l=>{const d=new Date(l.date_creation||0);return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`}))].sort().reverse();
  const selectedMois=appStorage.getItem('benai_ventes_mois')||`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  const [yr,mo]=selectedMois.split('-').map(Number);
  const leadsFiltered=leads.filter(l=>{const d=new Date(l.date_creation||0);return d.getFullYear()===yr&&d.getMonth()+1===mo;});
  const total=leadsFiltered.reduce((s,l)=>s+Number(l.prix_vendu||0),0);
  const srcIcons2=LEAD_SOURCE_ICONS;
  const html=`
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
      <div style="font-size:15px;font-weight:700">📈 Tableau de ventes</div>
      <div style="display:flex;gap:8px;align-items:center">
        <select onchange="window.appStorage.setItem('benai_ventes_mois',this.value);renderLeadsDashboard()" style="background:var(--s2);border:1px solid var(--b1);border-radius:8px;padding:6px 10px;font-size:12px;color:var(--t1);outline:none;font-family:'Outfit',sans-serif">
          ${moisDispos.map(m=>`<option value="${m}"${m===selectedMois?' selected':''}>${formatMois(m)}</option>`).join('')}
        </select>
        <button onclick="exportTableauVentes()" style="padding:6px 12px;background:var(--g2);color:var(--g);border:1px solid rgba(34,197,94,.3);border-radius:8px;font-size:11px;font-weight:600;cursor:pointer;font-family:inherit">📥 Export</button>
      </div>
    </div>
    <div style="font-size:22px;font-weight:800;color:var(--g);margin-bottom:12px">${total.toLocaleString('fr-FR')} € HT <span style="font-size:12px;color:var(--t3);font-weight:400">${leadsFiltered.length} ventes</span></div>
    <div style="overflow-x:auto">
      <table style="width:100%;border-collapse:collapse;font-size:12px">
        <thead><tr style="background:var(--s3)">
          <th style="padding:7px 10px;text-align:left;color:var(--t3);font-size:10px;text-transform:uppercase">Src</th>
          <th style="padding:7px 10px;text-align:left;color:var(--t3);font-size:10px;text-transform:uppercase">Commercial</th>
          <th style="padding:7px 10px;text-align:left;color:var(--t3);font-size:10px;text-transform:uppercase">Client</th>
          <th style="padding:7px 10px;text-align:left;color:var(--t3);font-size:10px;text-transform:uppercase">Produit</th>
          <th style="padding:7px 10px;text-align:right;color:var(--t3);font-size:10px;text-transform:uppercase">HT (€)</th>
        </tr></thead>
        <tbody>
          ${leadsFiltered.map(l=>{
            const comm=getAllUsers().find(u=>u.id===l.commercial);
            return `<tr style="border-top:1px solid var(--b1)">
              <td style="padding:8px 10px">${srcIcons2[l.source]||'📋'}</td>
              <td style="padding:8px 10px;font-weight:500">${esc(comm?.name||'—')}</td>
              <td style="padding:8px 10px">${esc(l.nom)}</td>
              <td style="padding:8px 10px;color:var(--t2)">${esc(l.produit_vendu||l.type_projet||'—')}</td>
              <td style="padding:8px 10px;text-align:right;font-weight:700;color:var(--g)">${Number(l.prix_vendu||0).toLocaleString('fr-FR')}</td>
            </tr>`;
          }).join('')}
        </tbody>
        <tfoot><tr style="background:var(--a3);border-top:2px solid var(--a)">
          <td colspan="4" style="padding:8px 10px;font-weight:700;color:var(--a)">TOTAL</td>
          <td style="padding:8px 10px;text-align:right;font-weight:800;font-size:14px;color:var(--a)">${total.toLocaleString('fr-FR')}</td>
        </tr></tfoot>
      </table>
    </div>`;
  if(container)container.innerHTML=html;
}

function formatMois(str){
  if(!str)return '';const[y,m]=str.split('-');
  const mois=['Jan','Fév','Mar','Avr','Mai','Jun','Jul','Aoû','Sep','Oct','Nov','Déc'];
  return `${mois[Number(m)-1]} ${y}`;
}

function exportTableauVentes(){
  const leads=getCompanyScopedLeads(getLeads()).filter(l=>l.statut==='vert');
  const selectedMois=appStorage.getItem('benai_ventes_mois')||'';
  const [yr,mo]=selectedMois?selectedMois.split('-').map(Number):[new Date().getFullYear(),new Date().getMonth()+1];
  const filtered=selectedMois?leads.filter(l=>{const d=new Date(l.date_creation||0);return d.getFullYear()===yr&&d.getMonth()+1===mo;}):leads;
  const header='Source,Commercial,Client,Téléphone,Ville,Produit,HT (€),Date signature\n';
  const rows=filtered.map(l=>{
    const comm=getAllUsers().find(u=>u.id===l.commercial);
    return[l.source||'',csv(comm?.name||''),csv(l.nom),csv(l.telephone),csv(l.ville),csv(l.produit_vendu||l.type_projet),l.prix_vendu||'',l.date_signature||''].join(',');
  }).join('\n');
  download(`Ventes_${selectedMois||'export'}.csv`,header+rows,'text/csv;charset=utf-8');
}

// ══════════════════════════════════════════
// 📞 TEL + GPS
// ══════════════════════════════════════════
function makeCallLink(tel){
  if(!tel)return '';
  const clean=tel.replace(/\s/g,'');
  return `<a href="tel:${clean}" onclick="event.stopPropagation()" style="color:var(--bl);font-size:11px;text-decoration:none;background:var(--bl2);padding:2px 7px;border-radius:6px;font-weight:600">📞 Appeler</a>`;
}

function makeLeadCallLink(leadId,tel){
  if(!tel)return '';
  const clean=tel.replace(/\s/g,'');
  return `<a href="tel:${clean}" onclick="event.stopPropagation();logLeadCall(${leadId})" style="color:var(--bl);font-size:11px;text-decoration:none;background:var(--bl2);padding:2px 7px;border-radius:6px;font-weight:600">📞 Appeler</a>`;
}

function logLeadCall(leadId){
  const leads=getLeads();
  const idx=leads.findIndex(l=>l.id===leadId);
  if(idx===-1)return;
  const lead=leads[idx];
  lead.call_count=(lead.call_count||0)+1;
  lead.last_call_at=new Date().toISOString();
  addLeadTimelineEntry(lead,'Appel téléphonique lancé',currentUser?.name||'Utilisateur');
  leads[idx]=lead;
  saveLeads(leads);
}

function makeGPSLink(adresse,ville,cp){
  const q=encodeURIComponent([adresse,cp,ville].filter(Boolean).join(' '));
  const pill='font-size:11px;text-decoration:none;padding:2px 7px;border-radius:6px;font-weight:600;white-space:nowrap';
  return `<span style="display:inline-flex;align-items:center;gap:3px;flex-wrap:wrap" onclick="event.stopPropagation()">
    <a href="https://maps.google.com/?q=${q}" target="_blank" rel="noopener" style="color:var(--g);background:var(--g2);${pill}">🗺️ Maps</a>
    <a href="https://www.waze.com/ul?q=${q}" target="_blank" rel="noopener" style="color:#fff;background:#33ccff;${pill}">🚗 Waze</a>
  </span>`;
}

// ══════════════════════════════════════════
// 🚨 RAPPORT ALERTES DIRECTEUR CO
// ══════════════════════════════════════════
function genererRapportAlertes(){
  const leads=getCompanyScopedLeads(getLeads()).filter(l=>!l.archive&&l.statut!=='vert'&&l.statut!=='rouge');
  const now=new Date();
  const nonAttrib=leads.filter(l=>!l.commercial);
  const nonOuverts=leads.filter(l=>l.commercial&&!l.vu_date);
  const enRetard=leads.filter(l=>l.commercial&&l.vu_date&&isLeadAlerte(l));
  const devisSansReponse=leads.filter(l=>l.statut==='jaune'&&l.rappel_devis&&new Date(l.rappel_devis)<now);
  return{nonAttrib,nonOuverts,enRetard,devisSansReponse,total:nonAttrib.length+nonOuverts.length+enRetard.length+devisSansReponse.length};
}

// ══════════════════════════════════════════
// 🔒 BLOQUER COMMERCIAL
// ══════════════════════════════════════════
function toggleBlockCommercial(uid){
  if(!canManageBenaiUsersAdmin()){alert('Réservé à l’administrateur.');return;}
  const access=getAccess();
  const u=getAllUsers().find(x=>x.id===uid);
  const isBlocked=access[uid]===false;
  if(!confirm(`${isBlocked?'Débloquer':'Bloquer'} ${u?.name||uid} du CRM ?`))return;
  access[uid]=isBlocked?true:false;
  saveAccess(access);
  renderUsersList();
  pushNotif(isBlocked?'✅ Accès rétabli':'🔒 Accès bloqué',`${u?.name||uid} — CRM`,'🔒','benjamin');
  logActivity(`${currentUser.name} a ${isBlocked?'débloqué':'bloqué'} ${u?.name||uid}`);
  showDriveNotif(isBlocked?`✅ ${u?.name} débloqué`:`🔒 ${u?.name} bloqué`);
}

// ══════════════════════════════════════════
// 📊 COMPTEUR CONTACTS PAR COMMERCIAL
// ══════════════════════════════════════════
function getContactsParCommercial(){
  const leads=getCompanyScopedLeads(getLeads());
  const result={};
  leads.forEach(l=>{
    if(!l.commercial)return;
    if(!result[l.commercial])result[l.commercial]={total:0,cette_semaine:0,ce_mois:0};
    result[l.commercial].total++;
    const d=new Date(l.date_creation||0);
    const now=new Date();
    if(d.getMonth()===now.getMonth()&&d.getFullYear()===now.getFullYear())result[l.commercial].ce_mois++;
    if(getWeekNumber(d)===getWeekNumber(now)&&d.getFullYear()===now.getFullYear())result[l.commercial].cette_semaine++;
  });
  return result;
}

// ══════════════════════════════════════════
// 🗑️ JOURNAL DES SUPPRESSIONS
// ══════════════════════════════════════════
function logDeletion(type,nom){
  try{
    const key='benai_deletions';
    const logs=JSON.parse(appStorage.getItem(key)||'[]');
    logs.unshift({
      type,nom:nom||'?',
      user:currentUser?.name||'?',
      date:new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'})
    });
    appStorage.setItem(key,JSON.stringify(logs));
  }catch(e){}
}

function getDeletions(){
  try{return JSON.parse(appStorage.getItem('benai_deletions'))||[];}catch{return[];}
}

// ══════════════════════════════════════════
// 🎂 RAPPEL ANNIVERSAIRES
// ══════════════════════════════════════════
function checkAnniversaires(){
  if(currentUser?.id!=='benjamin')return;
  const today=new Date();
  const ann=getAnnuaireActive();
  const today_key=`${today.getDate()}-${today.getMonth()}`;
  const seen=appStorage.getItem('benai_anniv_seen_'+today_key);
  if(seen)return;
  const anniversaires=ann.filter(e=>{
    if(!e.naissance)return false;
    const d=new Date(e.naissance);
    return d.getDate()===today.getDate()&&d.getMonth()===today.getMonth();
  });
  if(anniversaires.length>0){
    anniversaires.forEach(e=>{
      const age=today.getFullYear()-new Date(e.naissance).getFullYear();
      pushBenAINotif('🎂 Anniversaire',`${e.prenom} ${e.nom} a ${age} ans aujourd'hui !`,'🎂','benjamin');
    });
    appStorage.setItem('benai_anniv_seen_'+today_key,'1');
  }
}

// ══════════════════════════════════════════
// 📅 GOOGLE AGENDA LEAD
// ══════════════════════════════════════════
function makeGoogleAgendaLink(l){
  const rdv=l.date_rdv||l.rappel;
  if(!rdv)return '';
  const start=new Date(rdv);
  const end=new Date(start.getTime()+60*60*1000);
  const fmt=d=>d.toISOString().replace(/-|:|\.\d{3}/g,'');
  const titre=encodeURIComponent('RDV '+l.nom+' — '+(l.type_projet||''));
  const details=encodeURIComponent('Client: '+l.nom+'\nTél: '+(l.telephone||'')+'\nProjet: '+(l.type_projet||'')+'\nCommentaire: '+(l.commentaire||''));
  const lieu=encodeURIComponent([l.adresse,l.cp,l.ville].filter(Boolean).join(' '));
  return 'https://calendar.google.com/calendar/render?action=TEMPLATE&text='+titre+'&dates='+fmt(start)+'/'+fmt(end)+'&details='+details+'&location='+lieu;
}

function ouvrirAgenda(){
  if(!currentLeadId)return;
  const l=getLeads().find(x=>x.id===currentLeadId);
  const dateRef=l?.rappel||l?.date_rdv;
  if(!l||!dateRef){showDriveNotif('⚠️ Aucun RDV programmé sur ce lead');return;}
  const url=makeGoogleAgendaLink(l);
  window.open(url,'_blank');
  const leads=getLeads();
  const idx=leads.findIndex(x=>x.id===currentLeadId);
  if(idx!==-1){
    addLeadTimelineEntry(leads[idx],'RDV envoyé vers Google Agenda',currentUser.name);
    saveLeads(leads);
  }
  logActivity(currentUser.name+' a ajouté le RDV de '+l.nom+' à Google Agenda');
}

function markLeadRdvDone(id,source='manuel'){
  if(!id)return;
  const leads=getLeads();
  const idx=leads.findIndex(x=>x.id===id);
  if(idx===-1)return;
  const lead=leads[idx];
  if(!registerLeadRdvDone(lead,source)){
    showDriveNotif('ℹ️ RDV déjà compté aujourd’hui');
    return;
  }
  lead.date_rdv_fait=new Date().toISOString().slice(0,10);
  if(lead.statut==='gris')lead.statut='rdv';
  addLeadTimelineEntry(lead,'RDV effectué (pont Google Agenda)',currentUser.name);
  lead.date_modification=new Date().toISOString();
  saveLeads(leads);
  if(currentLeadId===id){
    openLead(id);
  }else{
    renderLeads();
  }
  refreshLeadsBadge();
  if(currentUser?.role==='admin'||isCRMScopePilotageRole(currentUser?.role)){
    renderLeadsDashboard();
  }
  showDriveNotif(`✅ RDV comptabilisé pour ${lead.nom}`);
}

// ══════════════════════════════════════════
// 📧 EXPORT LEAD PAR EMAIL
// ══════════════════════════════════════════
function exportLeadParEmail(id){
  const l=getLeads().find(x=>x.id===id);if(!l)return;
  const srcLabel=LEAD_SOURCE_LABELS[l.source]||'';
  const sujet=encodeURIComponent(`Lead CRM — ${l.nom} — ${l.type_projet||''}`);
  const corps=encodeURIComponent(`Bonjour,

Voici un lead à traiter :

Client : ${l.nom}
Téléphone : ${l.telephone||'—'}
Adresse : ${[l.adresse,l.cp,l.ville].filter(Boolean).join(', ')||'—'}
Projet : ${l.type_projet||'—'}
Source : ${srcLabel}
Secteur : ${getLeadSecteurLabel(l.secteur)}
${l.commentaire?'Commentaire : '+l.commentaire:''}

Bonne chance !
BenAI — Nemausus Fermetures`);
  window.location.href=`mailto:?subject=${sujet}&body=${corps}`;
  logActivity(`${currentUser.name} a exporté le lead ${l.nom} par email`);
}

// ══════════════════════════════════════════
// 🔀 FUSION DE LEADS EN DOUBLE
// ══════════════════════════════════════════
let fusionSelectIds=[];

function ouvrirFusion(){
  // Ouvrir un mini panel de fusion
  const leads=getLeads().filter(l=>!l._deleted&&!l.archive);
  const panel=document.createElement('div');
  panel.id='fusion-panel';
  panel.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:3000;display:flex;align-items:center;justify-content:center;padding:16px';
  panel.innerHTML=`
    <div style="background:var(--s1);border:1px solid var(--b1);border-radius:16px;width:100%;max-width:500px;max-height:80vh;overflow-y:auto;padding:20px">
      <div style="font-size:15px;font-weight:700;margin-bottom:4px">🔀 Fusionner deux leads</div>
      <div style="font-size:12px;color:var(--t3);margin-bottom:14px">Sélectionnez 2 leads à fusionner. Le premier sera conservé avec toutes les données du second.</div>
      <input type="text" id="fusion-search" placeholder="🔍 Rechercher..." oninput="renderFusionList()" style="width:100%;padding:8px 10px;background:var(--s2);border:1px solid var(--b1);border-radius:8px;color:var(--t1);font-family:'Outfit',sans-serif;font-size:12px;outline:none;margin-bottom:10px">
      <div id="fusion-list" style="display:flex;flex-direction:column;gap:6px;max-height:300px;overflow-y:auto"></div>
      <div id="fusion-selected" style="margin-top:10px;font-size:12px;color:var(--a);min-height:20px"></div>
      <div style="display:flex;gap:8px;margin-top:12px">
        <button onclick="confirmerFusion()" class="btn-primary" style="flex:1">🔀 Fusionner</button>
        <button onclick="document.getElementById('fusion-panel').remove();fusionSelectIds=[]" style="padding:10px 16px;background:transparent;border:1px solid var(--b2);color:var(--t2);border-radius:8px;cursor:pointer;font-family:inherit;font-size:13px">Annuler</button>
      </div>
    </div>`;
  document.body.appendChild(panel);
  fusionSelectIds=[];
  renderFusionList();
}

function renderFusionList(){
  const list=document.getElementById('fusion-list');if(!list)return;
  const search=(document.getElementById('fusion-search')?.value||'').toLowerCase();
  let leads=getLeads().filter(l=>!l._deleted&&!l.archive);
  if(search)leads=leads.filter(l=>(l.nom||'').toLowerCase().includes(search)||(l.telephone||'').includes(search));
  list.innerHTML=leads.map(l=>{
    const sel=fusionSelectIds.includes(l.id);
    return`<div onclick="toggleFusionSelect(${l.id})" style="padding:9px 12px;background:${sel?'var(--a3)':'var(--s2)'};border:1px solid ${sel?'var(--a)':'var(--b1)'};border-radius:8px;cursor:pointer;transition:.12s">
      <div style="font-size:13px;font-weight:600">${esc(l.nom)}</div>
      <div style="font-size:11px;color:var(--t2)">${esc(l.telephone||'')} · ${esc(l.ville||'')} · ${esc(l.type_projet||'')}</div>
    </div>`;
  }).join('');
}

function toggleFusionSelect(id){
  if(fusionSelectIds.includes(id)){
    fusionSelectIds=fusionSelectIds.filter(x=>x!==id);
  } else if(fusionSelectIds.length<2){
    fusionSelectIds.push(id);
  }
  const info=document.getElementById('fusion-selected');
  if(info)info.textContent=fusionSelectIds.length===2?'✅ 2 leads sélectionnés — prêt à fusionner':fusionSelectIds.length===1?'1 lead sélectionné — choisissez le doublon':'';
  renderFusionList();
}

function confirmerFusion(){
  if(fusionSelectIds.length!==2){showDriveNotif('⚠️ Sélectionnez exactement 2 leads');return;}
  if(!confirm('Fusionner ces 2 leads ? Le premier sera conservé, le second supprimé.'))return;
  const leads=getLeads();
  const [id1,id2]=fusionSelectIds;
  const l1=leads.find(l=>l.id===id1);
  const l2=leads.find(l=>l.id===id2);
  if(!l1||!l2)return;
  // Fusionner : l1 garde la priorité, compléter avec l2 si champs vides
  ['telephone','adresse','ville','cp','commentaire','suivi','commercial','source'].forEach(k=>{
    if(!l1[k]&&l2[k])l1[k]=l2[k];
  });
  // Fusionner les timelines
  l1.timeline=[...(l1.timeline||[]),...(l2.timeline||[])];
  const dateStr=new Date().toLocaleString('fr-FR',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  addLeadTimelineEntry(l1,`Fusionné avec ${l2.nom}`,currentUser.name);
  // Supprimer l2
  logDeletion('Lead (fusion)',l2.nom);
  l2._deleted=true;
  l2.archive=true;
  l2.date_modification=new Date().toISOString();
  const newLeads=[...leads];
  const idx=newLeads.findIndex(l=>l.id===id1);
  if(idx!==-1)newLeads[idx]=l1;
  saveLeads(newLeads);
  document.getElementById('fusion-panel')?.remove();
  fusionSelectIds=[];
  renderLeads();
  showDriveNotif(`✅ ${l1.nom} et ${l2.nom} fusionnés`);
  logActivity(`${currentUser.name} a fusionné ${l1.nom} + ${l2.nom}`);
}

// ══════════════════════════════════════════
// 🏢 CRM LAMBERT SAS
// ══════════════════════════════════════════
function getSocieteFromUser(uid){
  const u=getAllUsers().find(x=>x.id===uid);
  if(!u)return 'nemausus';
  if(u.societe==='lambert')return'lambert';
  if(u.id==='benjamin')return'les-deux';
  return u.societe||'nemausus';
}

function getAutoAttribution(societe){
  // Attribution automatique si un seul commercial sur la société
  const commerciaux=getAllUsers().filter(u=>u.role==='commercial'&&(u.societe===societe||u.societe==='les-deux'));
  if(commerciaux.length===1)return commerciaux[0].id;
  return null;
}

const esc=s=>(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const escAttr=s=>esc(s).replace(/'/g,'&#39;').replace(/"/g,'&quot;');
const md=txt=>(txt||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>').replace(/\n/g,'<br>');
const csv=s=>`"${(s||'').replace(/"/g,'""')}"`;
const getUserColor=uid=>USERS[uid]?.color||getExtraUserById(uid)?.color||'#333';
const getUserInitial=uid=>USERS[uid]?.initial||getExtraUserById(uid)?.initial||'?';
const formatDate=d=>{if(!d)return '';try{return new Date(d).toLocaleDateString('fr-FR');}catch{return d;}};

function download(filename,content,type){
  const blob=new Blob([content],{type});const url=URL.createObjectURL(blob);
  const a=document.createElement('a');a.href=url;a.download=filename;a.click();URL.revokeObjectURL(url);
}

function logActivity(txt){
  const mem=getMem();if(!mem.activity)mem.activity=[];
  const now=new Date().toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'});
  mem.activity.push({user:currentUser?.id||'?',txt,time:now});
  if(mem.activity.length>100)mem.activity=mem.activity.slice(-100);
  saveMem(mem);
}

// ══════════════════════════════════════════
// ACCORDION DASHBOARD
// ══════════════════════════════════════════
function toggleAccordion(id){
  const body=document.getElementById(id);if(!body)return;
  const isOpen=!body.classList.contains('collapsed');
  body.classList.toggle('collapsed',isOpen);
  const arrow=body.previousElementSibling?.querySelector('.accordion-arrow');
  if(arrow)arrow.style.transform=isOpen?'rotate(-90deg)':'rotate(0deg)';
}

// ══════════════════════════════════════════
// SYSTÈME DE NOTIFICATIONS
// ══════════════════════════════════════════
function getNotifs(){try{return JSON.parse(appStorage.getItem('benai_notifs_'+( currentUser?.id||'?')))||[];}catch{return[];}}
function saveNotifs(n){appStorage.setItem('benai_notifs_'+(currentUser?.id||'?'),JSON.stringify(n));}
function isBrowserNotificationDisabled(){
  return appStorage.getItem('benai_notif_browser_disabled')==='1';
}
function disableBrowserNotifications(reason=''){
  appStorage.setItem('benai_notif_browser_disabled','1');
  if(reason){
    try{console.warn('[BenAI] Notifications navigateur désactivées:',reason);}catch(e){}
  }
}

/** Notifications OS du navigateur (hors onglet) : désactivées sur téléphone / petite fenêtre — l’app garde la cloche in-app. */
function shouldUseBrowserOSNotifications(){
  if(isBrowserNotificationDisabled())return false;
  try{
    if(window.matchMedia('(max-width: 768px)').matches)return false;
  }catch{}
  if(/iphone|ipad|ipod|android|mobile/i.test((navigator.userAgent||'').toLowerCase()))return false;
  return true;
}

function pushNotif(titre,msg,icon='🔔',uid){
  // uid = destinataire (si vide = utilisateur courant)
  const target=uid||currentUser?.id||'?';
  const now=Date.now();
  const notifSignature=`${String(titre||'').trim()}::${String(msg||'').trim()}::${target}`;
  const dedupeKey='benai_notif_last_'+target;
  let allowInsert=true;
  try{
    const rawState=appStorage.getItem(dedupeKey)||'';
    const state=rawState?JSON.parse(rawState):{};
    if(state?.sig===notifSignature&&Number(state?.ts||0)&&now-Number(state.ts)<90*1000){
      allowInsert=false;
    }
  }catch(e){}
  const timeStr=new Date().toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'});
  const selfTarget=!uid||normalizeId(String(target))===normalizeId(String(currentUser?.id||''));
  if(selfTarget){
    try{
      const key='benai_notifs_'+target;
      const notifs=JSON.parse(appStorage.getItem(key)||'[]');
      if(allowInsert){
        notifs.unshift({id:Date.now(),titre,msg,icon,time:timeStr,read:false});
        if(notifs.length>50)notifs.splice(50);
        appStorage.setItem(key,JSON.stringify(notifs));
        appStorage.setItem(dedupeKey,JSON.stringify({sig:notifSignature,ts:now}));
      }
    }catch(e){}
    if(allowInsert&&shouldUseBrowserOSNotifications()&&'Notification' in window&&normalizeId(String(target))===normalizeId(String(currentUser?.id||''))){
      const BrowserNotif=window.Notification;
      if(BrowserNotif&&BrowserNotif.permission==='granted'){
        try{
          new BrowserNotif('BenAI — '+titre,{body:msg,icon:'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="8" fill="%23E8943A"/><text x="16" y="22" text-anchor="middle" font-size="18" fill="white">B</text></svg>'});
        }catch(e){
          const errTxt=String(e?.message||e||'');
          if(/failed to construct ['"]?notification['"]?|illegal constructor|serviceworkerregistration\.shownotification/i.test(errTxt)){
            disableBrowserNotifications(errTxt);
          }else{
            reportAutoBug('Notifications',`Erreur JavaScript: ${errTxt}`,'mineur');
          }
        }
      }
    }
  }else if(allowInsert&&SUPABASE_CONFIG.enabled&&currentUser?.id){
    const feedId=now*1000+Math.floor(Math.random()*1000);
    const item={id:feedId,target_uid:String(target),titre,msg,icon,time:timeStr,ts:now};
    void pushSharedCoreDataToSupabase(getMem(),getAnnuaire(),getLeads(),[item]).then(()=>{refreshNotifBadge();});
    try{appStorage.setItem(dedupeKey,JSON.stringify({sig:notifSignature,ts:now}));}catch(e){}
  }
  refreshNotifBadge();
}

function pushBenAINotif(titre,msg,icon='🤖',uid){
  return pushNotif(`BenAI · ${titre}`,msg,icon,uid);
}

function refreshNotifBadge(){
  if(!currentUser)return;
  const notifs=getNotifs();
  const unread=notifs.filter(n=>!n.read).length;
  const dot=document.getElementById('notif-dot');
  if(!dot)return;
  if(unread>0){
    dot.style.display='flex';
    dot.textContent=unread>9?'9+':unread;
  } else {
    dot.style.display='none';
  }
}

function toggleNotifPanel(){
  const panel=document.getElementById('notif-panel');
  if(!panel)return;
  const isOpen=panel.style.display!=='none';
  panel.style.display=isOpen?'none':'block';
  if(!isOpen)renderNotifList();
}

function renderNotifList(){
  const list=document.getElementById('notif-list');if(!list)return;
  const notifs=getNotifs();
  if(!notifs.length){list.innerHTML='<div style="padding:16px;color:var(--t3);font-size:12px;text-align:center">Aucune notification</div>';return;}
  list.innerHTML=notifs.map(n=>`
    <div class="notif-item${n.read?'':' unread'}" onclick="readNotif(${n.id})">
      <div class="notif-item-icon">${n.icon}</div>
      <div style="flex:1">
        <div style="font-size:12px;font-weight:600">${esc(n.titre)}</div>
        <div class="notif-item-txt">${esc(n.msg)}</div>
        <div class="notif-item-time">${n.time}</div>
      </div>
    </div>`).join('');
  // Marquer comme lus après affichage
  setTimeout(()=>markAllNotifsRead(),1500);
}

function readNotif(id){
  const notifs=getNotifs();
  const n=notifs.find(x=>x.id===id);
  if(n)n.read=true;
  saveNotifs(notifs);
  refreshNotifBadge();
}

function markAllNotifsRead(){
  const notifs=getNotifs();
  notifs.forEach(n=>n.read=true);
  saveNotifs(notifs);
  refreshNotifBadge();
  renderNotifList();
}

function clearReadNotifs(){
  const notifs=getNotifs();
  const kept=notifs.filter(n=>!n.read);
  if(kept.length===notifs.length){
    showDriveNotif('ℹ️ Aucune notification déjà lue à effacer');
    return;
  }
  saveNotifs(kept);
  refreshNotifBadge();
  renderNotifList();
}

function requestNotifPermission(){
  if(isBrowserNotificationDisabled())return;
  if(!shouldUseBrowserOSNotifications())return;
  const BrowserNotif=window.Notification;
  if(BrowserNotif&&BrowserNotif.permission==='default'){
    try{BrowserNotif.requestPermission();}catch(e){}
  }
}

// ══════════════════════════════════════════
// BADGE ABSENCES SIDEBAR
// ══════════════════════════════════════════
function refreshAbsBadge(){
  const badge=document.getElementById('abs-badge');if(!badge)return;
  const mem=getMem();
  const isAd=currentUser?.role==='admin';
  const uid=currentUser?.id||'';
  const today=new Date();today.setHours(0,0,0,0);
  const upcoming=(mem.absences||[]).filter(a=>{
    if(a._deleted)return false;
    if(!isAd&&!absenceNotifsIncludeUser(a,uid))return false;
    const debut=new Date(a.debut);debut.setHours(0,0,0,0);
    const fin=new Date(a.fin);fin.setHours(0,0,0,0);
    const diff=Math.ceil((debut-today)/(1000*60*60*24));
    return diff>=0&&diff<=7&&today<=fin;
  }).length;
  if(upcoming>0){badge.style.display='flex';badge.textContent=upcoming;}
  else badge.style.display='none';
}

// ══════════════════════════════════════════
// TÉLÉCHARGER MA CONVERSATION
// ══════════════════════════════════════════
function downloadMyChat(){
  const msgs=loadChatMem(currentUser?.id||'?');
  if(!msgs.length){showDriveNotif('Aucune conversation à télécharger');return;}
  const lines=msgs.filter(m=>m.role&&m.content).map(m=>{
    const who=m.role==='user'?currentUser.name:'BenAI';
    return `[${who}]\n${m.content}\n`;
  }).join('\n---\n\n');
  const header=`BenAI — Conversation de ${currentUser.name}\nExportée le ${new Date().toLocaleString('fr-FR')}\n${'═'.repeat(50)}\n\n`;
  download(`BenAI_conversation_${currentUser.name}_${new Date().toLocaleDateString('fr-FR').replace(/\//g,'-')}.txt`,header+lines,'text/plain');
}

// ══════════════════════════════════════════
// SAUVEGARDE À LA FERMETURE
// ══════════════════════════════════════════
window.addEventListener('beforeunload',()=>{
  flushPersistRuntimeToSession(currentUser?.id);
  if(currentUser?.id){
    void flushSupabaseSyncNow();
    const snapshot=serializeCloudAppStorage();
    void persistAppStorageToSupabaseNow(currentUser.id,snapshot);
  }
  if(!currentUser||currentUser.id!=='benjamin')return;
  // Sauvegarde JSON silencieuse en session avec timestamp
  try{
    const mem=getMem();const pwds=getPwds();const extras=getExtraUsers();const access=getAccess();const annuaire=getAnnuaire();
    const backup={version:BENAI_VERSION,date:new Date().toISOString(),data:mem,pwds,extras,access,annuaire};
    appStorage.setItem('benai_auto_backup',JSON.stringify(backup));
    appStorage.setItem('benai_auto_backup_date',new Date().toISOString());
  }catch(e){}
});

// FERMER SETTINGS si clic ailleurs
document.addEventListener('click',e=>{
  const panel=document.getElementById('settings-panel');
  const btn=document.querySelector('.tb-icon-btn');
  if(panel&&!panel.contains(e.target)&&btn&&!btn.contains(e.target))panel.style.display='none';
  const sp=document.getElementById('switch-panel');
  const sb=document.getElementById('btn-switch-session');
  if(sp&&!sp.contains(e.target)&&sb&&!sb.contains(e.target))sp.style.display='none';
  const np=document.getElementById('notif-panel');
  const nb=document.getElementById('btn-notif');
  if(np&&!np.contains(e.target)&&nb&&!nb.contains(e.target))np.style.display='none';
});

// ══════════════════════════════════════════
// INIT AU CHARGEMENT
// ══════════════════════════════════════════
window.addEventListener('load',async()=>{
  const client=getSupabaseClient();
  initManifestAndIcons();
  registerBenAISW();
  refreshInstallButton();
  // Thème
  applyTheme();
  // Clé API : configuration centralisée côté admin dans Paramètres
  await maybeMigratePwds();
  // Charger utilisateurs extra avec leur vrai rôle
  getExtraUsers().forEach(u=>{
    if(!USERS[u.id]){
      const role=u.role||'assistante';
      const isCRM=CRM_PAGES_ONLY.includes(role);
      const systemPrompt=isCRM
        ?`Tu es BenAI CRM, assistant de ${u.name} (${ROLE_LABELS[role]||role}).`
        :`Tu es BenAI, l'assistant de ${u.name}. Tu proposes toujours — ${u.name} décide toujours.`;
      USERS[u.id]={name:u.name,pwd:getPwds()[u.id]||'1234',role,color:u.color,initial:u.initial,system:systemPrompt};
    }
  });
  const rememberedLogin=getRememberedLoginForForm();
  const loginInput=document.getElementById('login-user');
  if(loginInput&&rememberedLogin)loginInput.value=rememberedLogin;
  if(client){
    try{
      const {data}=await client.auth.getSession();
      currentSupabaseSession=data?.session||null;
      if(currentSupabaseSession?.access_token){
        currentAuthMode='supabase';
        const profile=await fetchSupabaseProfile(currentSupabaseSession.access_token,currentSupabaseSession.user?.id||'');
        const user=makeUserFromSupabaseProfile(profile,currentSupabaseSession.user?.email||'');
        if(user){
          user.system=getSystemPromptForUser(user);
          currentUser=user;
          await hydrateAppStorageFromSupabase(currentUser.id);
          document.getElementById('login-screen').style.display='none';
          document.getElementById('app').classList.add('visible');
          initApp(true);
          void syncSupabasePostLogin();
          checkUpdate();
          initOfflineDetection();
          enableSpellcheckEverywhere();
          return;
        }
      }
    }catch(e){}
  }
  // Pas de session Supabase valide : écran login (souvenir d’email via rememberLoginId si activé).
  // Vérif mise à jour
  checkUpdate();
  // Mode hors-ligne
  initOfflineDetection();
  // Correction automatique partout
  enableSpellcheckEverywhere();
});

window.addEventListener('beforeinstallprompt',e=>{
  e.preventDefault();
  deferredInstallPrompt=e;
  refreshInstallButton();
});

window.addEventListener('appinstalled',()=>{
  deferredInstallPrompt=null;
  refreshInstallButton();
  showDriveNotif('✅ BenAI installé sur l’écran d’accueil');
});

// MISE À JOUR
async function checkUpdate(){
  try{
    const res=await fetch(`https://raw.githubusercontent.com/BenAI30/benai/main/version.json?t=${Date.now()}`);
    if(!res.ok)return;
    const data=await res.json();
    if(data.version&&isNewerVersion(data.version,BENAI_VERSION)){
      // Filtrer les notes selon le rôle
      const role=currentUser?.role||'assistante';
      let notes=[];
      if(data.notes_admin&&role==='admin')notes=notes.concat(data.notes_admin);
      if(data.notes_directeur_co&&role==='directeur_co')notes=notes.concat(data.notes_directeur_co);
      if(data.notes_commercial&&role==='commercial')notes=notes.concat(data.notes_commercial);
      if(data.notes_assistante&&(role==='assistante'||role==='metreur'))notes=notes.concat(data.notes_assistante);
      // Fallback si pas de notes filtrées
      if(!notes.length)notes=Array.isArray(data.notes)?data.notes:[data.notes||'Améliorations et corrections'];
      document.getElementById('update-version').textContent='Mise à jour BenAI prête';
      const notesList=document.getElementById('update-notes');
      notesList.innerHTML=notes.map(n=>'<li>✅ '+esc(n)+'</li>').join('');
      window._pendingUpdate={version:data.version,notes:data.notes};
      document.getElementById('update-popup').style.display='flex';
    }
  }catch(e){}
}

function confirmInstallUpdate(){
  document.getElementById('update-popup').style.display='none';
  if(window._pendingUpdate)installUpdate(window._pendingUpdate.version,window._pendingUpdate.notes||'');
}

function isNewerVersion(remote,current){
  const r=remote.split('.').map(Number);
  const c=current.split('.').map(Number);
  for(let i=0;i<Math.max(r.length,c.length);i++){
    const rv=r[i]||0,cv=c[i]||0;
    if(rv>cv)return true;
    if(rv<cv)return false;
  }
  return false;
}

// ══════════════════════════════════════════
// ✏️ CORRECTION AUTOMATIQUE PARTOUT
// ══════════════════════════════════════════
function isBenAIDesktopAutocorrect(){
  try{return window.matchMedia('(min-width: 769px)').matches;}catch{return false;}
}
function enableSpellcheckEverywhere(){
  const isPhone=/iphone|ipad|ipod|android|mobile/i.test((navigator.userAgent||'').toLowerCase());
  document.querySelectorAll('input[type="text"],input[type="search"],textarea').forEach(el=>{
    if(isPhone){
      el.spellcheck=false;
      el.setAttribute('autocorrect','off');
      el.setAttribute('autocapitalize','none');
      return;
    }
    el.spellcheck=true;
    el.setAttribute('autocorrect','on');
    el.setAttribute('autocapitalize','sentences');
  });
  // Observer pour les éléments ajoutés dynamiquement
  new MutationObserver(()=>{
    document.querySelectorAll('input[type="text"]:not([spellcheck]),textarea:not([spellcheck])').forEach(el=>{
      if(isPhone){
        el.spellcheck=false;
        el.setAttribute('autocorrect','off');
        el.setAttribute('autocapitalize','none');
      }else{
        el.spellcheck=true;
        el.setAttribute('autocorrect','on');
        el.setAttribute('autocapitalize','sentences');
      }
    });
  }).observe(document.body,{childList:true,subtree:true});
}

try{
  const _mqDesk=window.matchMedia('(min-width: 769px)');
  const _syncCorrectBtn=()=>{
    const btn=document.getElementById('btn-correct');
    if(btn&&document.getElementById('app')?.classList.contains('visible')){
      btn.style.display=_mqDesk.matches?'inline-flex':'none';
    }
  };
  if(_mqDesk.addEventListener)_mqDesk.addEventListener('change',_syncCorrectBtn);
  else if(_mqDesk.addListener)_mqDesk.addListener(_syncCorrectBtn);
}catch(e){}

const TUTO_SLIDES_BY_ROLE={
  commercial:[
    {icon:'🎯',title:'Ton rôle en une phrase',desc:'Tu peux faire vivre chaque opportunité dans le CRM : appels, RDV, devis, signature ou perte documentée. BenAI affiche la liste ; tu peux y consigner la réalité terrain après chaque contact, quand tu le souhaites.',highlight:'Onglets visibles : Leads CRM, Messages, Absences, Guide — et « Signaler » pour un blocage technique'},
    {icon:'🗺️',title:'Navigation CRM',desc:'Dans Leads CRM : « Mes leads » (liste ou kanban), puis « Mes ventes » (tableau des ventes et suivi d’objectifs).',highlight:'Tu peux basculer ☰ / ⊞ selon ce qui te convient le mieux'},
    {icon:'🔎',title:'Trier et filtrer',desc:'Les pastilles Tous, Non traité, RDV pris, Devis envoyé, Vendu, Perdu, Archives et Alertes aident à structurer la vue. La recherche texte retrouve nom, téléphone ou ville. Beaucoup d’équipes combinent Alertes et Non traité en premier regard.',highlight:'Tu peux adapter l’ordre de traitement à ta journée'},
    {icon:'➕',title:'Créer un lead (terrain / prospection)',desc:'Bouton + Nouveau lead : nom, téléphone, code postal et type de projet sont demandés à l’enregistrement. La source « ACTIF » t’attribue automatiquement le dossier ; pour les autres cas, l’organisation gère l’attribution selon vos règles.',highlight:'Un secteur cohérent avec le CP limite les erreurs de zone'},
    {icon:'👤',title:'Étape A — Ouvrir et comprendre',desc:'Tu peux ouvrir la fiche : projet, adresse et commentaire de création. Tu peux compléter Suivi ou Commentaire quand tu as une info nouvelle — la timeline détaillée reste surtout utile à la direction (admin, dir. commercial, DG).',highlight:'Lire la fiche avant d’appeler fait souvent gagner du temps'},
    {icon:'📞',title:'Étape B — Premier contact',desc:'Le bouton Appeler peut t’aider à composer. Après l’échange, tu peux choisir le sous-statut adapté (à rappeler, injoignable, faux numéro, RDV programmé…). Si un rappel plus tard t’aide, tu peux fixer une date/heure : BenAI peut te notifier autour de ce créneau.',highlight:'Tu peux noter chaque appel dans le CRM, même brièvement'},
    {icon:'📅',title:'Étape C — RDV',desc:'Quand un créneau est validé, tu peux renseigner la date (sous-statut RDV programmé) puis utiliser Agenda : Google Calendar s’ouvre avec le client et le lieu. Après le passage, « RDV fait » permet d’indiquer que le rendez-vous a eu lieu.',highlight:'Une date renseignée limite le risque de double réservation'},
    {icon:'🧾',title:'Étape D — Devis envoyé',desc:'Tu peux passer en jaune « Devis envoyé », saisir montant HT, date d’envoi et prochaine relance. Au bout de 7 jours sans nouvelle, BenAI peut proposer une alerte « devis sans réponse » : tu peux rappeler le client quand tu le juges utile.',highlight:'Montant + date aident le suivi du dossier'},
    {icon:'🟢',title:'Étape E — Vendu',desc:'Tu peux passer en statut vert et saisir le prix vendu HT aligné sur le montant signé (champ demandé pour enregistrer). Tu peux compléter date de signature ou produit si ton entreprise le demande.',highlight:'Le montant exact permet d’enregistrer le vendu correctement'},
    {icon:'🔴',title:'Étape F — Perdu',desc:'Tu peux passer en rouge « Perdu », puis choisir un motif de refus détaillé dans la liste avant d’enregistrer.',highlight:'Un motif permet de clôturer la fiche proprement'},
    {icon:'⚠️',title:'Cas particulier — Hors secteur',desc:'Si le code postal sort de la zone habituelle, BenAI peut demander une courte justification avant d’enregistrer (ex. chantier exceptionnel, client historique).',highlight:'Même logique à la création ou à la mise à jour'},
    {icon:'🔔',title:'Notifications et badge',desc:'Le badge sur Leads met surtout en avant les dossiers avec alerte métier. Les notifications résument aussi les volumes à traiter : tu peux t’en servir comme rappel.',highlight:'Tu peux mettre à jour le CRM après traitement pour faire baisser les alertes'},
    {icon:'💬',title:'Messages et absences',desc:'Messages sert aux accords rapides (« je passe demain », « client veut l’autre coloris »). Les décisions structurantes peuvent rester dans la fiche lead. Tu peux déclarer tes absences pour que l’équipe anticipe si besoin.',highlight:'Messages et CRM se complètent'},
    {icon:'🛟',title:'Signaler un problème',desc:'Si un écran bloque, qu’un bouton ne répond pas ou qu’un message d’erreur apparaît, ouvre « Signaler » dans le menu. Décris la page, l’action juste avant le problème et la gravité : BenAI prépare un rapport structuré pour l’équipe technique (tu ne vois pas la liste des tickets).',highlight:'Un signalement détaillé aide en général à corriger plus vite'},
    {icon:'✅',title:'Pistes de journée',desc:'Exemples : alertes ou non traités, appel + mise à jour du statut, RDV → Agenda puis « RDV fait », devis avec montants/dates, vendu avec prix HT, perdu avec motif, devis anciens à relancer, « Signaler » si l’outil bloque.',highlight:'Tu restes libre d’organiser ta journée comme tu préfères'},
  ],
  directeur_co:[
    {icon:'🏢',title:'Ton entreprise sur BenAI',desc:'Ce tutoriel décrit ce que tu vois dans BenAI pour ton compte : l’entreprise et les zones CRM affichées suivent le réglage du profil. Filtres secteur, cartes du tableau de bord et commerciaux listés restent alignés sur ce périmètre.',highlight:'Si un collègue te prête sa session, tu peux vérifier en haut de l’écran que c’est bien ton compte.'},
    {icon:'📊',title:'Menus visibles',desc:'Leads CRM, Messages, Absences, Guide. En cas de blocage : menu « Signaler » (la liste des tickets reste côté administration). Le CRM sert à attribuer, suivre et chiffrer les dossiers.',highlight:'Tu peux ouvrir Leads CRM quand tu en as besoin'},
    {icon:'🧭',title:'Les trois onglets CRM',desc:'« À attribuer » : nouveaux dossiers sans commercial assigné. « Tous les leads » : le pipeline (recherche, pastilles de statut, liste ou kanban, archives, alertes). « Dashboard » : synthèses (KPI, secteurs, CA, équipe, exports, objectifs).',highlight:'Tu peux commencer par « À attribuer » pour enchaîner les nouveaux dossiers, si tu le souhaites'},
    {icon:'🎛️',title:'Filtres liste & kanban',desc:'Tu peux combiner recherche texte, pastilles (Non traité, RDV, Devis, Vendu, Perdu, Archives, Alertes), filtre secteur (les zones de ton périmètre), filtre commercial. Le filtre par société n’apparaît que si ton accès couvre les deux entités.',highlight:'Tu peux réinitialiser les filtres si une vue semble vide'},
    {icon:'🤝',title:'Attribuer ou reprendre un lead',desc:'Sur « À attribuer » ou dans la fiche : champ « Commercial » — tu peux choisir un vendeur, un autre dirigeant, ou toi-même pour porter le dossier. Changement = notification + entrée dans l’historique.',highlight:'Tu peux t’assigner comme un commercial'},
    {icon:'📇',title:'Ouvrir une fiche lead attribué',desc:'En cliquant un dossier : identité, projet, commentaire d’origine, secteur, source. Tu peux corriger les champs de base, suivi, commentaire, montants et dates selon les droits affichés.',highlight:'Une fiche à jour limite les doublons d’appels'},
    {icon:'📜',title:'Historique & timeline',desc:'Sur un lead attribué (ou sur le tien), la section Historique liste les actions : ouverture de fiche, changements de statut, d’attribution, de montants, etc., avec date et auteur — trace utile sur le terrain.',highlight:'Tu peux lire la timeline avant d’appeler le commercial ou le client'},
    {icon:'👁️',title:'Pastille « non ouvert »',desc:'Indique que le commercial assigné n’a pas encore ouvert la fiche une première fois — simple indicateur de suivi, pas une sanction.',highlight:'Souvent levé avec un rappel interne'},
    {icon:'📈',title:'Dashboard — ce que tu y vois',desc:'Résumé KPI, Secteurs & mois (souvent replié en premier pour toi), ventes & RDV, CA par entreprise, détail ventes, équipe puis Objectifs commerciaux (ouvert par défaut), pertes & archives, exports. Les objectifs listent l’annuaire vendeurs + les attributions visibles sur les leads.',highlight:'Barre « Aller à » : lien Objectifs après Équipe'},
    {icon:'📥',title:'Exports',desc:'Depuis le dashboard : menu Export secteur + bouton Performance secteurs ; plus bas « Exporter tous les leads » et paquets CSV. Les fichiers respectent ton périmètre leads + secteurs visibles.',highlight:'UTF-8, séparateur ; pour Excel France'},
    {icon:'🔔',title:'Badge & notifications',desc:'Le badge sur Leads compte surtout les dossiers à traiter sur ton périmètre (non attribués, alertes). Les notifications résument aussi les volumes à traiter.',highlight:'Tu peux mettre à jour les statuts des dossiers traités pour faire baisser les alertes'},
    {icon:'💬',title:'Messages, absences, signalement',desc:'Messages pour les accords rapides ; onglet Absences si tu consultes les disponibilités saisies par l’équipe ; menu « Signaler » pour un blocage technique (page, clic, message d’erreur) — sans accès à la liste des tickets.',highlight:'Un signalement précis aide l’équipe technique'},
    {icon:'✅',title:'Pistes pour t’organiser',desc:'Exemples : parcourir les dossiers à attribuer, repérer alertes ou non traités, ouvrir les fiches sensibles (timeline), consulter le dashboard en fin de période. Des champs devis / vendu / perdu renseignés améliorent la qualité des statistiques.',highlight:'Tu restes libre d’adapter la routine à ton équipe'},
  ],
  directeur_general:[
    {icon:'🏢',title:'Même périmètre que le dir. commercial',desc:'Tu consultes et agis sur les mêmes écrans que le dir. commercial ; entreprise et zones visibles suivent le réglage de ton compte.',highlight:'Tu as accès aux onglets CRM pilotage sur ton périmètre'},
    {icon:'🧭',title:'Onglets & dashboard',desc:'À attribuer, Tous les leads, Dashboard — mêmes usages que le directeur commercial (attribution, statuts, chiffres, exports).',highlight:'Tu peux dépanner si le dir. co est momentanément indisponible'},
    {icon:'📜',title:'Timeline & qualité des fiches',desc:'Tu vois la même timeline sur les dossiers attribués ; tu peux t’en servir pour repérer les infos manquantes (devis datés, vendu avec montant, perdu avec motif).',highlight:'Des fiches complètes rendent les comités plus fiables'},
    {icon:'💬',title:'Messages & signalement',desc:'Messages pour les arbitrages rapides ; menu « Signaler » pour les incidents répétés (pas d’accès à la liste des tickets).',highlight:'Tu peux détailler le signalement pour faciliter la correction'},
  ],
  assistante:[
    {icon:'🧭',title:'Ta place dans BenAI',desc:'Tu es souvent le premier contact avec le client et tu peux saisir le dossier dans l’outil. Un lead clair limite les allers-retours. Tu as BenAI IA, Notes, Messages, SAV, Leads CRM, Évolutions, Guide — et « Signaler » en cas de blocage.',highlight:'Les absences sont en pratique gérées par l’administration'},
    {icon:'📥',title:'Entrants : saisir le contact',desc:'Quand le téléphone sonne : tu peux noter nom, projet, code postal, téléphone. Tu crées le lead avec ces infos — tu n’as pas à chercher toi-même les doublons ; la direction commerciale peut voir une alerte en cas de dossier similaire.',highlight:'Le code postal détermine automatiquement le secteur'},
    {icon:'➕',title:'Créer un lead',desc:'Leads CRM → + Nouveau lead. À l’enregistrement : nom, téléphone, adresse, code postal, type de projet. Le secteur se remplit selon le CP. Tu peux ajouter ville et un commentaire clair (source : magasin, site, téléphone…).',highlight:'Tu enregistres ici la prise de contact, pas la planification du RDV commercial'},
    {icon:'🏷️',title:'Après ta saisie',desc:'Le directeur commercial peut attribuer un vendeur ; sans dir. co, une attribution automatique peut s’appliquer. Tu peux compléter commentaire ou suivi sur tes fiches ; le tunnel avancé (devis, vendu…) est en général tenu par le terrain une fois le dossier attribué.',highlight:'Pas de notification « leads urgents » automatique sur ton compte — la messagerie reste un canal calme'},
    {icon:'📝',title:'Compléter une fiche',desc:'Tu peux mettre à jour commentaire ou suivi avec ce qui s’est dit (« client veut devis pergola », « rappeler semaine prochaine »). Il est préférable d’éviter statuts ou montants une fois le dossier pris en charge par un commercial.',highlight:'Une phrase claire vaut souvent un long échange'},
    {icon:'🔧',title:'SAV chantier',desc:'Menu SAV → Nouveau : client, problème, fournisseur, rappel. Les notifications suivent les règles BenAI.',highlight:'SAV et lead vente sont deux filières différentes'},
    {icon:'💬',title:'Messages',desc:'Tu peux écrire à l’équipe pour consignes, questions ou coordination ; indique le nom du client ou l’identifiant du lead.',highlight:'Les urgences terrain peuvent aussi passer par le téléphone ou la fiche une fois le dossier attribué'},
    {icon:'📝',title:'Notes perso',desc:'Notes sert à ton organisation personnelle. Pour une info utile au magasin ou au commercial, la fiche lead ou un message convient souvent mieux.',highlight:'CRM et Messages portent le partage d’équipe'},
    {icon:'🛟',title:'Signalement — en cas de blocage',desc:'Menu « Signaler » : page concernée, ce que tu faisais, message d’erreur. Évolutions = nouveautés produit ; les correctifs sont traités côté administration.',highlight:'Un signalement précis aide à corriger plus vite'},
    {icon:'✅',title:'Pistes de journée',desc:'Exemples : appel pertinent → fiche complète, messages pour la coordination, SAV si problème d’installation.',highlight:'Tu poses les bases ; le commercial peut prendre le relais sur la suite'},
  ],
  admin:[
    {icon:'🛡️',title:'Espace administrateur',desc:'Tu peux piloter l’activité globale : tableau de bord synthétique, SAV, messages, notes, et accès CRM complet selon les droits des comptes.',highlight:'Tu peux commencer par les alertes du tableau de bord si tu le souhaites'},
    {icon:'📊',title:'Tableau de bord & SAV',desc:'Tu peux consulter les cartes d’activité ; ouvrir SAV pour suivre les dossiers ouverts et les rappels ; archiver ou mettre en sourdine quand c’est clos.',highlight:'Tu peux rafraîchir le SAV en fin de journée si le volume est élevé'},
    {icon:'👥',title:'Leads & équipes',desc:'Tu peux attribuer ou réattribuer des leads, vérifier les connexions et l’usage mobile, et consulter le journal des suppressions si besoin.',highlight:'Les changements de commercial notifient la personne concernée'},
    {icon:'🎫',title:'Qualité & tickets',desc:'Tu gères les tickets depuis l’onglet réservé admin ; tu peux encourager l’équipe à utiliser « Signaler » (page, étapes, gravité).',highlight:'Un signalement précis limite les allers-retours'},
  ],
  metreur:[
    {icon:'📐',title:'Métreur — usage BenAI',desc:'BenAI peut t’aider à transmettre les informations chantier rapidement et clairement.',highlight:'Souvent utile : Messages, Notes, Signaler'},
    {icon:'💬',title:'Messages internes',desc:'Tu peux partager les infos chantier importantes avec la bonne personne via Messages.',highlight:'Messages = communication équipe'},
    {icon:'📝',title:'Notes personnelles',desc:'Tu peux utiliser Notes pour tes rappels et éléments à vérifier.',highlight:'Notes = organisation perso'},
    {icon:'🤖',title:'BenAI IA',desc:'Tu peux t’appuyer sur BenAI IA pour reformuler un message, préparer un email ou clarifier une réponse.',highlight:'BenAI IA = aide de rédaction'},
    {icon:'🛟',title:'Signaler un problème',desc:'En cas de blocage, ouvre « Signaler » dans le menu avec une description précise.',highlight:'Contexte + action qui bloque aident beaucoup'},
    {icon:'✅',title:'Piste utile',desc:'Laisser une trace claire des infos chantier limite souvent les oublis côté équipe.',highlight:null},
  ],
  default:[
    {icon:'👋',title:'Bienvenue sur BenAI',desc:'BenAI peut t’aider à traiter les actions du quotidien un peu plus vite et un peu plus proprement.',highlight:null},
    {icon:'✅',title:'Prêt(e)',desc:'Tu peux suivre le guide de ton rôle, mettre à jour tes actions au fil de l’eau, et en cas de blocage technique utiliser « Signaler » pour un ticket structuré.',highlight:null},
  ]
};

let tutoStep=0;
let currentTutoSlides=[];
const TUTO_MOBILE_INSTALL_SLIDE={
  icon:'📲',
  title:'BenAI sur téléphone',
  desc:'Tu peux installer BenAI comme une appli mobile pour un accès direct depuis l’écran d’accueil.',
  highlight:'Bouton "📲 Installer" (en haut)'
};

function getRoleTutoSlides(role){
  const slides=TUTO_SLIDES_BY_ROLE[role]||TUTO_SLIDES_BY_ROLE.default;
  return canUseBenAIMobileApp(role)?[...slides,TUTO_MOBILE_INSTALL_SLIDE]:slides.slice();
}

function startTuto(){
  if(!currentUser)return;
  currentTutoSlides=getRoleTutoSlides(currentUser.role||'assistante');
  tutoStep=0;
  renderTutoSlide();
  document.getElementById('tuto-overlay').style.display='flex';
}

function renderTutoSlide(){
  if(!currentTutoSlides.length)currentTutoSlides=getRoleTutoSlides(currentUser?.role||'assistante');
  const slide=currentTutoSlides[tutoStep]||currentTutoSlides[0];
  document.getElementById('tuto-icon').textContent=slide.icon;
  document.getElementById('tuto-title').textContent=slide.title;
  document.getElementById('tuto-desc').textContent=slide.desc;
  const hl=document.getElementById('tuto-highlight');
  if(slide.highlight){hl.style.display='block';hl.textContent=slide.highlight;}
  else hl.style.display='none';
  // Progress dots
  const prog=document.getElementById('tuto-progress');
  prog.innerHTML=currentTutoSlides.map((_,i)=>`<div class="tuto-dot ${i===tutoStep?'active':''}"></div>`).join('');
  const prevBtn=document.getElementById('tuto-prev');
  if(prevBtn)prevBtn.style.display=tutoStep>0?'inline-block':'none';
  // Bouton suivant / terminer
  const btn=document.getElementById('tuto-next');
  if(tutoStep===currentTutoSlides.length-1){
    btn.textContent='Commencer 🚀';
  } else {
    btn.textContent='Suivant →';
  }
}

function prevTutoSlide(){
  if(tutoStep>0){
    tutoStep--;
    renderTutoSlide();
  }
}

function nextTutoSlide(){
  if(tutoStep<currentTutoSlides.length-1){
    tutoStep++;renderTutoSlide();
  } else {
    closeTuto();
  }
}

function closeTuto(){
  document.getElementById('tuto-overlay').style.display='none';
  if(currentUser){
    const keys=getTutoDoneKeysForCurrentUser();
    keys.forEach(key=>appStorage.setItem(key,'1'));
    try{
      keys.forEach(key=>localStorage.setItem(TUTO_DONE_LOCAL_PREFIX+key,'1'));
    }catch(e){}
  }
}

function getTutoDoneKeysForCurrentUser(){
  const keys=[];
  if(currentUser?.id)keys.push('benai_tuto_done_'+currentUser.id);
  const authUid=currentSupabaseSession?.user?.id||'';
  if(authUid)keys.push('benai_tuto_done_auth_'+authUid);
  return [...new Set(keys)];
}

function shouldShowTuto(){
  if(!currentUser)return false;
  const keys=getTutoDoneKeysForCurrentUser();
  if(keys.some(key=>appStorage.getItem(key)==='1'))return false;
  try{
    const hasLocalDone=keys.some(key=>localStorage.getItem(TUTO_DONE_LOCAL_PREFIX+key)==='1');
    if(hasLocalDone){
      keys.forEach(key=>appStorage.setItem(key,'1'));
      return false;
    }
  }catch(e){}
  return true;
}

async function installUpdate(version,notes){
  if(!confirm(`Installer BenAI v${version} ?\n\nBenAI va se recharger.`))return;
  try{
    const res=await fetch(`https://raw.githubusercontent.com/BenAI30/benai/main/index.html?t=${Date.now()}`);
    if(!res.ok)throw new Error('Impossible de télécharger');
    const html=await res.text();
    document.open();document.write(html);document.close();
  }catch(e){alert('❌ Erreur : '+e.message);}
}
