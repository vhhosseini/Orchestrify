// Orchestrify: invia le notifiche push dei nuovi messaggi di chat (ogni ensemble). Eseguito da GitHub Actions ogni 5 minuti.
import admin from 'firebase-admin';

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON || '{}');
admin.initializeApp({credential: admin.credential.cert(serviceAccount)});
const db = admin.firestore();
const messaging = admin.messaging();
const t = (k, fallback) => fallback;

async function notify(){
  const ens=await db.collection('ensembles').get();
  for(const e of ens.docs){ try{ await notifyEnsemble(e.ref,e.id); }catch(err){ console.error(e.id,err.message); } }
}
// Notifiche di un ensemble: legge i messaggi dopo l'ultimo controllo e li invia (messaggi solo «data») ai token FCM dei
// membri attivi, escluso chi ha scritto il messaggio. Più di 4 messaggi nuovi vengono riassunti in una sola notifica.
async function notifyEnsemble(base,eid){
  const stateRef=db.doc('automation/state_'+eid); const stateSnap=await stateRef.get();
  const after=stateSnap.exists && stateSnap.data().lastChatTimestamp ? stateSnap.data().lastChatTimestamp : admin.firestore.Timestamp.fromMillis(Date.now()-10*60*1000);
  const snap=await base.collection('chatMessages').where('timestamp','>',after).orderBy('timestamp').get();
  if(snap.empty) return;
  const ensName=(await base.get()).data()?.name||'Orchestrify';
  const members=(await base.collection('members').get()).docs.map(d=>({uid:d.id,ref:d.ref,...d.data()})).filter(m=>m.active!==false);
  const nameOf=uid=>{ const m=members.find(x=>x.uid===uid); return m?[m.name,m.surname].filter(Boolean).join(' '):''; };
  const preview=m=>{ const tx=typeof m.text==='string'?m.text.trim():''; return (tx||'📎').slice(0,140); };
  const docs=snap.docs.map(d=>({id:d.id,...d.data()}));
  const jobs = docs.length>4 ? [{id:docs.at(-1).id,uid:null,title:ensName,body:docs.length+' new messages',pinned:docs.some(m=>m.pinned)}]
    : docs.map(m=>({id:m.id,uid:m.uid,title:(m.pinned?t('pinned','Pinned message')+' · ':'')+ensName,body:(nameOf(m.uid)?nameOf(m.uid)+': ':'')+preview(m),pinned:!!m.pinned}));
  for(const job of jobs){
    const targets=members.filter(m=>m.fcmToken && m.uid!==job.uid);
    const tokens=[...new Set(targets.map(m=>m.fcmToken))];
    if(!tokens.length) continue;
    const res=await messaging.sendEachForMulticast({tokens,data:{title:job.title,body:job.body,messageId:job.id,eid,pinned:job.pinned?'1':'0'},webpush:{headers:{Urgency:'high',TTL:'86400'}}});
    // Rimuove i token non più validi (app disinstallata / permesso revocato).
    await Promise.all(res.responses.map(async (r,i)=>{ if(!r.success && /registration-token-not-registered|invalid-registration-token|invalid-argument/.test(r.error?.code||'')){
      for(const m of targets.filter(x=>x.fcmToken===tokens[i])) await m.ref.update({fcmToken:admin.firestore.FieldValue.delete()}).catch(()=>{}); } }));
    console.log(eid,'sent',res.successCount,'failed',res.failureCount);
  }
  await stateRef.set({lastChatTimestamp:snap.docs.at(-1).data().timestamp},{merge:true});
}

await notify();
