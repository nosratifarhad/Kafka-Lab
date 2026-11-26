
const $=id=>document.getElementById(id), clamp=(v,a,b)=>Math.max(a,Math.min(b,v));let B=[],P=[],t=0,running=false,events=[],history=[],sent=0,rejected=0,rr=0,tab='logs',epoch=0,s={},selected=null,rebalanceUntil=0,ackCount=0,unacked=0;
const cfg=[['brokers','Brokers',3,1,12],['topics','Topics',2,1,6],['parts','Partitions / topic',3,1,12],['rf','Replication factor',3,1,12],['initial','Initial records / partition',8,0,100000]];
const live=[['producers','Producers',2,0,20],['rate','Produce / producer / sec',5,0,1000],['consumers','Consumers / group',3,0,30],['groups','Consumer groups',1,1,5],['consume','Consume / consumer / sec',3,0,1000],['replicate','Replica fetch / sec',60,0,10000],['minisr','min.insync.replicas',2,1,12],['retention','Retention records',100,1,100000],['commitInterval','Commit interval (s)',5,1,60],['rebalanceDelay','Rebalance delay (s)',2,0,30],['lagTimeout','Replica lag ticks',3,1,60]];
function fields(arr,where){$(where).innerHTML=arr.map(([id,label,v,min,max])=>'<div class="dialfield"><div class="dialtitle"><label for="'+id+'">'+label+'</label><output id="out-'+id+'">'+v+'</output></div><div class="dialrow"><button class="dial" data-input="'+id+'" aria-label="Adjust '+label+'" style="--angle:0deg"><span></span></button><input id="'+id+'" type="range" min="'+min+'" max="'+max+'" value="'+v+'" step="1"><button class="nudge" onclick="nudge(&quot;'+id+'&quot;,-1)">−</button><button class="nudge" onclick="nudge(&quot;'+id+'&quot;,1)">+</button></div></div>').join('')}
fields(cfg,'config');fields(live,'live');$('live').insertAdjacentHTML('beforeend','<label class="field">acks<select id="acks"><option>all</option><option>1</option><option>0</option></select></label><label class="field">Auto commit<input id="auto" type="checkbox" checked></label><label class="field">Partitioner<select id="partitioner"><option value="rr">Round robin</option><option value="key">Key hash</option></select></label><label class="field">Key<input id="key" value="order-42"></label>');
function read(id){let e=$(id);return clamp(Math.floor(+e.value||0),+e.min,+e.max)}
function log(m){events.unshift('['+t+'s] '+m);events=events.slice(0,100)}
function apply(){s=Object.fromEntries(cfg.map(([id])=>[id,read(id)]));s.rf=Math.min(s.rf,s.brokers);$('rf').value=s.rf;reset()}
function reset(){running=false;t=sent=rejected=rr=epoch=ackCount=unacked=0;rebalanceUntil=0;selected=null;history=[];events=[];B=Array.from({length:s.brokers},(_,i)=>({id:i,online:true}));P=[];for(let k=0;k<s.topics;k++)for(let i=0;i<s.parts;i++){let replicas=Array.from({length:s.rf},(_,j)=>(i+k+j)%B.length);P.push({topic:k,id:i,replicas,leader:replicas[0],leo:s.initial,start:0,hw:s.initial,offsets:Object.fromEntries(replicas.map(x=>[x,s.initial])),isr:[...replicas],behind:{},pending:[],pos:[],comm:[]})}rebalance();log('Cluster initialized');render()}
function syncGroups(p){for(let g=0;g<read('groups');g++){if(p.pos[g]===undefined)p.pos[g]=p.comm[g]=p.start}}
function rebalance(){epoch++;rebalanceUntil=t+read('rebalanceDelay');P.forEach(syncGroups);log('Group generation '+epoch+': partitions assigned across '+read('consumers')+' consumers')}
function elect(p){if(p.leader!==null&&B[p.leader].online)return;let n=p.isr.find(x=>B[x].online);if(n===undefined){p.leader=null;return}p.leader=n;p.leo=p.offsets[n];p.hw=Math.min(p.hw,p.leo);p.pending=p.pending.filter(x=>x<=p.leo);for(let b of p.replicas)p.offsets[b]=Math.min(p.offsets[b],p.leo);log('Leader elected B'+n+' for T'+p.topic+'/P'+p.id)}
function toggle(i){B[i].online=!B[i].online;log('Broker '+i+(B[i].online?' recovered':' stopped'));P.forEach(p=>{if(!B[i].online){elect(p);p.isr=p.isr.filter(x=>x!==i)}else{if(p.offsets[i]>=p.hw&&p.replicas.includes(i)&&!p.isr.includes(i))p.isr.push(i);elect(p)} });render()}
function produce(n){n=clamp(Math.floor(n)||0,0,10000);let key=$('key').value,h=0;for(let c of key)h=(h*31+c.charCodeAt(0))>>>0;let a=$('acks').value;let accepted=0;for(let i=0;i<n;i++){let p=P[$('partitioner').value==='key'?h%P.length:rr++%P.length];if(p.leader===null||!B[p.leader].online||(a==='all'&&p.isr.length<read('minisr'))){rejected++;continue}p.leo++;p.offsets[p.leader]=p.leo;sent++;accepted++;if(a==='all')p.pending.push(p.leo);else if(a==='1')ackCount++}if(n)log('Produce '+accepted+'/'+n+' appended; acks='+a)}
function commit(){P.forEach(p=>{syncGroups(p);p.comm=[...p.pos]});log('Offsets committed to __consumer_offsets')}
function restart(){P.forEach(p=>p.pos=[...p.comm]);rebalance();log('Consumer resumed at committed offsets')}
function retain(){P.forEach(p=>{p.start=Math.max(p.start,p.hw-read('retention'));});log('Retention advanced log start offsets (record-count abstraction)')}
function step(){t++;produce(read('producers')*read('rate'));for(let p of P){elect(p);if(p.leader!==null){for(let b of p.replicas){if(b!==p.leader&&B[b].online){p.offsets[b]=Math.min(p.leo,Math.max(p.start,p.offsets[b])+read('replicate'));if(p.offsets[b]===p.leo&&!p.isr.includes(b))p.isr.push(b);p.behind[b]=p.offsets[b]===p.leo?0:(p.behind[b]||0)+1;if(p.behind[b]>=read('lagTimeout'))p.isr=p.isr.filter(x=>x!==b)}}p.hw=Math.max(p.hw,Math.min(...p.isr.map(b=>p.offsets[b]),p.leo));let done=p.pending.filter(x=>x<=p.hw);if(p.isr.length>=read('minisr'))ackCount+=done.length;else if(done.length)log('NotEnoughReplicasAfterAppend: '+done.length);p.pending=p.pending.filter(x=>x>p.hw);}syncGroups(p)}let c=read('consumers');if(t>=rebalanceUntil)for(let g=0;g<read('groups');g++)for(let i=0;i<c;i++){let budget=read('consume');for(let j=0;j<P.length;j++){let ix=(j+t)%P.length;if(ix%c!==i)continue;let p=P[ix];if(p.leader===null)continue;if(p.pos[g]<p.start){p.pos[g]=p.start;log('Offset out of range; reset to earliest')}let take=Math.min(budget,Math.max(0,p.hw-p.pos[g]));p.pos[g]+=take;budget-=take}}if($('auto').checked&&t%read('commitInterval')===0&&t>=rebalanceUntil)commit();history.push(lag());history=history.slice(-80);render()}
function lag(){return P.reduce((sum,p)=>{syncGroups(p);return sum+p.comm.slice(0,read('groups')).reduce((a,x)=>a+Math.max(0,p.leo-x),0)},0)}
function render(){let n=lag();$('clock').textContent='t = '+t+' s';$('pause').textContent=running?'Pause':'Run';$('stats').innerHTML=[['Committed lag',n],['Records retained',P.reduce((a,p)=>a+p.leo-p.start,0)],['Brokers online',B.filter(b=>b.online).length+'/'+B.length],['Under replicated',P.filter(p=>p.isr.length<p.replicas.length).length],['Acknowledged',ackCount],['Rejected',rejected],['Pending ACK',P.reduce((a,p)=>a+p.pending.length,0)],['Group state',t<rebalanceUntil?'REBALANCING':'STABLE']].map(([a,b])=>'<div class="stat"><small>'+a+'</small><strong>'+b+'</strong></div>').join('');drawTopology();drawView();if(typeof renderInternal==='function'&&window.labReady)renderInternal();if(window.labReady)refreshDials();$('log').textContent=events.join('\n');$('log').style.whiteSpace='pre-wrap';let max=Math.max(1,...history);$('chart').innerHTML='<path d="M0 110 H600 M0 55 H600" stroke="#334155" fill="none"/><polyline fill="none" stroke="#2dd4bf" stroke-width="3" points="'+history.map((v,i)=>[i*600/Math.max(1,history.length-1),110-v/max*100].join(',')).join(' ')+'"/>'}
function recoverAll(){B.forEach(b=>{if(!b.online)toggle(b.id)});log('Recovery started');render()}
function seekOffsets() {
  const topologyTab = $('workspace-topology-tab');
  const openedFromAnotherTab = topologyTab.getAttribute('aria-selected') === 'false';
  chooseWorkspace(topologyTab);

  const existingPanel = $('seekPanel');
  if (existingPanel) {
    if (!openedFromAnotherTab) {
      existingPanel.remove();
      return;
    }
  } else {
    const max = Math.max(0, ...P.map(partition => partition.hw));
    $('nodeDetail').insertAdjacentHTML('afterend',
      '<div class="detail" id="seekPanel"><label>Seek group 0 <input id="seekSlider" type="range" min="0" max="' +
      max + '" value="0" oninput="document.getElementById(&quot;seekValue&quot;).textContent=this.value">' +
      '<output id="seekValue">0</output></label><div class="toolbar">' +
      '<button onclick="doSeek(&quot;earliest&quot;)">Earliest</button>' +
      '<button onclick="doSeek(&quot;latest&quot;)">Latest</button>' +
      '<button onclick="doSeek(document.getElementById(&quot;seekSlider&quot;).value)">Apply offset</button></div></div>');
  }

  $('seekPanel').scrollIntoView({block: 'nearest'});
  $('seekSlider').focus({preventScroll: true});
}
function doSeek(v){for(let p of P)p.pos[0]=v==='earliest'?p.start:v==='latest'?p.hw:clamp(Number(v)||0,p.start,p.hw);log('Seek group 0: '+v);$('seekPanel')?.remove();render()}
function selectNode(kind,i,j){selected={kind,i,j};drawTopology()}

const topologyViewport = $('flow');
let topologyZoom = 1;
let topologyFit = false;

function updateTopologyZoom() {
  topologyViewport.classList.toggle('fit', topologyFit);
  topologyViewport.style.setProperty('--topology-zoom', topologyZoom);
  $('topology-zoom-level').textContent = topologyFit ? 'Fit' : Math.round(topologyZoom * 100) + '%';
  $('topology-fit').setAttribute('aria-pressed', topologyFit);
  $('topology-zoom-out').disabled = !topologyFit && topologyZoom === 0.5;
  $('topology-zoom-in').disabled = !topologyFit && topologyZoom === 2;
}

function changeTopologyZoom(delta) {
  if (topologyFit) {
    topologyZoom = clamp(Math.round(Math.min(1, topologyViewport.clientWidth / 1280) * 4) / 4, 0.5, 1);
  }
  topologyZoom = clamp(topologyZoom + delta, 0.5, 2);
  topologyFit = false;
  updateTopologyZoom();
}

$('topology-zoom-out').addEventListener('click', () => changeTopologyZoom(-0.25));
$('topology-zoom-in').addEventListener('click', () => changeTopologyZoom(0.25));
$('topology-fit').addEventListener('click', () => {
  topologyFit = !topologyFit;
  updateTopologyZoom();
});
$('topology-clear').addEventListener('click', () => {
  selected = null;
  drawTopology();
  topologyViewport.focus({preventScroll: true});
});
topologyViewport.addEventListener('keydown', event => {
  if (event.key === 'Escape' && selected) {
    $('topology-clear').click();
  } else if (event.target.dataset.topologyNode && ['Enter', ' '].includes(event.key)) {
    event.preventDefault();
    event.target.dispatchEvent(new MouseEvent('click', {bubbles: true}));
  }
});

function drawTopology() {
  const consumers = read('consumers');
  const groups = read('groups');
  const producers = read('producers');
  const producing = read('rate') > 0;
  const consuming = t >= rebalanceUntil && read('consume') > 0;
  const columns = Math.min(3, B.length);
  const brokerWidth = 226;
  const brokerGap = 24;
  const brokerStart = 255 + (3 - columns) * (brokerWidth + brokerGap) / 2;
  const brokerPartitions = B.map(broker => P.map((partition, index) => ({partition, index}))
    .filter(({partition}) => partition.replicas.includes(broker.id)));
  const brokerHeight = 66 + Math.max(...brokerPartitions.map(partitions => partitions.length)) * 28;
  const nodeStride = 58;
  const firstRow = 46;
  const producerX = 24;
  const consumerX = 1100;
  const clientWidth = 156;
  const height = Math.max(
    Math.ceil(B.length / columns) * (brokerHeight + 38) + 24,
    Math.max(producers, consumers * groups) * nodeStride + 70,
    440
  );
  const backgrounds = [];
  const edges = [];
  const nodes = [];
  const locations = P.map(() => []);
  const markerColors = {produce: '#2dd4bf', consume: '#7dd3fc', replica: '#c4a5ff'};
  const markers = Object.entries(markerColors).map(([kind, color]) =>
    '<marker id="topology-' + kind + '" viewBox="0 0 9 8" refX="8" refY="4" markerWidth="7" markerHeight="7" markerUnits="userSpaceOnUse" orient="auto"><path d="M0 0 L9 4 L0 8 Z" fill="' + color + '"/></marker>'
  ).join('');

  if (selected?.kind === 'producer' && selected.i >= producers ||
      selected?.kind === 'consumer' && (selected.i >= consumers || selected.j >= groups)) {
    selected = null;
  }
  const activeNode = document.activeElement?.dataset.topologyNode;
  const nodeIsSelected = (kind, i, j) => selected?.kind === kind && selected.i === i && selected.j === j;
  function nodeAttributes(kind, i, j, label, action) {
    return 'class="svgclick' + (nodeIsSelected(kind, i, j) ? ' selected' : '') +
      '" data-topology-node="' + kind + '-' + i + '-' + j + '" tabindex="0" role="button" aria-label="' +
      label + '" onclick="' + action + '"';
  }
  function box(x, y, width, title, subtitle, color, attributes) {
    return '<g ' + attributes + '><rect x="' + x + '" y="' + y + '" width="' + width +
      '" height="44" rx="8" fill="#15243a" stroke="' + color + '"/>' +
      '<text x="' + (x + 12) + '" y="' + (y + 18) + '" class="node-title">' + title + '</text>' +
      '<text x="' + (x + 12) + '" y="' + (y + 35) + '" class="node-subtitle" fill="' + color + '">' +
      subtitle + '</text></g>';
  }
  function connect(kind, from, to, active, matches) {
    const middle = (from.x + to.x) / 2;
    const state = selected ? (matches ? ' highlighted' : ' dimmed') : '';
    const path = 'M' + from.x + ' ' + from.y + ' C' + middle + ' ' + from.y +
      ' ' + middle + ' ' + to.y + ' ' + to.x + ' ' + to.y;
    edges.push({highlighted: selected && matches, html: '<path class="edge ' + kind +
      (active ? ' animated' : '') + state + '" marker-end="url(#topology-' + kind + ')" d="' + path + '"/>'});
  }

  nodes.push(
    '<text x="24" y="27" class="lane-label">PRODUCERS</text>',
    '<text x="' + brokerStart + '" y="27" class="lane-label">BROKERS / PARTITION REPLICAS</text>',
    '<text x="1100" y="27" class="lane-label">CONSUMERS</text>'
  );
  B.forEach((broker, i) => {
    const x = brokerStart + (i % columns) * (brokerWidth + brokerGap);
    const y = firstRow + Math.floor(i / columns) * (brokerHeight + 38);
    const color = broker.online ? '#5eead4' : '#fb7185';
    backgrounds.push('<rect x="' + x + '" y="' + y + '" width="' + brokerWidth + '" height="' +
      brokerHeight + '" rx="12" fill="#0c1728" stroke="' + (broker.online ? '#405c7b' : color) + '"/>');
    nodes.push(box(x + 7, y + 7, brokerWidth - 14, 'Broker ' + i, broker.online ? 'Online' : 'Offline', color,
      nodeAttributes('broker', i, 0, (broker.online ? 'Stop' : 'Recover') + ' broker ' + i, 'toggle(' + i + ')')));
    brokerPartitions[i].forEach(({partition, index}, row) => {
      const top = y + 59 + row * 28;
      const leader = partition.leader === i;
      const title = 'T' + partition.topic + ' / P' + partition.id + ' · ' + (leader ? 'Leader' : 'Follower');
      const attributes = nodeAttributes('partition', index, i, title + ' on broker ' + i,
        'selectNode(&quot;partition&quot;,' + index + ',' + i + ')');
      locations[index][i] = {left: x + 8, right: x + brokerWidth - 8, y: top + 12};
      nodes.push('<g ' + attributes + '><rect x="' + (x + 8) + '" y="' + top + '" width="' +
        (brokerWidth - 16) + '" height="24" rx="5" fill="' + (leader ? '#473a19' : '#20324a') +
        '" stroke="' + (leader ? '#fbbf24' : '#405c7b') + '"/><text x="' + (x + 17) + '" y="' +
        (top + 16) + '" fill="#e2e8f0" font-size="13">' + title + ' · ' + partition.offsets[i] +
        (partition.isr.includes(i) ? ' ✓' : '') + '</text></g>');
    });
  });
  for (let i = 0; i < producers; i++) {
    const y = firstRow + i * nodeStride;
    nodes.push(box(producerX, y, clientWidth, 'Producer ' + i, read('rate') + ' msg/s', '#5eead4',
      nodeAttributes('producer', i, 0, 'Inspect producer ' + i, 'selectNode(&quot;producer&quot;,' + i + ',0)')));
    P.forEach((partition, index) => {
      if (partition.leader === null) return;
      const leader = locations[index][partition.leader];
      if (producers <= 5 || index % producers === i || selected?.kind === 'producer' && selected.i === i) {
        connect('produce', {x: producerX + clientWidth, y: y + 22}, {x: leader.left, y: leader.y},
          producing, selected?.kind === 'partition' && selected.i === index || nodeIsSelected('producer', i, 0));
      }
    });
  }
  P.forEach((partition, index) => {
    if (partition.leader === null) return;
    const leader = locations[index][partition.leader];
    const partitionSelected = selected?.kind === 'partition' && selected.i === index;
    for (const broker of partition.replicas) {
      if (broker === partition.leader || !B[broker].online) continue;
      const follower = locations[index][broker];
      const flowsRight = follower.left >= leader.left;
      connect('replica', {x: flowsRight ? leader.right : leader.left, y: leader.y},
        {x: flowsRight ? follower.left : follower.right, y: follower.y}, true, partitionSelected);
    }
    for (let group = 0; group < groups; group++) {
      if (!consumers) continue;
      const owner = index % consumers;
      connect('consume', {x: leader.right, y: leader.y},
        {x: consumerX, y: firstRow + (group * consumers + owner) * nodeStride + 22},
        consuming, partitionSelected || nodeIsSelected('consumer', owner, group));
    }
  });
  for (let group = 0; group < groups; group++) {
    for (let i = 0; i < consumers; i++) {
      const y = firstRow + (group * consumers + i) * nodeStride;
      const assigned = P.filter((_, index) => index % consumers === i).length;
      const label = 'Group ' + group + ' / Consumer ' + i;
      nodes.push(box(consumerX, y, clientWidth, 'G' + group + ' / C' + i,
        t < rebalanceUntil ? 'Rebalancing' : assigned + ' partitions', '#7dd3fc',
        nodeAttributes('consumer', i, group, 'Inspect ' + label, 'selectNode(&quot;consumer&quot;,' + i + ',' + group + ')')));
    }
  }

  edges.sort((a, b) => Number(a.highlighted) - Number(b.highlighted));
  topologyViewport.classList.toggle('paused', !running);
  topologyViewport.innerHTML = '<svg viewBox="0 0 1280 ' + height + '" xmlns="http://www.w3.org/2000/svg"><defs>' +
    markers + '</defs>' + backgrounds.join('') + edges.map(edge => edge.html).join('') + nodes.join('') + '</svg>';
  if (activeNode) {
    const replacement = topologyViewport.querySelector('[data-topology-node="' + activeNode + '"]');
    (replacement ?? topologyViewport).focus({preventScroll: true});
  }

  const detail = $('nodeDetail');
  $('topology-clear').disabled = !selected;
  if (!selected) {
    detail.textContent = 'Select a partition, producer, or consumer to inspect its state.';
    return;
  }
  const {kind, i, j} = selected;
  if (kind === 'partition') {
    const partition = P[i];
    detail.innerHTML = '<b>Topic ' + partition.topic + ' / Partition ' + partition.id + ' · Broker ' + j +
      '</b><div class="metricgrid"><span>Leader: ' + (partition.leader ?? 'none') + '</span><span>LEO: ' +
      partition.leo + '</span><span>HW: ' + partition.hw + '</span><span>ISR: ' + partition.isr.join(', ') +
      '</span><span>Start: ' + partition.start + '</span><span>Replica LEO: ' + partition.offsets[j] +
      '</span></div>LEO is the next write offset; HW is the readable boundary; ISR is the in-sync replica set.';
  } else if (kind === 'consumer') {
    const own = P.filter((_, index) => index % consumers === i);
    detail.innerHTML = '<b>Group ' + j + ' / Consumer ' + i + '</b><br>Assigned: ' +
      (own.map(partition => 'T' + partition.topic + '/P' + partition.id).join(', ') || 'none') + '<br>Lag: ' +
      own.reduce((total, partition) => total + Math.max(0, partition.leo - (partition.comm[j] ?? 0)), 0) +
      '<br>Each partition has at most one owner per group.';
  } else {
    detail.innerHTML = '<b>Producer ' + i + '</b><br>acks=' + $('acks').value + ' · ' + read('rate') +
      ' records/s<br>Producers use metadata to locate the partition leader.';
  }
}
const tabs=[['logs','Partition logs'],['groups','Consumer groups'],['replicas','Replication / ISR'],['internals','Concepts'],['protocol','Protocol flow']];
$('tabs').innerHTML=tabs.map(([id,l])=>'<button class="tab" id="tab-'+id+'" onclick="tab=\''+id+'\';drawView()">'+l+'</button>').join('');
function drawView(){tabs.forEach(([id])=>$('tab-'+id).classList.toggle('active',id===tab));let v=$('view');if(tab==='logs')v.innerHTML=P.map(p=>'<div class="partition"><strong>Topic '+p.topic+' / Partition '+p.id+'</strong> <small>LEO '+p.leo+' · HW '+p.hw+' · Start '+p.start+'</small><div class="blocks">'+Array.from({length:Math.min(24,p.leo-p.start)},(_,i)=>{let o=Math.max(p.start,p.leo-24)+i;return '<span class="block '+(o>=p.hw?'pending':o<p.comm[0]?'consumed':'')+'">'+o+'</span>'}).join('')+'</div><small>Green: committed · blue: readable · amber: above HW</small></div>').join('');if(tab==='groups')v.innerHTML=Array.from({length:read('groups')},(_,g)=>'<h3>Group '+g+' · generation '+epoch+'</h3><table><tr><th>Partition</th><th>Owner</th><th>Position</th><th>Commit</th><th>Lag</th></tr>'+P.map((p,i)=>'<tr><td>T'+p.topic+'/P'+p.id+'</td><td>'+(read('consumers')?'C'+i%read('consumers'):'unassigned')+'</td><td>'+p.pos[g]+'</td><td>'+p.comm[g]+'</td><td>'+Math.max(0,p.leo-p.comm[g])+'</td></tr>').join('')+'</table>').join('');if(tab==='replicas')v.innerHTML='<table><tr><th>Partition</th><th>Leader</th><th>ISR</th><th>Replica LEO</th><th>HW</th></tr>'+P.map(p=>'<tr><td>T'+p.topic+'/P'+p.id+'</td><td>'+ (p.leader===null?'NONE':'B'+p.leader)+'</td><td>'+p.isr.join(', ')+'</td><td>'+p.replicas.map(b=>'B'+b+': '+p.offsets[b]).join(' / ')+'</td><td>'+p.hw+'</td></tr>').join('')+'</table>';if(tab==='protocol')v.innerHTML='<div class="hint"><h3>Protocol flow Kafka</h3><div dir="ltr" class="detail">Metadata request · Producer → Controller metadata via Broker<br>Produce request · Producer → Partition Leader<br>Replica Fetch · Follower → Leader<br>Produce response · Leader → Producer (acks)<br>FindCoordinator · Consumer → Broker<br>JoinGroup / SyncGroup · Consumer → Coordinator<br>Fetch · Consumer → Leader (up to HW)<br>OffsetCommit · Consumer → __consumer_offsets</div><h3>ACK versus offset commit</h3><p>ACK confirms a write; offset commit checkpoints consumer progress. Reduce replication speed to observe pending ACKs.</p></div>';if(tab==='internals')v.innerHTML='<div class="hint">'+lessons.map(([h,b])=>'<h3>'+h+'</h3><p>'+b+'</p>').join('')+'</div>'}
const lessons=[['Log and offsets','Each partition is an ordered append-only log. Offsets are local to a partition. Consuming records does not delete them.'],['Leader, follower, ISR','Producers append to the leader. Followers fetch from the leader. ISR tracks replicas sufficiently caught up for safe election.'],['High watermark','Consumers normally read only records below the high watermark.'],['Consumer groups','Each partition has one owner per group. Position and committed offset differ; restart can replay records.'],['Acknowledgments','acks=all requires sufficient in-sync replicas. acks=0 does not wait for a response.'],['Retention','Kafka removes data independently of consumer progress; real retention uses segment age and size.'],['KRaft','Controllers replicate metadata and elect leaders. Losing controller quorum prevents metadata changes.'],['Model scope','Internal pipelines use simplified logical counters and demonstrate causal relationships, not physical Kafka timing.']];
const scenarios=[['Lag growing',()=>{$('rate').value=30;$('consume').value=2}],['Catch up',()=>{$('rate').value=0;$('consume').value=50}],['Slow replication',()=>{$('replicate').value=1;$('rate').value=30}],['Leader failure',()=>{let p=P.find(p=>p.leader!==null);if(p)toggle(p.leader)}],['Replay after restart',()=>{$('auto').checked=false;restart()}],['ACK blocked by ISR',()=>{$('acks').value='all';$('minisr').value=s.rf;let b=B.find(b=>b.online);if(b)toggle(b.id)}],['Uncommitted replay',()=>{$('auto').checked=false;restart()}],['More consumers',()=>{$('consumers').value=read('consumers')+2;rebalance()}]];
$('scenarios').innerHTML=scenarios.map(([l],i)=>'<button style="display:block;width:100%;margin:6px 0" onclick="scenarios['+i+'][1]();render()">'+l+'</button>').join('');$('config').addEventListener('change',()=>apply());$('live').addEventListener('change',e=>{if(['groups','consumers'].includes(e.target.id))rebalance();render()});apply();setInterval(()=>{if(running)step()},1000)
