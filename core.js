
const $=id=>document.getElementById(id), clamp=(v,a,b)=>Math.max(a,Math.min(b,v));let B=[],P=[],t=0,running=false,sent=0,rejected=0,rr=0,tab='logs',s={},selected=null,ackCount=0,unacked=0;
const cfg=[['brokers','Brokers',3,1,12],['topics','Topics',2,1,6],['parts','Partitions / topic',3,1,12],['rf','Replication factor',3,1,12],['initial','Initial records / partition',8,0,100000]];
const live=[['producers','Producers',2,0,20],['rate','Produce / producer / sec',5,0,1000],['consumers','Consumers / group',3,0,30],['groups','Consumer groups',1,1,5],['consume','Consume / consumer / sec',3,0,1000],['replicate','Replica fetch / sec',60,0,10000],['minisr','min.insync.replicas',2,1,12],['retention','Retention records',100,1,100000],['commitInterval','Commit interval (s)',5,1,60],['rebalanceDelay','Rebalance delay (s)',2,0,30],['lagTimeout','Replica lag ticks',3,1,60]];
function fields(arr,where){$(where).innerHTML=arr.map(([id,label,v,min,max])=>'<div class="dialfield"><div class="dialtitle"><label for="'+id+'">'+label+'</label><output id="out-'+id+'">'+v+'</output></div><div class="dialrow"><button class="dial" data-input="'+id+'" aria-label="Adjust '+label+'" style="--angle:0deg"><span></span></button><input id="'+id+'" type="range" min="'+min+'" max="'+max+'" value="'+v+'" step="1"><button class="nudge" onclick="nudge(&quot;'+id+'&quot;,-1)">−</button><button class="nudge" onclick="nudge(&quot;'+id+'&quot;,1)">+</button></div></div>').join('')}
const quickControls = new Set(['rate', 'consume']);
fields(cfg,'config');
fields(live.filter(([id]) => quickControls.has(id)), 'flow-settings');
fields(live.filter(([id]) => !quickControls.has(id)), 'live');$('live').insertAdjacentHTML('beforeend','<label class="field">acks<select id="acks"><option>all</option><option>1</option><option>0</option></select></label><label class="field">Auto commit<input id="auto" type="checkbox" checked></label><label class="field">Partitioner<select id="partitioner"><option value="rr">Round robin</option><option value="key">Key hash</option></select></label><label class="field">Key<input id="key" value="order-42"></label>');
function read(id){let e=$(id);return clamp(Math.floor(+e.value||0),+e.min,+e.max)}
function log(message, source = 'Cluster', level = 'info') {
  monitoring.record(t, source, level, message);
}
function apply(){s=Object.fromEntries(cfg.map(([id])=>[id,read(id)]));s.rf=Math.min(s.rf,s.brokers);$('rf').value=s.rf;reset()}
function reset(){running=false;t=sent=rejected=rr=ackCount=unacked=0;selected=null;monitoring.reset();B=Array.from({length:s.brokers},(_,i)=>({id:i,online:true}));P=[];for(let k=0;k<s.topics;k++)for(let i=0;i<s.parts;i++){let replicas=Array.from({length:s.rf},(_,j)=>(i+k+j)%B.length);P.push({topic:k,id:i,replicas,leader:replicas[0],leo:s.initial,start:0,hw:s.initial,offsets:Object.fromEntries(replicas.map(x=>[x,s.initial])),isr:[...replicas],behind:{},pending:[],pos:[],comm:[]})}recordTrace.reset(s.topics);transactions.reset();consumerMembership.reset();log('Cluster initialized');render()}
function syncGroups(p){for(let g=0;g<read('groups');g++){if(p.pos[g]===undefined)p.pos[g]=p.comm[g]=p.start}}
function rebalance() {
  consumerMembership.sync(true);
}
function toggleConsumer(group, consumer) {
  consumerMembership.toggle(group, consumer);
  render();
}
function elect(p){if(p.leader!==null&&B[p.leader].online)return;let n=p.isr.find(x=>B[x].online);if(n===undefined){p.leader=null;return}p.leader=n;p.leo=p.offsets[n];p.hw=Math.min(p.hw,p.leo);p.pending=p.pending.filter(x=>x<=p.leo);for(let b of p.replicas)p.offsets[b]=Math.min(p.offsets[b],p.leo);recordTrace.prune(p);transactions.prune(p);logStorage.prune(p);log('Leader elected B'+n+' for T'+p.topic+'/P'+p.id, 'Controller')}
function toggle(i){B[i].online=!B[i].online;log('Broker '+i+(B[i].online?' recovered':' stopped'), 'Broker', B[i].online?'info':'warn');P.forEach(p=>{if(!B[i].online){elect(p);p.isr=p.isr.filter(x=>x!==i)}else{if(p.offsets[i]>=p.hw&&p.replicas.includes(i)&&!p.isr.includes(i))p.isr.push(i);elect(p)} });render()}
function hashKey(key) {
  let hash = 0;
  for (const character of key) hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  return hash;
}

function appendRejection(partition, acks, minIsr) {
  if (partition.leader === null || !B[partition.leader].online) return 'No online leader';
  if (acks === 'all' && partition.isr.length < minIsr) return 'Not enough in-sync replicas';
  return '';
}

function appendRecord(partition, acks) {
  const offset = partition.leo++;
  partition.offsets[partition.leader] = partition.leo;
  sent++;
  if (acks === 'all') partition.pending.push(partition.leo);
  else if (acks === '1') ackCount++;
  return offset;
}

function produce(n) {
  n = clamp(Math.floor(n) || 0, 0, 10000);
  const keyed = $('partitioner').value === 'key';
  const hash = hashKey($('key').value);
  const acks = $('acks').value;
  const minIsr = read('minisr');
  let accepted = 0;
  for (let i = 0; i < n; i++) {
    const partition = P[(keyed ? hash : rr++) % P.length];
    if (appendRejection(partition, acks, minIsr)) {
      rejected++;
      continue;
    }
    appendRecord(partition, acks);
    accepted++;
  }
  if (n) log('Produce ' + accepted + '/' + n + ' appended; acks=' + acks, 'Producer',
    accepted === 0 ? 'error' : accepted < n ? 'warn' : 'info');
}
function commit() {
  let assigned = false;
  for (const group of consumerMembership.groups) {
    for (let index = 0; index < P.length; index++) {
      if (consumerMembership.owner(group.id, index) === null) continue;
      const partition = P[index];
      syncGroups(partition);
      partition.comm[group.id] = partition.pos[group.id];
      assigned = true;
    }
  }
  if (assigned) log('Offsets committed to __consumer_offsets', 'Consumer group');
}
function restart(){P.forEach(p=>p.pos=[...p.comm]);rebalance();log('Consumer positions restored from committed offsets', 'Consumer group')}
function retain() {
  const retention = read('retention');
  let expired = 0;
  for (const partition of P) {
    const start = logStorage.retentionStart(partition, retention);
    expired += logStorage.recordCount(partition, partition.start, start);
    partition.start = start;
    recordTrace.prune(partition);
    transactions.prune(partition);
    logStorage.prune(partition);
  }
  log('Retention removed ' + expired + ' records from the start of partition logs', 'Storage');
}

function replicatePartition(partition, fetchRate, lagTimeout, minIsr) {
  const p = partition;
  for (const broker of p.replicas) {
    if (broker === p.leader || !B[broker].online) continue;
    p.offsets[broker] = Math.min(p.leo, Math.max(p.start, p.offsets[broker]) + fetchRate);
    const caughtUp = p.offsets[broker] === p.leo;
    if (caughtUp && !p.isr.includes(broker)) p.isr.push(broker);
    p.behind[broker] = caughtUp ? 0 : (p.behind[broker] || 0) + 1;
    if (p.behind[broker] >= lagTimeout) p.isr = p.isr.filter(id => id !== broker);
  }

  p.hw = Math.max(p.hw, Math.min(...p.isr.map(broker => p.offsets[broker]), p.leo));
  const done = p.pending.filter(offset => offset <= p.hw);
  p.pending = p.pending.filter(offset => offset > p.hw);
  const acknowledged = p.isr.length >= minIsr;
  if (acknowledged) ackCount += done.length;
  else if (done.length) log('NotEnoughReplicasAfterAppend: ' + done.length, 'Replication', 'error');
  if (p.tracedRecords?.size) {
    for (const offset of done) recordTrace.replicaAck(p, offset, acknowledged);
  }
}

function consumePartitions() {
  const limit = read('consume');
  let processed = 0;
  for (const {id: group, members, rebalanceAt} of consumerMembership.groups) {
    if (rebalanceAt !== null) continue;
    for (let consumer = 0; consumer < members.length; consumer++) {
      if (!members[consumer]) continue;
      let budget = limit;
      for (let j = 0; j < P.length; j++) {
        const index = (j + t) % P.length;
        if (consumerMembership.owner(group, index) !== consumer) continue;
        const partition = P[index];
        if (partition.leader === null) continue;
        if (partition.pos[group] < partition.start) {
          partition.pos[group] = partition.start;
          log('Offset out of range; reset to earliest', 'Consumer group', 'warn');
        }
        const {position, consumed} = transactions.readRecords(partition, partition.pos[group], budget,
          (start, end) => recordTrace.consume(partition, group, consumer, start, end));
        partition.pos[group] = position;
        budget -= consumed;
        processed += consumed;
      }
    }
  }
  return processed;
}

function step() {
  consumerMembership.sync();
  t++;
  produce(read('producers') * read('rate'));
  const fetchRate = read('replicate');
  const lagTimeout = read('lagTimeout');
  const minIsr = read('minisr');
  for (const partition of P) {
    elect(partition);
    if (partition.leader !== null) replicatePartition(partition, fetchRate, lagTimeout, minIsr);
    syncGroups(partition);
  }

  transactions.advance();
  consumerMembership.advance();
  const processed = consumePartitions();
  if ($('auto').checked && t % read('commitInterval') === 0) commit();
  render();
  return processed;
}
function lagByGroup() {
  const totals = Array(read('groups')).fill(0);
  for (const partition of P) {
    syncGroups(partition);
    for (let group = 0; group < totals.length; group++) {
      totals[group] += Math.max(0, partition.leo - partition.comm[group]);
    }
  }
  return totals;
}
function lag() {
  return lagByGroup().reduce((total, group) => total + group, 0);
}
function readableOffsets(start, end, skipped) {
  let count = end - start;
  let frontier = start;
  for (const range of skipped) {
    if (range.start >= end) break;
    const from = Math.max(frontier, range.start);
    const to = Math.min(end, range.end);
    if (to <= from) continue;
    count -= to - from;
    frontier = to;
  }
  return count;
}

function lagBreakdownByGroup() {
  const groups = Array.from({length: read('groups')}, () => ({
    replication: 0, ready: 0, commit: 0, transaction: 0, unavailable: 0
  }));
  for (const partition of P) {
    syncGroups(partition);
    const skipped = transactions.skippedRanges(partition);
    const readLimit = transactions.readLimit(partition);
    for (const [group, breakdown] of groups.entries()) {
      const checkpoint = clamp(partition.comm[group], 0, partition.leo);
      const retained = clamp(partition.start, checkpoint, partition.leo);
      const replicated = clamp(partition.hw, retained, partition.leo);
      const readable = clamp(readLimit, retained, replicated);
      const position = clamp(partition.pos[group], retained, readable);
      const ready = readableOffsets(position, readable, skipped);
      const commit = readableOffsets(retained, position, skipped);

      breakdown.replication += partition.leo - replicated;
      breakdown.transaction += replicated - readable;
      breakdown.ready += ready;
      breakdown.commit += commit;
      breakdown.unavailable += readable - checkpoint - ready - commit;
    }
  }
  return groups;
}
function render(){consumerMembership.sync();let n=monitoring.addSample(t,lagBreakdownByGroup());$('clock').textContent='t = '+t+' s';$('pause').textContent=running?'Pause':'Run';$('stats').innerHTML=[['Committed lag',n],['Records retained',P.reduce((total, partition) => total + logStorage.recordCount(partition), 0)],['Brokers online',B.filter(b=>b.online).length+'/'+B.length],['Under replicated',P.filter(p=>p.isr.length<p.replicas.length).length],['Acknowledged',ackCount],['Rejected',rejected],['Pending ACK',P.reduce((a,p)=>a+p.pending.length,0)],['Group state',consumerMembership.summary()]].map(([a,b])=>'<div class="stat"><small>'+a+'</small><strong>'+b+'</strong></div>').join('');drawTopology();drawView();transactions.updateControls();if(typeof renderInternal==='function'&&window.labReady)renderInternal();if(window.labReady)refreshDials()}
function recoverAll(){B.forEach(b=>{if(!b.online)toggle(b.id)});log('Recovery started', 'Broker');render()}
function seekOffsets(){let id='seekPanel';if($(id)){ $(id).remove();return }let max=Math.max(0,...P.map(p=>p.hw));$('nodeDetail').insertAdjacentHTML('afterend','<div class="detail" id="seekPanel"><label>Seek group 0 <input id="seekSlider" type="range" min="0" max="'+max+'" value="0" oninput="document.getElementById(&quot;seekValue&quot;).textContent=this.value"><output id="seekValue">0</output></label><div class="toolbar"><button onclick="doSeek(&quot;earliest&quot;)">Earliest</button><button onclick="doSeek(&quot;latest&quot;)">Latest</button><button onclick="doSeek(document.getElementById(&quot;seekSlider&quot;).value)">Apply offset</button></div></div>');$('seekPanel').scrollIntoView({block:'nearest'})}
function doSeek(v){for(let p of P)p.pos[0]=v==='earliest'?p.start:v==='latest'?transactions.readLimit(p):clamp(Number(v)||0,p.start,p.hw);log('Seek group 0: '+v, 'Consumer group');$('seekPanel')?.remove();render()}
function selectNode(kind,i,j){selected={kind,i,j};drawTopology()}

const topologyViewport = $('flow');
const topologyWidth = 1280;
const topologyOffsets = new Map();
let topologyBounds = new Map();
let topologyEdgesByNode = new Map();
let topologyHeight = 0;
let topologyGesture = null;
let topologyRedrawPending = false;
let suppressTopologyClick = false;
let topologyZoom = 1;
let topologyFit = false;

function topologyOffset(key) {
  return topologyOffsets.get(key) ?? {x: 0, y: 0};
}

function boundedTopologyOffset(bounds, x, y) {
  return {
    x: clamp(x, 8 - bounds.x, topologyWidth - bounds.x - bounds.width - 8),
    y: clamp(y, 38 - bounds.y, topologyHeight - bounds.y - bounds.height - 8)
  };
}

function topologyTransform({x, y}) {
  return 'translate(' + x + ' ' + y + ')';
}

function topologyEdgePath({from, to}) {
  const start = topologyOffset(from.owner);
  const end = topologyOffset(to.owner);
  const forward = from.left + from.right + 2 * start.x <= to.left + to.right + 2 * end.x;
  const x = (forward ? from.right : from.left) + start.x;
  const X = (forward ? to.left : to.right) + end.x;
  const y = from.y + start.y;
  const Y = to.y + end.y;
  const middle = (x + X) / 2;
  return 'M' + x + ' ' + y + ' C' + middle + ' ' + y + ' ' + middle + ' ' + Y + ' ' + X + ' ' + Y;
}

function moveTopologyNode(key, x, y) {
  const bounds = topologyBounds.get(key);
  if (!bounds) return;
  const offset = boundedTopologyOffset(bounds, x, y);
  topologyOffsets.set(key, offset);
  for (const element of topologyViewport.querySelectorAll('[data-topology-layout="' + key + '"]')) {
    element.setAttribute('transform', topologyTransform(offset));
  }
  for (const edge of topologyEdgesByNode.get(key) ?? []) {
    edge.element.setAttribute('d', topologyEdgePath(edge));
  }
}

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
    topologyZoom = clamp(Math.round(Math.min(1, topologyViewport.clientWidth / topologyWidth) * 4) / 4, 0.5, 1);
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
$('topology-reset-layout').addEventListener('click', () => {
  topologyOffsets.clear();
  topologyViewport.scrollTo(0, 0);
  drawTopology();
});
$('topology-clear').addEventListener('click', () => {
  selected = null;
  drawTopology();
  topologyViewport.focus({preventScroll: true});
});
topologyViewport.addEventListener('pointerdown', event => {
  if (!event.isPrimary || event.button !== 0) return;
  suppressTopologyClick = false;
  const target = event.target.closest('[data-topology-node]');
  const key = target?.dataset.topologyDrag;
  if (target && !key) return;
  if (!target && !event.target.closest('svg')) return;
  topologyGesture = {
    id: event.pointerId, key, moved: false, x: event.clientX, y: event.clientY,
    start: key ? topologyOffset(key) : {x: topologyViewport.scrollLeft, y: topologyViewport.scrollTop},
    scale: topologyViewport.querySelector('svg').getBoundingClientRect().width / topologyWidth
  };
});
topologyViewport.addEventListener('pointermove', event => {
  const gesture = topologyGesture;
  if (gesture?.id !== event.pointerId) return;
  const dx = event.clientX - gesture.x;
  const dy = event.clientY - gesture.y;
  if (!gesture.moved) {
    if (Math.hypot(dx, dy) < 5) return;
    gesture.moved = true;
    topologyViewport.setPointerCapture(event.pointerId);
    topologyViewport.classList.add('dragging');
  }
  if (gesture.key) {
    moveTopologyNode(gesture.key, gesture.start.x + dx / gesture.scale, gesture.start.y + dy / gesture.scale);
  } else {
    topologyViewport.scrollTo(gesture.start.x - dx, gesture.start.y - dy);
  }
});
function finishTopologyGesture(event) {
  if (topologyGesture?.id !== event.pointerId) return;
  suppressTopologyClick = topologyGesture.moved;
  topologyGesture = null;
  topologyViewport.classList.remove('dragging');
  if (topologyRedrawPending) {
    requestAnimationFrame(() => {
      if (topologyRedrawPending) drawTopology();
    });
  }
}
topologyViewport.addEventListener('pointercancel', finishTopologyGesture);
topologyViewport.addEventListener('lostpointercapture', finishTopologyGesture);
document.addEventListener('pointerup', finishTopologyGesture);
topologyViewport.addEventListener('click', event => {
  if (!suppressTopologyClick || !event.detail) return;
  suppressTopologyClick = false;
  event.preventDefault();
  event.stopPropagation();
}, true);
topologyViewport.addEventListener('keydown', event => {
  if (event.key === 'Escape' && selected) {
    $('topology-clear').click();
  } else if (event.target.dataset.topologyNode && ['Enter', ' '].includes(event.key)) {
    event.preventDefault();
    event.target.dispatchEvent(new MouseEvent('click', {bubbles: true}));
  } else if (event.shiftKey && event.target.dataset.topologyDrag) {
    const directions = {ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1]};
    if (!Object.hasOwn(directions, event.key)) return;
    event.preventDefault();
    const key = event.target.dataset.topologyDrag;
    const offset = topologyOffset(key);
    const [dx, dy] = directions[event.key];
    moveTopologyNode(key, offset.x + dx * 20, offset.y + dy * 20);
  }
});

function drawTopology() {
  if (topologyGesture && !topologyGesture.moved) {
    topologyRedrawPending = true;
    return;
  }
  topologyRedrawPending = false;
  const groups = consumerMembership.groups.length;
  const consumers = consumerMembership.groups[0]?.members.length ?? 0;
  const producers = read('producers');
  const producing = read('rate') > 0;
  const consuming = read('consume') > 0;
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
  topologyHeight = Math.max(
    Math.ceil(B.length / columns) * (brokerHeight + 38) + 24,
    Math.max(producers, consumers * groups) * nodeStride + 70,
    440
  );
  const backgrounds = [];
  const edges = [];
  const nodes = [];
  const clients = new Map();
  const locations = P.map(() => []);
  topologyBounds = new Map();
  const markerColors = {produce: '#2dd4bf', consume: '#7dd3fc', replica: '#c4a5ff'};
  const markers = Object.entries(markerColors).map(([kind, color]) =>
    '<marker id="topology-' + kind + '" viewBox="0 0 9 8" refX="8" refY="4" markerWidth="7" markerHeight="7" markerUnits="userSpaceOnUse" orient="auto"><path d="M0 0 L9 4 L0 8 Z" fill="' + color + '"/></marker>'
  ).join('');

  if (selected?.kind === 'producer' && selected.i >= producers ||
      selected?.kind === 'consumer' && (selected.i >= consumers || selected.j >= groups)) {
    selected = null;
  }
  const activeNode = document.activeElement?.dataset.topologyNode;
  const memberActionFocused = document.activeElement?.id === 'consumer-member-action';
  const nodeIsSelected = (kind, i, j) => selected?.kind === kind && selected.i === i && selected.j === j;
  function layoutAttributes(owner) {
    return 'data-topology-layout="' + owner + '" transform="' + topologyTransform(topologyOffset(owner)) + '"';
  }
  function registerNode(owner, x, y, width, height) {
    const bounds = {x, y, width, height};
    topologyBounds.set(owner, bounds);
    if (topologyOffsets.has(owner)) {
      const offset = topologyOffset(owner);
      topologyOffsets.set(owner, boundedTopologyOffset(bounds, offset.x, offset.y));
    }
  }
  function nodeAttributes(kind, i, j, label, action, owner) {
    const key = kind + '-' + i + '-' + j;
    const drag = key === owner ? ' data-topology-drag="' + owner +
      '" aria-keyshortcuts="Shift+ArrowLeft Shift+ArrowRight Shift+ArrowUp Shift+ArrowDown"' : '';
    return 'class="svgclick' + (nodeIsSelected(kind, i, j) ? ' selected' : '') +
      '" data-topology-node="' + key + '" tabindex="0" role="button" aria-label="' +
      label + '" onclick="' + action + '" ' + layoutAttributes(owner) + drag;
  }
  function box(x, y, width, title, subtitle, color, attributes) {
    return '<g ' + attributes + '><rect x="' + x + '" y="' + y + '" width="' + width +
      '" height="44" rx="8" fill="#15243a" stroke="' + color + '"/>' +
      '<text x="' + (x + 12) + '" y="' + (y + 18) + '" class="node-title">' + title + '</text>' +
      '<text x="' + (x + 12) + '" y="' + (y + 35) + '" class="node-subtitle" fill="' + color + '">' +
      subtitle + '</text></g>';
  }
  function connect(kind, from, to, active, matches) {
    const state = selected ? (matches ? ' highlighted' : ' dimmed') : '';
    edges.push({from, to, highlighted: selected && matches, html: '<path class="edge ' + kind +
      (active ? ' animated' : '') + state + '" marker-end="url(#topology-' + kind + ')" d="' +
      topologyEdgePath({from, to}) + '"/>'});
  }

  nodes.push(
    '<text x="24" y="27" class="lane-label">PRODUCERS</text>',
    '<text x="' + brokerStart + '" y="27" class="lane-label">BROKERS / PARTITION REPLICAS</text>',
    '<text x="1100" y="27" class="lane-label">CONSUMERS</text>'
  );
  B.forEach((broker, i) => {
    const owner = 'broker-' + i + '-0';
    const x = brokerStart + (i % columns) * (brokerWidth + brokerGap);
    const y = firstRow + Math.floor(i / columns) * (brokerHeight + 38);
    const color = broker.online ? '#5eead4' : '#fb7185';
    registerNode(owner, x, y, brokerWidth, brokerHeight);
    backgrounds.push('<rect ' + layoutAttributes(owner) + ' x="' + x + '" y="' + y + '" width="' + brokerWidth + '" height="' +
      brokerHeight + '" rx="12" fill="#0c1728" stroke="' + (broker.online ? '#405c7b' : color) + '"/>');
    nodes.push(box(x + 7, y + 7, brokerWidth - 14, 'Broker ' + i, broker.online ? 'Online' : 'Offline', color,
      nodeAttributes('broker', i, 0, (broker.online ? 'Stop' : 'Recover') + ' broker ' + i, 'toggle(' + i + ')', owner)));
    brokerPartitions[i].forEach(({partition, index}, row) => {
      const top = y + 59 + row * 28;
      const leader = partition.leader === i;
      const title = 'T' + partition.topic + ' / P' + partition.id + ' · ' + (leader ? 'Leader' : 'Follower');
      const attributes = nodeAttributes('partition', index, i, title + ' on broker ' + i,
        'selectNode(&quot;partition&quot;,' + index + ',' + i + ')', owner);
      locations[index][i] = {owner, left: x + 8, right: x + brokerWidth - 8, y: top + 12};
      nodes.push('<g ' + attributes + '><rect x="' + (x + 8) + '" y="' + top + '" width="' +
        (brokerWidth - 16) + '" height="24" rx="5" fill="' + (leader ? '#473a19' : '#20324a') +
        '" stroke="' + (leader ? '#fbbf24' : '#405c7b') + '"/><text x="' + (x + 17) + '" y="' +
        (top + 16) + '" fill="#e2e8f0" font-size="13">' + title + ' · ' + partition.offsets[i] +
        (partition.isr.includes(i) ? ' ✓' : '') + '</text></g>');
    });
  });
  for (let i = 0; i < producers; i++) {
    const owner = 'producer-' + i + '-0';
    const y = firstRow + i * nodeStride;
    registerNode(owner, producerX, y, clientWidth, 44);
    nodes.push(box(producerX, y, clientWidth, 'Producer ' + i, read('rate') + ' msg/s', '#5eead4',
      nodeAttributes('producer', i, 0, 'Inspect producer ' + i, 'selectNode(&quot;producer&quot;,' + i + ',0)', owner)));
    P.forEach((partition, index) => {
      if (partition.leader === null) return;
      const leader = locations[index][partition.leader];
      if (producers <= 5 || index % producers === i || selected?.kind === 'producer' && selected.i === i) {
        connect('produce', {owner, left: producerX, right: producerX + clientWidth, y: y + 22}, leader,
          producing, selected?.kind === 'partition' && selected.i === index || nodeIsSelected('producer', i, 0));
      }
    });
  }
  for (let group = 0; group < groups; group++) {
    for (let i = 0; i < consumers; i++) {
      const owner = 'consumer-' + i + '-' + group;
      const y = firstRow + (group * consumers + i) * nodeStride;
      const membership = consumerMembership.groups[group];
      const online = membership.members[i];
      const assigned = membership.owners.filter(owner => owner === i).length;
      const subtitle = !online ? 'Stopped' : membership.rebalanceAt !== null ? 'Rebalancing' : assigned + ' partitions';
      const label = 'Group ' + group + ' / Consumer ' + i;
      registerNode(owner, consumerX, y, clientWidth, 44);
      clients.set(owner, {owner, left: consumerX, right: consumerX + clientWidth, y: y + 22});
      nodes.push(box(consumerX, y, clientWidth, 'G' + group + ' / C' + i,
        subtitle, online ? '#7dd3fc' : '#fb7185',
        nodeAttributes('consumer', i, group, 'Inspect ' + label, 'selectNode(&quot;consumer&quot;,' + i + ',' + group + ')', owner)));
    }
  }
  P.forEach((partition, index) => {
    if (partition.leader === null) return;
    const leader = locations[index][partition.leader];
    const partitionSelected = selected?.kind === 'partition' && selected.i === index;
    for (const broker of partition.replicas) {
      if (broker === partition.leader || !B[broker].online) continue;
      const follower = locations[index][broker];
      connect('replica', leader, follower, true, partitionSelected);
    }
    for (let group = 0; group < groups; group++) {
      const owner = consumerMembership.owner(group, index);
      if (owner === null) continue;
      connect('consume', leader, clients.get('consumer-' + owner + '-' + group),
        consuming, partitionSelected || nodeIsSelected('consumer', owner, group));
    }
  });
  edges.sort((a, b) => Number(a.highlighted) - Number(b.highlighted));
  topologyViewport.classList.toggle('paused', !running);
  topologyViewport.innerHTML = '<svg viewBox="0 0 ' + topologyWidth + ' ' + topologyHeight + '" xmlns="http://www.w3.org/2000/svg"><defs>' +
    markers + '</defs>' + backgrounds.join('') + edges.map(edge => edge.html).join('') + nodes.join('') + '</svg>';
  topologyEdgesByNode = new Map();
  topologyViewport.querySelectorAll('.edge').forEach((element, index) => {
    const edge = edges[index];
    edge.element = element;
    for (const owner of [edge.from.owner, edge.to.owner]) {
      if (!topologyEdgesByNode.has(owner)) topologyEdgesByNode.set(owner, []);
      topologyEdgesByNode.get(owner).push(edge);
    }
  });
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
      partition.leo + '</span><span>HW: ' + partition.hw + '</span>' + transactionMetric(partition, '<span>LSO: ', '</span>') + '<span>ISR: ' + partition.isr.join(', ') +
      '</span><span>Start: ' + partition.start + '</span><span>Replica LEO: ' + partition.offsets[j] +
      '</span></div>LEO is the next write offset; HW is the replicated boundary; ISR is the in-sync replica set.' +
      (partition.transactionRanges?.length ? ' LSO is the read committed boundary.' : '');
  } else if (kind === 'consumer') {
    const group = consumerMembership.groups[j];
    const online = group.members[i];
    const own = P.filter((_, index) => consumerMembership.owner(j, index) === i);
    const state = !online ? 'Stopped' : group.rebalanceAt !== null ? 'Rebalancing' : 'Online';
    const countdown = group.rebalanceAt === null ? '' : ' · Group rebalance in ' + (group.rebalanceAt - t) + 's';
    detail.innerHTML = '<div class="consumer-detail-heading"><b>Group ' + j + ' / Consumer ' + i + '</b>' +
      '<button type="button" id="consumer-member-action" class="smallbtn" onclick="toggleConsumer(' + j + ',' + i + ')">' +
      (online ? 'Stop consumer' : 'Resume consumer') + '</button></div>' +
      '<span class="' + (online ? 'muted' : 'bad') + '">' + state + countdown + '</span><br>Assigned: ' +
      (own.map(partition => 'T' + partition.topic + '/P' + partition.id).join(', ') || 'none') +
      '<br>Assigned lag: ' + own.reduce((total, partition) => total + Math.max(0, partition.leo - (partition.comm[j] ?? 0)), 0);
    if (memberActionFocused) $('consumer-member-action').focus({preventScroll: true});
  } else {
    detail.innerHTML = '<b>Producer ' + i + '</b><br>acks=' + $('acks').value + ' · ' + read('rate') +
      ' records/s<br>Producers use metadata to locate the partition leader.';
  }
}
const dataViews=[['logs','Partition logs'],['groups','Consumer groups'],['replicas','Replication / ISR'],['internals','Concepts'],['protocol','Protocol flow']];
const dataView = $('data-view');
dataView.innerHTML = dataViews.map(([id, label]) => '<option value="' + id + '">' + label + '</option>').join('');
dataView.addEventListener('change', () => {
  tab = dataView.value;
  drawView();
});
function transactionMetric(partition, prefix, suffix = '') {
  return partition.transactionRanges?.length ? prefix + transactions.lastStableOffset(partition) + suffix : '';
}

function escapeLogText(value) {
  const entities = {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'};
  return value.replace(/[&<>"']/g, character => entities[character]);
}

function partitionLogBlock(partition, offset) {
  if (logStorage.isGap(partition, offset)) {
    return '<span class="block compacted-gap" title="Removed by log compaction">' + offset + ' · gap</span>';
  }
  const trace = partition.tracedRecords?.get(offset);
  const record = logStorage.recordAt(partition, offset);
  const visibility = offset >= partition.hw ? 'pending' : offset < partition.comm[0] ? 'consumed' : '';
  const transaction = transactions.markerAt(partition, offset);
  const transactionLabel = transaction ? ' · ' + transaction.id + ' ' + transaction.state : '';
  const summary = record ? record.key + '=' + (record.value === null ? 'tombstone' : record.value) : '';
  const preview = summary.length > 24 ? summary.slice(0, 24) + '…' : summary;
  return '<span class="block ' + visibility + (trace ? ' tracked-record' : '') +
    (record?.value === null ? ' tombstone-record' : '') +
    (transaction ? ' transaction-' + transaction.state : '') + '"' +
    (record ? ' title="' + escapeLogText(summary) + '"' : '') + '>' + offset +
    (trace ? ' · ' + trace.id : '') + transactionLabel +
    (record ? ' · ' + escapeLogText(preview) : '') + '</span>';
}
function consumerGroupTable() {
  return consumerMembership.groups.map(group => {
    const online = group.members.filter(Boolean).length;
    const rows = P.map((partition, index) => {
      const owner = consumerMembership.owner(group.id, index);
      return '<tr><td>T' + partition.topic + '/P' + partition.id + '</td><td>' +
        (owner === null ? '—' : 'C' + owner) + '</td><td>' +
        partition.pos[group.id] + '</td><td>' + partition.comm[group.id] + '</td><td>' +
        Math.max(0, partition.leo - partition.comm[group.id]) + '</td></tr>';
    }).join('');
    return '<h3>Group ' + group.id + ' · ' + consumerMembership.status(group) +
      ' · generation ' + group.generation + ' · ' + online + '/' + group.members.length + ' online</h3>' +
      '<table><tr><th>Partition</th><th>Owner</th><th>Position</th><th>Commit</th><th>Lag</th></tr>' + rows + '</table>';
  }).join('');
}
function drawView(){let v=$('view');if(tab==='logs')v.innerHTML=P.map(p=>'<div class="partition"><strong>Topic '+p.topic+' / Partition '+p.id+'</strong> <small>LEO '+p.leo+' · HW '+p.hw+transactionMetric(p, ' · LSO ')+' · Start '+p.start+'</small><div class="blocks">'+Array.from({length:Math.min(24,p.leo-p.start)},(_,i)=>{let o=Math.max(p.start,p.leo-24)+i;return partitionLogBlock(p, o)}).join('')+'</div><small>Green: below group 0 commit · blue: below HW · amber: at or above HW</small></div>').join('');if(tab==='groups')v.innerHTML=consumerGroupTable();if(tab==='replicas')v.innerHTML='<table><tr><th>Partition</th><th>Leader</th><th>ISR</th><th>Replica LEO</th><th>HW</th></tr>'+P.map(p=>'<tr><td>T'+p.topic+'/P'+p.id+'</td><td>'+ (p.leader===null?'NONE':'B'+p.leader)+'</td><td>'+p.isr.join(', ')+'</td><td>'+p.replicas.map(b=>'B'+b+': '+p.offsets[b]).join(' / ')+'</td><td>'+p.hw+transactionMetric(p, ' · LSO ')+'</td></tr>').join('')+'</table>';if(tab==='protocol')v.innerHTML='<div class="hint"><h3>Protocol flow Kafka</h3><div dir="ltr" class="detail">Metadata request · Producer → Controller metadata via Broker<br>Produce request · Producer → Partition Leader<br>Replica Fetch · Follower → Leader<br>Produce response · Leader → Producer (acks)<br>FindCoordinator · Consumer → Broker<br>JoinGroup / SyncGroup · Consumer → Coordinator<br>Fetch · Consumer → Leader (up to HW)<br>OffsetCommit · Consumer → __consumer_offsets</div><h3>ACK versus offset commit</h3><p>ACK confirms a write; offset commit checkpoints consumer progress. Reduce replication speed to observe pending ACKs.</p></div>';if(tab==='internals')v.innerHTML='<div class="hint">'+lessons.map(([h,b])=>'<h3>'+h+'</h3><p>'+b+'</p>').join('')+'</div>'}
const lessons=[['Log and offsets','Each partition is an ordered append-only log. Offsets are local to a partition. Consuming records does not delete them.'],['Leader, follower, ISR','Producers append to the leader. Followers fetch from the leader. ISR tracks replicas sufficiently caught up for safe election.'],['High watermark','Consumers normally read only records below the high watermark.'],['Consumer groups','Each partition has one owner per group. Position and committed offset differ; restart can replay records.'],['Acknowledgments','acks=all requires sufficient in-sync replicas. acks=0 does not wait for a response.'],['Retention','Kafka removes data independently of consumer progress; real retention uses segment age and size.'],['KRaft','Controllers replicate metadata and elect leaders. Losing controller quorum prevents metadata changes.'],['Model scope','Internal pipelines use simplified logical counters and demonstrate causal relationships, not physical Kafka timing.']];
const scenarios=[['Lag growing',()=>{$('rate').value=30;$('consume').value=2}],['Catch up',()=>{$('rate').value=0;$('consume').value=50}],['Slow replication',()=>{$('replicate').value=1;$('rate').value=30}],['Leader failure',()=>{let p=P.find(p=>p.leader!==null);if(p)toggle(p.leader)}],['Replay after restart',()=>{$('auto').checked=false;restart()}],['ACK blocked by ISR',()=>{$('acks').value='all';$('minisr').value=s.rf;let b=B.find(b=>b.online);if(b)toggle(b.id)}],['Uncommitted replay',()=>{$('auto').checked=false;restart()}],['More consumers',()=>{$('consumers').value=read('consumers')+2;rebalance()}]];
const experimentSelect = $('scenarios');
scenarios.forEach(([label], index) => experimentSelect.add(new Option(label, index)));
$('experiment-form').addEventListener('submit', event => {
  event.preventDefault();
  if (experimentSelect.value === '') return;
  scenarios[Number(experimentSelect.value)][1]();
  render();
});
$('config').addEventListener('change', () => apply());
$('flow-settings').addEventListener('change', () => render());
$('live').addEventListener('change', render);
apply();
setInterval(() => { if (running) step(); }, 1000);
