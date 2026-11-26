const transactions = (() => {
  const byId = id => document.getElementById(id);
  const isolation = byId('transaction-isolation');
  const beginButton = byId('transaction-begin');
  const writeButton = byId('transaction-write');
  const commitButton = byId('transaction-commit');
  const abortButton = byId('transaction-abort');
  const replayButton = byId('transaction-replay');
  const status = byId('transaction-status');
  const hint = byId('transaction-hint');
  let current = null;
  let sequence = 0;
  let committed = 0;
  let aborted = 0;

  function emit(message, level = 'info') {
    note(message, 'Transaction', level);
  }

  function canWrite() {
    return current?.marker.state === 'open';
  }

  function isUnresolved(state) {
    return state === 'open' || state === 'committing' || state === 'aborting';
  }

  function begin() {
    if (current && isUnresolved(current.marker.state)) return;
    current = {
      marker: {id: 'TX' + ++sequence, state: 'open'},
      start: t, count: 0, partitions: new Set()
    };
    emit(current.marker.id + ' opened');
    render();
  }

  function markWrite(partition, start, end) {
    const ranges = partition.transactionRanges ??= [];
    const last = ranges.at(-1);
    if (last?.marker === current.marker && last.end === start) last.end = end;
    else ranges.push({start, end, marker: current.marker});
    current.partitions.add(partition);
    current.count += end - start;
    return current.marker.id;
  }

  function writeTestBatch() {
    if (!canWrite()) return;
    const partition = P[0];
    const count = read('batch');
    const rejectedReason = appendRejection(partition, 'all', read('minisr'));
    if (rejectedReason) {
      rejected += count;
      emit(current.marker.id + ' write rejected: ' + rejectedReason, 'error');
    } else {
      const start = partition.leo;
      for (let i = 0; i < count; i++) appendRecord(partition, 'all');
      const id = markWrite(partition, start, partition.leo);
      emit(id + ' appended at T0 / P0, offsets ' + start + '–' + (partition.leo - 1));
    }
    render();
  }

  function currentWrites() {
    if (!current) return [];
    return [...current.partitions].flatMap(partition =>
      partition.transactionRanges.filter(range => range.marker === current.marker)
        .map(range => ({partition, range}))
    );
  }

  function finishWhenReplicated() {
    if (!current) return;
    const state = current.marker.state;
    if (state !== 'committing' && state !== 'aborting') return;

    const writes = currentWrites();
    if (writes.some(({partition, range}) => partition.leader === null ||
      !B[partition.leader].online || partition.hw < range.end)) return;

    const outcome = state === 'committing' ? 'committed' : 'aborted';
    current.marker.state = outcome;
    if (outcome === 'committed') committed += current.count;
    else aborted += current.count;
    emit(current.marker.id + ' ' + outcome + ' · ' + current.count + ' records',
      outcome === 'aborted' ? 'warn' : 'info');
    for (const {partition, range} of writes) {
      recordTrace.transactionOutcome(partition, range.start, range.end, current.marker.id, outcome);
    }
  }

  function end(outcome, reason) {
    if (!canWrite()) return;
    current.marker.state = outcome;
    const action = outcome === 'committing' ? 'commit requested' : 'abort requested';
    emit(current.marker.id + ' ' + action + (reason ? ': ' + reason : ''), reason ? 'warn' : 'info');
    finishWhenReplicated();
  }

  function advance() {
    if (canWrite() && t - current.start >= internal.transactionTimeout) {
      end('aborting', 'transaction timeout reached');
    }
    finishWhenReplicated();
  }

  function prune(partition) {
    const ranges = partition.transactionRanges;
    if (!ranges) return;
    let lost = 0;
    for (const range of ranges) {
      if (range.marker === current?.marker && isUnresolved(range.marker.state)) {
        lost += Math.max(0, range.end - Math.max(range.start, partition.leo));
      }
      range.start = Math.max(range.start, partition.start);
      range.end = Math.min(range.end, partition.leo);
    }
    partition.transactionRanges = ranges.filter(range => range.start < range.end);
    if (lost) {
      current.count -= lost;
      if (current.marker.state !== 'aborting') {
        current.marker.state = 'aborting';
        emit(current.marker.id + ' abort requested after leader truncation', 'warn');
      }
    }
  }

  function lastStableOffset(partition) {
    const unresolved = partition.transactionRanges?.find(range => isUnresolved(range.marker.state));
    return unresolved ? Math.min(partition.hw, unresolved.start) : partition.hw;
  }

  function readLimit(partition) {
    return isolation.value === 'read_committed' ? lastStableOffset(partition) : partition.hw;
  }

  function readRecords(partition, position, budget, onRead) {
    const limit = readLimit(partition);
    let consumed = 0;
    if (!budget || position >= limit) return {position, consumed};

    function readUntil(end) {
      const count = Math.min(budget - consumed, end - position);
      if (count <= 0) return;
      onRead(position, position + count);
      position += count;
      consumed += count;
    }

    if (isolation.value === 'read_committed') {
      for (const range of partition.transactionRanges ?? []) {
        if (range.start >= limit) break;
        if (range.marker.state !== 'aborted' || range.end <= position) continue;
        readUntil(range.start);
        if (position < range.start) break;
        position = Math.min(limit, range.end);
      }
    }
    readUntil(limit);
    return {position, consumed};
  }

  function replay() {
    const firstOffsets = new Map();
    for (const {partition, range} of currentWrites()) {
      if (!firstOffsets.has(partition)) firstOffsets.set(partition, range.start);
    }
    for (const [partition, offset] of firstOffsets) {
      syncGroups(partition);
      partition.pos[0] = offset;
    }
    if (firstOffsets.size) emit(current.marker.id + ' replayed from its first offset for group G0');
    render();
  }

  function reset() {
    current = null;
    sequence = committed = aborted = 0;
    updateControls();
  }

  function updateControls() {
    const state = current?.marker.state;
    const writable = canWrite();
    beginButton.disabled = Boolean(state && isUnresolved(state));
    writeButton.disabled = commitButton.disabled = abortButton.disabled = !writable;
    byId('record-transaction').hidden = !writable;
    replayButton.hidden = !currentWrites().length;

    const mode = isolation.value;
    const message = state ? current.marker.id + ' · ' + state[0].toUpperCase() + state.slice(1) +
      ' · ' + current.count + ' records' : 'No transaction running';
    if (status.textContent !== message) status.textContent = message;
    const explanation = state === 'committing' || state === 'aborting'
      ? 'Waiting for the transaction writes to replicate. Step or run to advance.'
      : !state ? 'Begin a transaction, then write a test batch to T0 / P0.'
      : mode === 'read_committed'
        ? 'Reads stop at the last stable offset and skip aborted records.'
        : 'Reads up to the high watermark, including open and aborted records.';
    if (hint.textContent !== explanation) hint.textContent = explanation;
  }

  beginButton.addEventListener('click', begin);
  writeButton.addEventListener('click', writeTestBatch);
  commitButton.addEventListener('click', () => { end('committing'); render(); });
  abortButton.addEventListener('click', () => { end('aborting'); render(); });
  replayButton.addEventListener('click', replay);
  isolation.addEventListener('change', () => {
    emit('Consumer isolation set to ' + isolation.value);
    render();
  });

  return {
    canWrite, markWrite, advance, prune, readLimit, readRecords, lastStableOffset, reset, updateControls,
    get state() { return current?.marker.state.toUpperCase() ?? 'IDLE'; },
    get count() { return current?.count ?? 0; },
    get committed() { return committed; },
    get aborted() { return aborted; },
    get isolation() { return isolation.value; }
  };
})();
