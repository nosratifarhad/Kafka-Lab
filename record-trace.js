const recordTrace = (() => {
  const form = document.getElementById('record-form');
  const topicSelect = document.getElementById('record-topic');
  const history = document.getElementById('record-history');
  const result = document.getElementById('record-result');
  const location = document.getElementById('record-location');
  const timeline = document.getElementById('record-events');
  const route = document.getElementById('record-route');
  const transactionButton = document.getElementById('record-transaction');
  const keyInput = document.getElementById('record-key');
  const valueInput = document.getElementById('record-value');
  const tombstone = document.getElementById('record-tombstone');
  const traces = new Map();
  const nextPartition = new Map();
  let sequence = 0;
  let renderQueued = false;

  function journalTag(trace) {
    return '[' + trace.id + ']';
  }

  function addEvent(trace, message, source, level) {
    trace.events.push({time: t, message});
    if (trace.events.length > 40) trace.events.splice(1, 1);
    log(journalTag(trace) + ' ' + message, source, level);
    if (trace.id !== history.value || renderQueued) return;
    renderQueued = true;
    queueMicrotask(() => {
      renderQueued = false;
      renderSelection();
    });
  }

  function partitionLabel(partition) {
    return 'T' + partition.topic + ' / P' + partition.id;
  }

  function renderSelection() {
    const trace = traces.get(history.value);
    result.hidden = !trace;
    if (!trace) return;

    const placement = partitionLabel(trace.partition) + (trace.offset === null ? '' : ' · offset ' + trace.offset);
    const status = placement + ' · ' + trace.status;
    if (location.textContent !== status) location.textContent = status;
    document.getElementById('record-sent-key').textContent = trace.key || '—';
    document.getElementById('record-sent-value').textContent = trace.value === null ? 'Tombstone (null)' : trace.value;
    route.disabled = trace.offset === null;

    const entries = trace.events.map(({time, message}) => {
      const item = document.createElement('li');
      const timestamp = document.createElement('span');
      timestamp.className = 'record-time';
      timestamp.textContent = 'T+' + time + 's';
      item.append(timestamp, message);
      return item;
    });
    timeline.replaceChildren(...entries);
  }

  function reset(topicCount) {
    traces.clear();
    nextPartition.clear();
    sequence = 0;
    history.length = 0;
    const previous = topicSelect.value;
    topicSelect.length = 0;
    for (let topic = 0; topic < topicCount; topic++) {
      topicSelect.add(new Option('Topic ' + topic, topic));
    }
    if (Number(previous) < topicCount && previous !== '') topicSelect.value = previous;
    renderSelection();
  }

  function remember(trace) {
    if (traces.size === 50) {
      const oldest = traces.values().next().value;
      if (oldest.partition.tracedRecords?.get(oldest.offset) === oldest) {
        oldest.partition.tracedRecords.delete(oldest.offset);
      }
      traces.delete(oldest.id);
      history.remove(history.options.length - 1);
    }
    traces.set(trace.id, trace);
    history.add(new Option(trace.id + ' · ' + partitionLabel(trace.partition), trace.id), 0);
    history.value = trace.id;
  }

  function send(topic, key, value, transactional) {
    if (transactional && !transactions.canWrite()) return;
    const partitions = P.filter(partition => partition.topic === topic);
    if (!partitions.length) return;
    const index = key === '' ? (nextPartition.get(topic) ?? 0) : hashKey(key) % partitions.length;
    const partition = partitions[index];
    if (key === '') nextPartition.set(topic, (index + 1) % partitions.length);

    const acks = transactional ? 'all' : $('acks').value;
    const trace = {
      id: 'R' + ++sequence, partition, key, value, offset: null,
      status: 'Submitting', events: []
    };
    remember(trace);
    addEvent(trace, 'Routed to ' + partitionLabel(partition) + ' with acks=' + acks, 'Producer');

    const reason = appendRejection(partition, acks, read('minisr'));
    if (reason) {
      rejected++;
      trace.status = 'Rejected';
      addEvent(trace, 'Write rejected: ' + reason, 'Producer', 'error');
    } else {
      trace.offset = appendRecord(partition, acks);
      logStorage.remember(partition, trace.offset, key, value);
      partition.tracedRecords ??= new Map();
      partition.tracedRecords.set(trace.offset, trace);
      const transactionId = transactional ? transactions.markWrite(partition, trace.offset, trace.offset + 1) : '';
      addEvent(trace, 'Appended on B' + partition.leader + ' at offset ' + trace.offset +
        (transactionId ? ' in ' + transactionId : '') + (value === null ? ' · tombstone' : ''), 'Broker');
      if (acks === 'all') {
        trace.status = 'Waiting for ACK';
      } else if (acks === '1') {
        acknowledge(trace, true, 'Leader acknowledged the append');
      } else {
        trace.status = 'No ACK requested';
        addEvent(trace, 'Producer requested no ACK', 'Producer');
      }
    }
    render();
    renderSelection();
  }

  function acknowledge(trace, accepted, message) {
    trace.status = accepted ? 'Acknowledged' : 'ACK failed';
    addEvent(trace, message, 'Producer', accepted ? 'info' : 'error');
  }

  function replicaAck(partition, nextOffset, accepted) {
    const trace = partition.tracedRecords?.get(nextOffset - 1);
    if (!trace) return;
    acknowledge(trace, accepted, accepted ? 'Replication ACK received' : 'Not enough in-sync replicas after append');
  }

  function consume(partition, group, consumer, start, end) {
    if (!partition.tracedRecords) return;
    for (const [offset, trace] of partition.tracedRecords) {
      if (start <= offset && offset < end) {
        addEvent(trace, 'Read by group G' + group + ' / consumer C' + consumer, 'Consumer group');
      }
    }
  }

  function transactionOutcome(partition, start, end, id, outcome) {
    if (!partition.tracedRecords) return;
    for (const [offset, trace] of partition.tracedRecords) {
      if (start <= offset && offset < end) {
        addEvent(trace, id + ' ' + outcome, 'Transaction', outcome === 'aborted' ? 'warn' : 'info');
      }
    }
  }

  function prune(partition) {
    if (!partition.tracedRecords) return;
    for (const [offset, trace] of partition.tracedRecords) {
      if (offset >= partition.start && offset < partition.leo) continue;
      partition.tracedRecords.delete(offset);
      const expired = offset < partition.start;
      trace.status = expired ? 'Expired by retention' : 'Truncated after leader election';
      addEvent(trace, trace.status, expired ? 'Storage' : 'Controller', 'warn');
    }
  }

  function compacted(partition, offset, reason) {
    const trace = partition.tracedRecords?.get(offset);
    if (!trace) return;
    partition.tracedRecords.delete(offset);
    trace.status = 'Removed by compaction';
    addEvent(trace, trace.status + ': ' + reason, 'Storage');
  }

  tombstone.addEventListener('change', () => {
    keyInput.required = tombstone.checked;
    keyInput.placeholder = tombstone.checked ? 'Required' : 'Optional';
    valueInput.disabled = tombstone.checked;
  });
  form.addEventListener('submit', event => {
    event.preventDefault();
    const key = keyInput.value.slice(0, 128);
    const value = tombstone.checked ? null : valueInput.value.slice(0, 512);
    if (value === null ? key !== '' : value !== '') {
      send(Number(topicSelect.value), key, value, event.submitter === transactionButton);
    }
  });
  history.addEventListener('change', renderSelection);
  route.addEventListener('click', () => {
    const trace = traces.get(history.value);
    if (!trace || trace.offset === null) return;
    const index = P.indexOf(trace.partition);
    const broker = trace.partition.leader ?? trace.partition.replicas[0];
    selectNode('partition', index, broker);
    const node = topologyViewport.querySelector('[data-topology-node="partition-' + index + '-' + broker + '"]');
    node?.scrollIntoView({block: 'center', inline: 'nearest'});
    node?.focus({preventScroll: true});
  });
  document.getElementById('record-journal').addEventListener('click', () => {
    const trace = traces.get(history.value);
    if (!trace) return;
    $('journal-level').value = '';
    $('journal-search').value = journalTag(trace);
    $('journal-search').dispatchEvent(new Event('input'));
    $('journal-search').scrollIntoView({block: 'center'});
    $('journal-search').focus({preventScroll: true});
  });

  return {reset, replicaAck, consume, transactionOutcome, compacted, prune};
})();
