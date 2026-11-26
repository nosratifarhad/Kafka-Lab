const logStorage = (() => {
  function remember(partition, offset, key, value) {
    if (!key) return;
    partition.keyedRecords ??= new Map();
    partition.keyedRecords.set(offset, {key, value, time: t});
  }

  function gaps(partition) {
    return partition.compactedRanges ?? [];
  }

  function recordAt(partition, offset) {
    return partition.keyedRecords?.get(offset);
  }

  function isGap(partition, offset) {
    return gaps(partition).some(range => range.start <= offset && offset < range.end);
  }

  function mergeRanges(ranges, offsets) {
    const ordered = [...ranges, ...offsets.map(offset => ({start: offset, end: offset + 1}))]
      .sort((a, b) => a.start - b.start);
    const merged = [];
    for (const range of ordered) {
      const last = merged.at(-1);
      if (last && range.start <= last.end) last.end = Math.max(last.end, range.end);
      else merged.push({...range});
    }
    return merged;
  }

  function compact(partition, tombstoneGrace) {
    const keyed = partition.keyedRecords;
    if (!keyed?.size) return {removed: 0, tombstones: 0};

    const stableEnd = transactions.lastStableOffset(partition);
    const latest = new Map();
    const eligible = [];
    for (const [offset, record] of keyed) {
      if (offset >= stableEnd || transactions.markerAt(partition, offset)?.state === 'aborted') continue;
      eligible.push([offset, record]);
      latest.set(record.key, offset);
    }

    const removed = [];
    let tombstones = 0;
    for (const [offset, record] of eligible) {
      const superseded = offset !== latest.get(record.key);
      const expired = record.value === null && t - record.time >= tombstoneGrace;
      if (!superseded && !expired) continue;
      keyed.delete(offset);
      removed.push(offset);
      if (record.value === null) tombstones++;
      recordTrace.compacted(partition, offset, superseded ? 'Superseded by a later record for the key' : 'Tombstone grace period elapsed');
    }
    if (removed.length) partition.compactedRanges = mergeRanges(gaps(partition), removed);
    return {removed: removed.length, tombstones};
  }

  function recordCount(partition, start = partition.start, end = partition.leo) {
    const removed = gaps(partition).reduce((total, range) =>
      total + Math.max(0, Math.min(end, range.end) - Math.max(start, range.start)), 0);
    return Math.max(0, end - start - removed);
  }

  function retentionStart(partition, keep) {
    let boundary = partition.hw;
    let remaining = keep;
    const ranges = gaps(partition);
    for (let index = ranges.length - 1; index >= 0; index--) {
      const range = ranges[index];
      if (range.start >= boundary) continue;
      const live = Math.max(0, boundary - range.end);
      if (remaining <= live) break;
      remaining -= live;
      boundary = range.start;
    }
    return Math.max(partition.start, Math.min(boundary - remaining, transactions.lastStableOffset(partition)));
  }

  function prune(partition) {
    if (partition.keyedRecords) {
      for (const offset of partition.keyedRecords.keys()) {
        if (offset < partition.start || offset >= partition.leo) partition.keyedRecords.delete(offset);
      }
    }
    if (partition.compactedRanges) {
      partition.compactedRanges = gaps(partition)
        .map(range => ({start: Math.max(partition.start, range.start), end: Math.min(partition.leo, range.end)}))
        .filter(range => range.start < range.end);
    }
  }

  return {remember, gaps, recordAt, isGap, compact, recordCount, retentionStart, prune};
})();
