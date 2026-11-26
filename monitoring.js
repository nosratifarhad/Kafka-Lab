const monitoring = (() => {
  const byId = id => document.getElementById(id);
  const groupSelect = byId('lag-group');
  const rangeSelect = byId('lag-range');
  const plot = byId('lag-plot');
  const chart = byId('chart');
  const crosshair = byId('lag-crosshair');
  const tooltip = byId('lag-tooltip');
  const latest = byId('lag-latest');
  const average = byId('lag-average');
  const peak = byId('lag-peak');
  const journalRows = byId('journal-events');
  const journalScroll = byId('log');
  const journalLevel = byId('journal-level');
  const journalSearch = byId('journal-search');
  const journalStatus = byId('journal-status');
  const journalPause = byId('journal-pause');
  const number = new Intl.NumberFormat(undefined, {maximumFractionDigits: 1});
  const compactNumber = new Intl.NumberFormat(undefined, {notation: 'compact'});
  const levels = {info: 'Info', warn: 'Warn', error: 'Error'};
  const historyLimit = 901;
  const eventLimit = 500;
  const breakdownBar = byId('lag-breakdown-bar');
  const breakdownTime = byId('lag-breakdown-time');
  const breakdownRows = [...byId('lag-breakdown-values').children].map(row => {
    const key = row.dataset.lagPart;
    const segment = document.createElement('span');
    segment.dataset.lagPart = key;
    breakdownBar.append(segment);
    return {key, row, segment, value: row.querySelector('dd'), optional: row.hidden};
  });

  let samples = [];
  let chartPoints = [];
  let selectedTime = null;
  let events = [];
  let frozenJournal = null;
  let eventSequence = 0;
  let journalRenderQueued = false;

  function simTime(seconds, showHours = false) {
    const parts = [Math.floor(seconds / 3600), Math.floor(seconds / 60) % 60, seconds % 60];
    return 'T+' + parts.slice(showHours || parts[0] ? 0 : 1)
      .map(part => String(part).padStart(2, '0')).join(':');
  }

  function updateGroupOptions(groupCount) {
    if (groupSelect.options.length === groupCount + 1) return;
    const selectedGroup = groupSelect.value;
    groupSelect.length = 1;
    for (let index = 0; index < groupCount; index++) {
      groupSelect.add(new Option('Group ' + index, index));
    }
    groupSelect.value = selectedGroup === 'all' || Number(selectedGroup) < groupCount ? selectedGroup : 'all';
  }

  function addSample(time, groups) {
    const total = Object.fromEntries(breakdownRows.map(({key}) =>
      [key, groups.reduce((sum, breakdown) => sum + breakdown[key], 0)]));
    const sample = {time, groups, total};
    if (samples.at(-1)?.time === time) {
      samples[samples.length - 1] = sample;
    } else {
      samples.push(sample);
      if (samples.length > historyLimit) samples.shift();
    }
    updateGroupOptions(groups.length);
    renderChart();
    return totalLag(total);
  }

  function totalLag(breakdown) {
    return Object.values(breakdown).reduce((sum, amount) => sum + amount, 0);
  }

  function axisStep(maximum) {
    const target = Math.max(1, maximum / 4);
    const power = 10 ** Math.floor(Math.log10(target));
    return [1, 2, 5, 10].find(unit => unit * power >= target) * power;
  }

  function renderChart() {
    if (!samples.length) return;
    const end = samples.at(-1).time;
    const start = Math.max(0, end - Number(rangeSelect.value));
    const domainEnd = Math.max(start + 10, end);
    const group = groupSelect.value;
    const values = [];
    for (const sample of samples) {
      if (sample.time < start) continue;
      const breakdown = group === 'all' ? sample.total : sample.groups[Number(group)];
      if (breakdown) values.push({time: sample.time, lag: totalLag(breakdown), breakdown});
    }

    const maximum = Math.max(0, ...values.map(sample => sample.lag));
    const mean = values.reduce((sum, sample) => sum + sample.lag, 0) / Math.max(1, values.length);
    latest.textContent = number.format(values.at(-1)?.lag ?? 0);
    average.textContent = number.format(mean);
    peak.textContent = number.format(maximum);

    const width = Math.max(1, plot.clientWidth);
    const height = plot.clientHeight;
    const left = 42;
    const right = Math.max(left + 1, width - 32);
    const top = 12;
    const bottom = Math.max(top + 1, height - 27);
    const step = axisStep(maximum);
    const ceiling = Math.max(step * 2, Math.ceil(maximum / step) * step);
    const x = time => left + (time - start) / (domainEnd - start) * (right - left);
    const y = lag => bottom - lag / ceiling * (bottom - top);

    let grid = '';
    for (let value = 0; value <= ceiling; value += step) {
      const py = y(value);
      grid += '<path d="M' + left + ' ' + py + 'H' + right + '"/>' +
        '<text x="' + (left - 7) + '" y="' + (py + 4) + '" text-anchor="end">' +
        compactNumber.format(value) + '</text>';
    }
    const intervals = width < 350 ? 2 : width < 530 ? 3 : 5;
    for (let index = 0; index <= intervals; index++) {
      const time = start + Math.round((domainEnd - start) * index / intervals);
      const px = x(time);
      grid += '<path d="M' + px + ' ' + top + 'V' + bottom + '"/>' +
        '<text x="' + px + '" y="' + (bottom + 18) + '" text-anchor="middle">' + simTime(time) + '</text>';
    }

    chartPoints = values.map(sample => ({...sample, x: x(sample.time), y: y(sample.lag)}));
    let series = '';
    let segment = [];
    for (const point of chartPoints) {
      if (segment.length && point.time > segment.at(-1).time + 1) {
        series += drawLagSegment(segment, bottom);
        segment = [];
      }
      segment.push(point);
    }
    series += drawLagSegment(segment, bottom);

    const viewBox = '0 0 ' + width + ' ' + height;
    chart.setAttribute('viewBox', viewBox);
    crosshair.setAttribute('viewBox', viewBox);
    crosshair.querySelector('line').setAttribute('y1', top);
    crosshair.querySelector('line').setAttribute('y2', bottom);
    chart.innerHTML = '<defs><linearGradient id="lag-fill" x2="0" y2="1" gradientUnits="objectBoundingBox">' +
      '<stop stop-color="#54cdb9" stop-opacity=".30"/><stop offset="1" stop-color="#54cdb9" stop-opacity=".02"/>' +
      '</linearGradient></defs><g class="lag-grid">' + grid + '</g>' + series;
    inspectPoint(chartPoints.find(point => point.time === selectedTime));
  }

  function drawLagSegment(segment, bottom) {
    if (!segment.length) return '';
    const first = segment[0];
    if (segment.length === 1) {
      return '<circle class="lag-dot" cx="' + first.x + '" cy="' + first.y + '" r="3"/>';
    }
    const last = segment.at(-1);
    const path = 'M' + first.x + ' ' + first.y + segment.slice(1)
      .map(point => 'H' + point.x + 'V' + point.y).join('');
    return '<path class="lag-area" d="' + path + 'V' + bottom + 'H' + first.x + 'Z"/>' +
      '<path class="lag-line" d="' + path + '"/>';
  }

  function inspectPoint(point) {
    selectedTime = point?.time ?? null;
    tooltip.hidden = !point;
    crosshair.classList.toggle('active', Boolean(point));
    renderBreakdown(point);
    if (!point) return;
    const line = crosshair.querySelector('line');
    line.setAttribute('x1', point.x);
    line.setAttribute('x2', point.x);
    const dot = crosshair.querySelector('circle');
    dot.setAttribute('cx', point.x);
    dot.setAttribute('cy', point.y);
    tooltip.textContent = simTime(point.time, true) + ' · ' + number.format(point.lag) + ' offsets';
  }

  function renderBreakdown(point) {
    const breakdown = (point ?? chartPoints.at(-1))?.breakdown;
    breakdownTime.textContent = point ? simTime(point.time, true) : 'Latest';
    for (const {key, row, segment, value, optional} of breakdownRows) {
      const amount = breakdown?.[key] ?? 0;
      value.textContent = number.format(amount);
      row.hidden = optional && amount === 0;
      segment.hidden = amount === 0;
      segment.style.flexGrow = amount;
    }
  }

  function updateJournalStatus() {
    const count = journalRows.querySelectorAll('[data-event-id]').length;
    const total = frozenJournal?.events.length ?? events.length;
    const pending = frozenJournal ? eventSequence - frozenJournal.sequence : 0;
    journalStatus.textContent = (frozenJournal ? 'Paused' + (pending ? ' · ' + number.format(pending) + ' new' : '') : 'Latest events') +
      ' · ' + number.format(count) + ' of ' + number.format(total);
    journalPause.textContent = frozenJournal ? 'Resume feed' : 'Pause feed';
  }

  function renderJournal() {
    const query = journalSearch.value.trim().toLowerCase();
    const level = journalLevel.value;
    const source = frozenJournal?.events ?? events;
    const top = journalScroll.getBoundingClientRect().top;
    const anchor = journalScroll.scrollTop > 0
      ? [...journalRows.children].find(row => row.getBoundingClientRect().bottom > top)
      : null;
    const anchorId = anchor?.dataset.eventId;
    const anchorTop = anchor?.getBoundingClientRect().top;
    const results = source.filter(event =>
      (!level || event.level === level) &&
      (!query || (event.source + ' ' + event.message).toLowerCase().includes(query))
    );
    const rows = document.createDocumentFragment();
    for (const event of results) {
      const row = document.createElement('tr');
      row.dataset.eventId = event.id;
      const time = row.insertCell();
      time.className = 'journal-time';
      time.textContent = simTime(event.time, true);
      const severity = row.insertCell();
      const badge = document.createElement('span');
      badge.className = 'journal-level ' + event.level;
      badge.textContent = levels[event.level];
      severity.append(badge);
      const detail = row.insertCell();
      const label = document.createElement('span');
      label.className = 'journal-source';
      label.textContent = event.source;
      detail.append(label, event.message);
      rows.append(row);
    }
    if (!results.length) {
      const row = document.createElement('tr');
      const cell = row.insertCell();
      cell.colSpan = 3;
      cell.className = 'journal-empty';
      cell.textContent = source.length ? 'No events match these filters.' : 'No events recorded yet.';
      rows.append(row);
    }

    journalRows.replaceChildren(rows);
    if (anchorId) {
      const replacement = journalRows.querySelector('[data-event-id="' + anchorId + '"]');
      if (replacement) journalScroll.scrollTop += replacement.getBoundingClientRect().top - anchorTop;
    }
    updateJournalStatus();
  }

  function record(time, source, level, message) {
    events.unshift({id: ++eventSequence, time, source, level, message});
    if (events.length > eventLimit) events.pop();
    if (journalRenderQueued) return;
    journalRenderQueued = true;
    queueMicrotask(() => {
      journalRenderQueued = false;
      if (frozenJournal) updateJournalStatus();
      else renderJournal();
    });
  }

  function reset() {
    samples = [];
    events = [];
    eventSequence = 0;
    frozenJournal = null;
    selectedTime = null;
    journalScroll.scrollTop = 0;
    renderJournal();
  }

  function resetJournalScroll() {
    journalScroll.scrollTop = 0;
    renderJournal();
  }

  journalPause.addEventListener('click', () => {
    if (frozenJournal) {
      frozenJournal = null;
    } else {
      frozenJournal = {events: events.slice(), sequence: eventSequence};
    }
    resetJournalScroll();
  });
  journalSearch.addEventListener('input', resetJournalScroll);
  journalLevel.addEventListener('change', resetJournalScroll);
  rangeSelect.addEventListener('change', renderChart);
  groupSelect.addEventListener('change', renderChart);
  function inspectPointer(event) {
    const position = event.clientX - plot.getBoundingClientRect().left;
    if (!chartPoints.length) return;
    const point = chartPoints.reduce((closest, candidate) =>
      Math.abs(candidate.x - position) < Math.abs(closest.x - position) ? candidate : closest);
    inspectPoint(point);
  }
  plot.addEventListener('pointermove', inspectPointer);
  plot.addEventListener('pointerdown', inspectPointer);
  plot.addEventListener('pointerleave', event => {
    if (event.pointerType !== 'touch') inspectPoint();
  });
  plot.addEventListener('blur', () => inspectPoint());
  plot.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
      inspectPoint();
      return;
    }
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key) || !chartPoints.length) return;
    event.preventDefault();
    const current = chartPoints.findIndex(point => point.time === selectedTime);
    const last = chartPoints.length - 1;
    const next = {
      ArrowLeft: current < 0 ? last : Math.max(0, current - 1),
      ArrowRight: Math.min(last, current + 1),
      Home: 0,
      End: last
    }[event.key];
    inspectPoint(chartPoints[next]);
  });
  new ResizeObserver(renderChart).observe(plot);

  return {addSample, record, reset};
})();
