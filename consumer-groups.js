const consumerMembership = (() => {
  const groups = [];

  function schedule(group) {
    const delay = read('rebalanceDelay');
    group.rebalanceAt = t + delay;
    log('Group G' + group.id + ' rebalance started · ' + delay + 's', 'Consumer group');
  }

  function advance() {
    for (const group of groups) {
      if (group.rebalanceAt === null || t < group.rebalanceAt) continue;

      const online = group.members.flatMap((active, id) => active ? [id] : []);
      let moved = 0;
      const owners = P.map((partition, index) => {
        const owner = online.length ? online[index % online.length] : null;
        const previous = group.owners[index] ?? null;
        if (owner !== previous) {
          moved++;
          if (owner !== null && group.generation > 0) {
            syncGroups(partition);
            partition.pos[group.id] = partition.comm[group.id];
          }
        }
        return owner;
      });
      group.owners = owners;
      group.generation++;
      group.rebalanceAt = null;
      log('Group G' + group.id + ' · generation ' + group.generation + ' · ' +
        (online.length ? moved + ' ownership changes · ' + online.length + ' online' : 'no online members'),
        'Consumer group');
    }
  }

  function sync(force = false) {
    const count = read('groups');
    const members = read('consumers');
    for (const group of groups.splice(count)) {
      for (const partition of P) {
        if (partition.comm[group.id] !== undefined) partition.pos[group.id] = partition.comm[group.id];
      }
    }
    for (let id = 0; id < count; id++) {
      const created = !groups[id];
      const group = groups[id] ?? {id, members: [], owners: [], generation: 0, rebalanceAt: null};
      const changed = group.members.length !== members;
      if (changed) group.members = Array.from({length: members}, (_, index) => group.members[index] ?? true);
      groups[id] = group;
      if (force || created || changed) schedule(group);
    }
    advance();
  }

  function toggle(groupId, id) {
    const group = groups[groupId];
    if (group?.members[id] === undefined) return;
    const online = !group.members[id];
    group.members[id] = online;
    log('G' + groupId + ' / C' + id + (online ? ' rejoined' : ' stopped'),
      'Consumer group', online ? 'info' : 'warn');
    schedule(group);
    advance();
  }

  function owner(groupId, index) {
    const group = groups[groupId];
    return group?.rebalanceAt === null ? group.owners[index] ?? null : null;
  }

  function status(group) {
    if (group.rebalanceAt !== null) return 'Rebalancing';
    return group.members.some(Boolean) ? 'Stable' : 'Empty';
  }

  function summary() {
    if (groups.some(group => group.rebalanceAt !== null)) return 'Rebalancing';
    const empty = groups.filter(group => status(group) === 'Empty').length;
    return empty === groups.length ? 'Empty' : empty ? empty + ' empty' : 'Stable';
  }

  function reset() {
    groups.length = 0;
    sync();
  }

  return {groups, advance, sync, toggle, owner, status, summary, reset};
})();
