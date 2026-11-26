# Kafka Visual Lab

Offline educational simulator. No Kafka, Docker, Node.js, installation, or internet required.

## Run
Open index.html directly in Chrome or Firefox.

Optional local server:
```bash
python3 -m http.server 8080
```
Then visit http://localhost:8080 (run from this directory).

## Source
- index.html: UI and styles
- core.js: cluster topology, partition replicas, offsets, consumer groups
- lab.js: internal pipelines and interactive controls

This is a simplified educational model, not an actual Kafka broker.
