#!/bin/bash

set -e

PARAM_NUM_INSTANCES=${NUM_INSTANCES:-100}

PARAM_COORDINATOR=${COORDINATOR:-127.0.0.1}
PARAM_COORDINATOR_API_PORT=${COORDINATOR_API_PORT:-7203}
PARAM_COORDINATOR_DEBUG_PORT=${COORDINATOR_DEBUG_PORT:-7203}

PARAM_DBNODES=${DBNODES:-127.0.0.1}
PARAM_DBNODE_DEBUG_PORT=${DBNODE_DEBUG_PORT:-9004}

CFG_COORDINATOR="${PARAM_COORDINATOR}:${PARAM_COORDINATOR_API_PORT}"

CFG_SCRAPE_FAKEWEBSERVERS=""
for N in $(seq 1 $PARAM_NUM_INSTANCES); do
  PORT=$(expr 8080 + $N - 1)
  if [ "$CFG_SCRAPE_FAKEWEBSERVERS" != "" ]; then
    CFG_SCRAPE_FAKEWEBSERVERS="${CFG_SCRAPE_FAKEWEBSERVERS},"
  fi
  CFG_SCRAPE_FAKEWEBSERVERS="${CFG_SCRAPE_FAKEWEBSERVERS}\"fakewebserver01:${PORT}\""
done

CFG_SCRAPE_DBNODES=""
for DBNODE in $(echo $PARAM_DBNODES | tr "," "\n"); do
  if [ "$CFG_SCRAPE_DBNODES" != "" ]; then
    CFG_SCRAPE_DBNODES="${CFG_SCRAPE_DBNODES},"
  fi
  CFG_SCRAPE_DBNODES="${CFG_SCRAPE_DBNODES}\"${DBNODE}:${PARAM_DBNODE_DEBUG_PORT}\""
done

CFG_SCRAPE_COORDINATOR="\"${PARAM_COORDINATOR}:${PARAM_COORDINATOR_DEBUG_PORT}\""

sed -e "s/\${coordinator}/${CFG_COORDINATOR}/" \
  -e "s/\${fakewebserver_scrape_targets}/${CFG_SCRAPE_FAKEWEBSERVERS}/" \
  -e "s/\${dbnode_scrape_targets}/${CFG_SCRAPE_DBNODES}/" \
  -e "s/\${coordinator_scrape_target}/${CFG_SCRAPE_COORDINATOR}/" \
  prometheus-template.yml > /tmp/prometheus.yml

echo "Start loadgen containers"
docker-compose -f docker-compose.yml up
