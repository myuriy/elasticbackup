#!/usr/bin/env python

from __future__ import print_function

import os
import argparse
import datetime
import json
import time

import elasticsearch

from utils import log
from utils import log_es
from utils import log_levels
from utils import positive_int
from utils import nonnegative_float


today = datetime.datetime.utcnow().strftime("%Y%m%d")

parser = argparse.ArgumentParser(
    'elasticbackup',
    description='Back up settings, mappings and data from an ElasticSearch '
                'index')
parser.add_argument('host',
                    help='elasticsearch host')

parser.add_argument('index',
                    help='elasticsearch index name')

parser.add_argument('-d', '--backup-dir',
                    help='backup parent directory',
                    default="./")

parser.add_argument('-b', '--batch-size',
                    help='document download batch size',
                    type=positive_int,
                    default=1000)

parser.add_argument('-q', '--query',
                    help='query to pass to elasticsearch')

parser.add_argument('--sleep-time',
                    help='sleep time between scrolls in seconds',
                    type=nonnegative_float,
                    default=1.0)

parser.add_argument('--scroll-time',
                    help='scroll time in seconds',
                    type=positive_int,
                    default=600)

parser.add_argument('-u', '--user',
                    help='HTTP auth (in format user:pass)')

parser.add_argument('-v', '--verbose',
                    help='increase output verbosity',
                    action='count',
                    default=0)


def write_mappings(es, index, f):
    log.info("Write mappings")
    mapping = es.indices.get_mapping(index)
    json.dump(mapping[index], f)
    log.info("Write mappings complete")


def write_settings(es, index, f):
    log.info("Write settings")
    settings = es.indices.get_settings(index)[index]["settings"]["index"]

    del settings["uuid"]
    del settings["version"]
    del settings["creation_date"]
    del settings["number_of_shards"]
    del settings["number_of_replicas"]

    json.dump({"settings": settings}, f)
    log.info("Write settings complete")


def write_documents(es, index, f, sleep, scroll, batch_size=1000, query=None):
    def _write_hits(r):
        hits = r['hits']['hits']
        if hits:
            for hit in hits:
                hit.pop('_index', None)
                hit.pop('_score', None)
                f.write("%s\n" % json.dumps(hit))
            return r['_scroll_id'], len(hits)
        return None, 0

    if query is None:
        query = {"query": {"match_all": {}}}

    status = "got batch of %s (total: %s)"

    results = es.search(index=index, body=query, scroll=scroll,
                        size=batch_size)
    scroll_id, num = _write_hits(results)
    total = num
    log.info(status, num, total)

    while scroll_id is not None:
        time.sleep(sleep)
        results = es.scroll(scroll_id=scroll_id, scroll=scroll)
        scroll_id, num = _write_hits(results)
        total += num
        log.info(status, num, total)


def main():
    args = parser.parse_args()
    verbose = min(args.verbose, 2)
    log.setLevel(log_levels[verbose])
    log_es.setLevel(log_levels[verbose])

    backup_dir = os.path.join(args.backup_dir,
                              "%s-%s" % (args.index, today))

    mappings_path = os.path.join(backup_dir, "mappings.json")
    settings_path = os.path.join(backup_dir, "settings.json")
    documents_path = os.path.join(backup_dir, "documents.json")

    conn_kwargs = {}
    if args.user:
        conn_kwargs['http_auth'] = args.user
    es = elasticsearch.Elasticsearch([args.host], **conn_kwargs)

    if os.path.exists(backup_dir):
        return log.warn("Dir %s already exists. Resolve it's, please" %
                        backup_dir)

    os.mkdir(backup_dir)

    with open(settings_path, 'w+') as f:
        write_settings(es, args.index, f)

    with open(mappings_path, 'w+') as f:
        write_mappings(es, args.index, f)

    with open(documents_path, 'w+') as f:
        write_documents(es,
                        args.index,
                        f,
                        sleep=args.sleep_time,
                        scroll="%ds" % args.scroll_time,
                        batch_size=args.batch_size,
                        query=args.query)

    log.info("Backup can be found at %s" % backup_dir)

if __name__ == '__main__':
    main()
