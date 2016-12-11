#!/usr/bin/env python

from __future__ import print_function

import os
import time
import json
import argparse
import elasticsearch

from utils import log
from utils import log_es
from utils import log_levels
from utils import positive_int


parser = argparse.ArgumentParser(
    'elasticrestore',
    description='Restore data and mappings to an ElasticSearch index')

parser.add_argument('host',
                    help='elasticsearch host')

parser.add_argument('index',
                    help='elasticsearch index name')

parser.add_argument('-d', '--backup-dir',
                    help='backup path')

parser.add_argument('-b', '--batch-size',
                    help='document upload batch size',
                    type=positive_int,
                    default=1000)

parser.add_argument('-v', '--verbose',
                    help='increase output verbosity',
                    action='count',
                    default=0)

parser.add_argument('-u', '--user',
                    help='HTTP auth (in format user:pass)')


def create_index(es, index, f):
    mappings = json.load(f)
    es.indices.create(index=index, body=mappings)


def create_documents(es, index, f, pipelines=None, batch_size=1000):
    total = 0

    for size, batch, doc_type in document_batches(f, batch_size):
        pipeline = doc_type + "-pipeline"
        try:
            pipelines[pipeline]
        except KeyError:
            pipeline = None

        response = es.bulk(index=index, body=batch, pipeline=pipeline)
        if response["errors"]:
            print(response)
            exit(-1)
        total += size
        log.info("uploaded %s (total: %s)", size, total)


def document_batches(fp, batch_size):
    i = 0
    batch = []
    doc_type = None

    for line in fp:
        obj = json.loads(line)

        if not doc_type:
            doc_type = obj["_type"]

        if doc_type != obj["_type"]:
            yield i, batch, doc_type
            i = 0
            batch = []
            doc_type = None

        src = obj.pop('_source')
        batch.append(json.dumps({"create": obj}))
        batch.append(json.dumps(src))
        i += 1

        if i >= batch_size:
            yield i, batch, doc_type
            i = 0
            batch = []

    if batch:
        yield i, batch, doc_type


def main():
    args = parser.parse_args()

    verbose = min(args.verbose, 2)
    log.setLevel(log_levels[verbose])
    log_es.setLevel(log_levels[verbose])

    backup_dir = args.backup_dir
    if not os.path.exists(backup_dir):
        return log.warn("Backup path %s does not exists" % backup_dir)

    mappings_path = os.path.join(backup_dir, "mappings.json")
    if not os.path.exists(mappings_path):
        return log.warn("mappings path %s does not exists" % mappings_path)

    settings_path = os.path.join(backup_dir, "settings.json")
    if not os.path.exists(settings_path):
        return log.warn("settings path %s does not exists" % settings_path)

    documents_path = os.path.join(backup_dir, "documents.json")
    if not os.path.exists(documents_path):
        return log.warn("documents path %s does not exists" % documents_path)

    ingest_path = os.path.join(backup_dir, "ingest.json")
    ingest_pipelines = {}
    if not os.path.exists(ingest_path):
        log.debug("No ingest file has been found")
    else:
        with open(ingest_path) as f:
            ingest_pipelines = json.load(f)

    conn_kwargs = {}
    if args.user:
        conn_kwargs['http_auth'] = args.user

    conn_kwargs["timeout"] = 300
    conn_kwargs["retry_on_timeout"] = True

    es = elasticsearch.Elasticsearch([args.host], **conn_kwargs)
    if es.indices.exists(index=args.index):
        return log.warn(
            "Index %s already exists. Execute for delete: \n"
            "curl -XDELETE %s:9200/%s" % (args.index, args.host, args.index))

    es.indices.create(index=args.index)
    time.sleep(1)
    es.indices.close(index=args.index)
    time.sleep(1)

    with open(settings_path) as f:
        settings = json.load(f)
        es.indices.put_settings(index=args.index, body=settings)

    es.indices.open(index=args.index)
    time.sleep(1)

    with open(mappings_path) as f:
        mappings = json.load(f)
        for doc_type, doc_mapping in mappings["mappings"].items():
            es.indices.put_mapping(doc_type, index=args.index,
                                   body=doc_mapping)
    time.sleep(1)

    for pipeline_id, pipeline_body in ingest_pipelines.items():
        try:
            exists = es.ingest.get_pipeline(pipeline_id)
            print(exists)
        except elasticsearch.exceptions.NotFoundError:
            es.ingest.put_pipeline(pipeline_id, pipeline_body)

    with open(documents_path) as f:
        create_documents(es, args.index, f,
                         pipelines=ingest_pipelines,
                         batch_size=args.batch_size)


if __name__ == '__main__':
    main()
