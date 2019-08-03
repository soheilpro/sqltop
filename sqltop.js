#!/usr/bin/env node

const _ = require("lodash");
const moment = require('moment');
const numeral = require('numeral');
const colors = require('colors');
const Elasticsearch = require('./elasticsearch');
const { padLeft, truncate, makeProgressBar } = require('./string');

const fields = {
  'count': 'Count',
  'duration': 'Duration',
  'cpu': 'CPU',
  'reads': 'Reads',
  'writes': 'Writes',
  'query': 'QueryHash',
  'textdata': 'TextDataHash',
  'db': 'DatabaseName',
  'login': 'LoginName',
  'server': 'ServerName',
  'host': 'HostName',
};

const yargs = require('yargs')
  .option('address', { type: 'string', demandOption: true, describe: 'Elasticsearch address.'})
  .option('port', { type: 'number', default: 9200, describe: 'Elasticsearch port.'})
  .option('index-prefix', { type: 'string', default: 'sql-', describe: 'Elasticsearch index prefix.'})
  .option('metric', { type: 'string', demandOption: true, choices: ['Count', 'Duration', 'CPU', 'Reads', 'Writes'], coerce: value => fields[value], describe: 'Metric to calculate.'})
  .option('agg', { type: 'string', default: 'query', choices: ['QueryHash', 'TextDataHash', 'DatabaseName', 'LoginName', 'ServerName', 'HostName'], coerce: value => fields[value], describe: 'Aggregate 1.'})
  .option('agg2', { type: 'string', choices: ['QueryHash', 'TextDataHash', 'DatabaseName', 'LoginName', 'ServerName', 'HostName'], coerce: value => fields[value], describe: 'Aggregate 2.'})
  .option('db', { type: 'string', describe: 'DatabaseName filter.'})
  .option('login', { type: 'string', describe: 'LoginName filter.'})
  .option('server', { type: 'string', describe: 'ServerName filter.'})
  .option('host', { type: 'string', describe: 'HostName filter.'})
  .option('start', { type: 'string', default: moment().startOf('day').toDate(), coerce: value => moment(value).toDate(), describe: 'Start time.'})
  .option('end', { type: 'string', default: new Date(), coerce: value => moment(value).toDate(), describe: 'End time.'})
  .option('max-results', { type: 'number', alias: 'n', default: 10, describe: 'Maximum number of results to return.'})
  .strict(true);

async function top({ elasticsearch, metric, agg1, agg2, databaseName, loginName, serverName, hostName, startTime, endTime, maxResults }) {
  const request = {
    "size": 0,
    "aggs": {
      "_agg1": {
        "terms": {
          "field": `${agg1}.keyword`,
          "order": metric !== 'Count' ? { "_value": "desc"} : { "_count": "desc"},
          "size": maxResults
        },
        "aggs": {
          "_agg2": {
            "terms": {
              "field": `${agg2}.keyword`,
              "order": metric !== 'Count' ? { "_value": "desc"} : { "_count": "desc"},
              "size": 10
            },
            "aggs": metric !== 'Count' ? {
              "_value": {
                "sum": {
                  "field": metric
                }
              },
              "_text_data": agg2 === 'QueryHash' || agg2 === 'TextDataHash' ? {
                "top_hits": {
                  "sort": metric !== 'Count' ? {
                    [metric]: "desc",
                  } : undefined,
                  "size": 1,
                  "_source": {
                    "includes": [ "TextData" ]
                  }
                },
              } : undefined,
            } : undefined,
          },
          "_text_data": (agg1 === 'QueryHash' || agg1 === 'TextDataHash') && !(agg2 === 'QueryHash' || agg2 === 'TextDataHash') ? {
            "top_hits": {
              "sort": metric !== 'Count' ? {
                [metric]: "desc",
              } : undefined,
              "size": 1,
              "_source": {
                "includes": [ "TextData" ]
              }
            },
          } : undefined,
          "_value": metric !== 'Count' ? {
            "sum": {
              "field": metric
            }
          } : undefined,
        }
      }
    },
    "query": {
      "bool": {
        "must": [
          databaseName ? {
            "match_phrase": {
              "DatabaseName.keyword": {
                "query": databaseName
              }
            }
          } : { "match_all": {} },
          loginName ? {
            "match_phrase": {
              "LoginName.keyword": {
                "query": loginName
              }
            }
          } : { "match_all": {} },
          serverName ? {
            "match_phrase": {
              "ServerName.keyword": {
                "query": serverName
              }
            }
          } : { "match_all": {} },
          hostName ? {
            "match_phrase": {
              "HostName.keyword": {
                "query": hostName
              }
            }
          } : { "match_all": {} },
          {
            "range": {
              "@timestamp": {
                "format": "strict_date_optional_time",
                "gte": startTime.toISOString(),
                "lte": endTime.toISOString(),
              }
            }
          }
        ]
      }
    }
  };

  const response = await elasticsearch.search(request);

  if (response.aggregations._agg1.buckets.length === 0)
    return { agg1: [] };

  const totalValue = metric !== 'Count' ? response.aggregations._agg1.buckets[0]._value.value : response.aggregations._agg1.buckets[0].doc_count;

  const result = {
    agg1: response.aggregations._agg1.buckets.map(bucket => ({
      key: bucket.key,
      text: bucket._text_data ? bucket._text_data.hits.hits[0]._source.TextData : '',
      count: bucket.doc_count,
      value: metric !== 'Count' ? bucket._value.value : bucket.doc_count,
      valueAverage: metric !== 'Count' ? bucket._value.value / bucket.doc_count : 1,
      valuePercent: (metric !== 'Count' ? bucket._value.value : bucket.doc_count) / totalValue,
      agg2: bucket._agg2.buckets.map(subBucket => ({
        key: subBucket.key,
        text: subBucket._text_data ? subBucket._text_data.hits.hits[0]._source.TextData : '',
        count: subBucket.doc_count,
        value: metric !== 'Count' ? subBucket._value.value : subBucket.doc_count,
        valueAverage: metric !== 'Count' ? subBucket._value.value / subBucket.doc_count : 1,
        valuePercent: (metric !== 'Count' ? subBucket._value.value : subBucket.doc_count) / (metric !== 'Count' ? bucket._value.value : bucket.doc_count),
      })),
    })),
  };

  return result;
}

async function main() {
  const argv = yargs.argv;

  const elasticsearch = new Elasticsearch({
    address: argv['address'],
    port: argv['port'],
    indexPrefix: argv['index-prefix'],
  });

  const result = await top({
    elasticsearch: elasticsearch,
    metric: argv['metric'],
    agg1: argv['agg'],
    agg2: argv['agg2'],
    databaseName: argv['db'],
    loginName: argv['login'],
    serverName: argv['server'],
    hostName: argv['host'],
    startTime: argv['start'],
    endTime: argv['end'],
    maxResults: argv['max-results'],
  });

  const progressBarLength = 20;
  const maxValuePercentFormattedLength = '100.00%'.length;
  const valueUnit = argv['metric'] === 'Duration' ? 1000000 :
                    argv['metric'] === 'CPU' ? 1000 :
                    1;
  const format = argv['metric'] === 'Duration' ? '00:00:00' :
                 argv['metric'] === 'CPU' ? '00:00:00' :
                 (argv['metric'] === 'Reads' || argv['metric'] === 'Writes') ? '0b' :
                 '0,0';
  let maxValueFormattedLength = 0;

  for (const bucket of result.agg1) {
    bucket.countFormatted = numeral(bucket.count).format('0,0');
    bucket.valueFormatted = numeral(bucket.value / valueUnit).format(format);
    bucket.valueAverageFormatted = numeral(bucket.valueAverage / valueUnit).format(format);
    bucket.valuePercentFormatted = numeral(bucket.valuePercent).format('0.00%');

    if (bucket.valueFormatted.length > maxValueFormattedLength)
      maxValueFormattedLength = bucket.valueFormatted.length;

    for (const subBucket of bucket.agg2) {
      subBucket.countFormatted = numeral(subBucket.count).format('0,0');
      subBucket.valueFormatted = numeral(subBucket.value / valueUnit).format(format);
      subBucket.valueAverageFormatted = numeral(subBucket.valueAverage / valueUnit).format(format);
      subBucket.valuePercentFormatted = numeral(subBucket.valuePercent).format('0.00%');

      if (subBucket.valueFormatted.length > maxValueFormattedLength)
        maxValueFormattedLength = subBucket.valueFormatted.length;
    }
  }

  for (const bucket of _.reverse(result.agg1)) {
    const progressBar = makeProgressBar(bucket.valuePercent, progressBarLength);

    console.log('',
      progressBar.green.bold,
      padLeft(bucket.valuePercentFormatted, maxValuePercentFormattedLength).green.bold,
      padLeft(bucket.valueFormatted, maxValueFormattedLength).blue.bold,
      bucket.key.yellow,
      argv['metric'] !== 'Count' ? `${bucket.countFormatted} x ${bucket.valueAverageFormatted}`.yellow.dim : '',
    );

    for (const subBucket of bucket.agg2) {
      console.log('',
        new Array(progressBarLength + 1).join(' '),
        padLeft(subBucket.valuePercentFormatted, maxValuePercentFormattedLength).green.dim,
        padLeft(subBucket.valueFormatted, maxValueFormattedLength).blue,
        subBucket.key,
        argv['metric'] !== 'Count' ? `${subBucket.countFormatted} x ${subBucket.valueAverageFormatted}`.dim : '',
      );

      if (subBucket.text) {
        console.log();
        console.log(truncate(subBucket.text, 10000).dim);
        console.log();
      }
    }

    if (bucket.text) {
      console.log();
      console.log(truncate(bucket.text, 10000).dim);
      console.log();
    }

    console.log();
  }
}

main();
