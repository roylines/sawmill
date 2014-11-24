#!/usr/local/bin/node

var _ = require('lodash'),
  AWS = require('aws-sdk'),
  cloudwatchlogs = new AWS.CloudWatchLogs(),
  lynx = require('lynx'),
  lynxInstance = undefined;

function metrics() {
  if (!lynxInstance) {
    lynxInstance = new lynx(process.env.SAWMILL_STATSD_URL, 8125, {
      on_error: function(a, b) {
        console.log(a, b);
      }
    });
  }
  return lynxInstance;
}

function bucket(name) {
  return [process.env.SAWMILL_STATSD_PREFIX, name].join('.');
}

function isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

var statusCounts = {};

function run(nextToken) {
  var params = {
    logGroupName: '/var/log/haproxy.log',
    logStreamName: 'loadbalancers',
    nextToken: nextToken
  };

  cloudwatchlogs.getLogEvents(params, function(err, log) {
    if (err) {
      console.error('saw failure!', err);
    }

    if (!log) return wait(nextToken, run);
    if (!nextToken) return wait(log.nextForwardToken, run);

    var requestCount = 0;


    //reset the counts
    _.keys(statusCounts).forEach(function(k) {
      statusCounts[k] = 0;
    });

    function incStatusCode(code) {
      if (!statusCounts[code]) {
        statusCounts[code] = 0;
      }

      statusCounts[code] = statusCounts[code] + 1;
    }

    log.events.forEach(function(event) {
      var splits = event.message.split(' ');
      //find the haproxy index:
      var haproxyindex = -1;
      for (var i = 0; i < splits.length; ++i) {
        if (splits[i].substr(0, 7) === 'haproxy') {
          haproxyindex = i;
          break;
        }
      }

      if (haproxyindex === -1) {
        return;
      }

      requestCount++;

      var statuscode = splits[haproxyindex + 6],
        totalTimes = splits[haproxyindex + 5],
        haproxy = splits[haproxyindex],
        nodeserver = splits[haproxyindex + 4],
        connections = splits[haproxyindex + 11];

      //console.log('status', { statuscode: statuscode, haproxy: haproxy, nodeserver: nodeserver, connections: connections, splits: splits });

      haproxy = haproxy.replace('[', '.').replace(']', '').replace(':', '');

      if (isNumber(statuscode)) {
        incStatusCode(['statuscode', statuscode, 'all'].join('.'));
        incStatusCode(['statuscode', statuscode, haproxy].join('.'));
        incStatusCode(['statuscode', statuscode[0], 'all'].join('.'));
      }

      if (connections && connections.length) {
        var frontendConnections = connections.split('/')[1],
          backendConnections = connections.split('/')[2],
          frontendConnectionsBucket = bucket('connections.frontend.all'),
          backendConnectionsBucket = bucket(['connections.backend', nodeserver.replace('node-servers/', '')].join('.'));
        metrics().gauge(frontendConnectionsBucket, +frontendConnections);
        metrics().gauge(backendConnectionsBucket, +backendConnections);
      }

      var totalTimes = totalTimes.split('/');
      if (totalTimes && totalTimes.length === 5) {
        var tq = totalTimes[0];
        var tr = totalTimes[3];
        var tt = totalTimes[4];
        var payload = {};
        payload[bucket('totaltime.request')] = tq + '|ms';
        payload[bucket('totaltime.response')] = tr + '|ms';
        payload[bucket('totaltime.total')] = tt + '|ms';
        metrics().send(payload);
      }

    });

    var requestsPerSecond = log.events.length / 10;
    console.log(bucket('request.all'), requestsPerSecond);
    metrics().gauge(bucket('request.all'), requestsPerSecond);

    _.keys(statusCounts).forEach(function(k) {
      console.log(bucket(k), statusCounts[k]);
      metrics().gauge(bucket(k), statusCounts[k]);
      if (statusCounts[k] === 0) {
        delete statusCounts[k];
      }
    });

    console.log('');
    wait(log.nextForwardToken, run);
  });
}

function wait(token, cb) {
  setTimeout(function() {
    cb(token);
  }, 10000);
}

console.log('starting the saws...');
return run();
