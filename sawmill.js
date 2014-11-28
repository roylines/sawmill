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

function bucket() {
  var name = Array.prototype.slice.call(arguments).join('.');
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

    console.log('processing...', new Date());

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
      if(!event.message) {
        console.error('no message on event', event);
        return;
      }

      var splits = event.message.split(' ');
      //find the haproxy index:
      var haproxyindex = -1;
      for (var i = 0; i < splits.length; ++i) {
        if (splits[i].substr(0, 7) === 'haproxy') {
          haproxyindex = i;
          break;
        }
      }

      if (haproxyindex === -1 || splits.length <= haproxyindex + 11) {
        console.error('skipping, event has too few fields', event.message);
        return;
      }

      requestCount++;

      var statuscode = splits[haproxyindex + 6],
        totalTimes = splits[haproxyindex + 5],
        haproxy = splits[haproxyindex],
        nodeserver = splits[haproxyindex + 4],
        connections = splits[haproxyindex + 11];

      var splitNodeServer = nodeserver.split['/'];

      var backend = splitNodeServer[0];
      var backendServer = splitNodeServer[1];

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
          backendConnectionsBucket = bucket('connections.backend', backendServer);
        metrics().gauge(frontendConnectionsBucket, +frontendConnections);
        metrics().gauge(backendConnectionsBucket, +backendConnections);
      }

      var totalTimes = totalTimes.split('/');
      if (totalTimes && totalTimes.length === 5) {
        var tq = totalTimes[0];
        var tr = totalTimes[3];
        var tt = totalTimes[4];
        var payload = {};
        payload[bucket(backend, 'totaltime.request')] = tq + '|ms';
        payload[bucket(backend, 'totaltime.response')] = tr + '|ms';
        payload[bucket(backend, 'totaltime.total')] = tt + '|ms';
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
      if (statusCounts[k] > 0) {        
        console.log(bucket(k), statusCounts[k]);
      }

      metrics().gauge(bucket(k), statusCounts[k]);
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
