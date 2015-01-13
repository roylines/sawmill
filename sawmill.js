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

    var requestCount = 0;
    var minTime = 9007199254740992;
    var maxTime = 0;
    var lastmessage = '';

    log.events.forEach(function(event) {
      if(!event.message) {
        console.error('no message on event', event);
        return;
      }

      if(lastmessage === event.message) {
        //duplicate, ignore it
        return;
      }

      lastmessage = event.message;

      var parsed = parseLine(lastmessage);
      if (parsed.error) {
        console.error(parsed.error, event);
        return;
      }

      requestCount++;
      minTime = Math.min(minTime, parsed.date);
      maxTime = Math.max(maxTime, parsed.date);
      
      var statuscode = parsed.statusCode;
      var haproxy = parsed.haproxy;
      var nodeserver = parsed.nodeserver;

      var frontendConnections = parsed.frontendConnections;
      var backendConnections = parsed.backendConnections;

      var backend = parsed.backend;
      var backendServer = parsed.backendServer;

      var totalTime = parsed.totalTime;
      var totalRequestTime = parsed.totalRequestTime;
      var totalResponseTime = parsed.totalResonseTime;

      if (isNumber(statuscode)) {
        incStatusCode(['statuscode', statuscode, 'all'].join('.'));
        incStatusCode(['statuscode', statuscode, haproxy].join('.'));
        incStatusCode(['statuscode', statuscode[0], 'all'].join('.'));
      }

      if (frontendConnections) {
        metrics().gauge(bucket('connections.frontend.all'), +frontendConnections);
      }

      if (backendConnections) {
        metrics().gauge(bucket('connections.backend', backendServer), +backendConnections);
      }

      if (totalTime) {
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

    var elapsedSeconds = Math.max(1, (maxTime - minTime) / 1000);
    var requestsPerSecond = Math.round(requestCount / elapsedSeconds);
    console.log(bucket('request.all'), requestsPerSecond);
    metrics().gauge(bucket('request.all'), requestsPerSecond);

    _.keys(statusCounts).forEach(function(k) {
      if (statusCounts[k] > 0) {        
        console.log(bucket(k), statusCounts[k]);
      }

      metrics().gauge(bucket(k), statusCounts[k]);
    });

    console.log('.');
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
