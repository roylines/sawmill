#!/usr/local/bin/node

var AWS = require('aws-sdk'),
  cloudwatchlogs = new AWS.CloudWatchLogs(),
  lynx = require('lynx');

function metrics() {
  return new lynx(process.env.SAWMILL_STATSD_URL, 8125);
}

function bucket(name) {
  return [process.env.SAWMILL_STATSD_PREFIX, name].join('.');
}

function isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function run(nextToken) {
  var params = {
    logGroupName: '/var/log/haproxy.log',
    logStreamName: 'loadbalancers',
    nextToken: nextToken
  };

  process.stdout.write('chop!');
  cloudwatchlogs.getLogEvents(params, function(err, log) {
    if(err) {
      console.error('saw failure!', err);
    }

    if (!log) return wait(nextToken, run);
    if (!nextToken) return wait(log.nextForwardToken, run);

    log.events.forEach(function(event) {
      process.stdout.write('z');
      var splits = event.message.split(' ');
      //find the haproxy index:
      var haproxyindex = -1;
      for(var i=0; i<splits.length; ++i) {
        if(splits[i].substr(0, 7) === 'haproxy') {
          haproxyindex = i;
          break;
        }
      }
      
      if(haproxyindex === -1) {
        return;
      }

      var statuscode = splits[haproxyindex + 6],
        haproxy = splits[haproxyindex],
        nodeserver = splits[haproxyindex + 4],
        connections = splits[haproxyindex + 11];
      
      //console.log('status', { statuscode: statuscode, haproxy: haproxy, nodeserver: nodeserver, connections: connections, splits: splits });

      haproxy = haproxy.replace('[', '.').replace(']', '').replace(':', '');

      if (isNumber(statuscode)) {
        var statusCodeBucket = bucket(['statuscode', statuscode, 'all'].join('.'));
        var statusCodeHaproxyBucket = bucket(['statuscode', statuscode, haproxy].join('.'));
        var statusCodePrimaryHaproxyBucket = bucket(['statuscode', statuscode[0], 'all'].join('.'));
        metrics().increment([statusCodeBucket, statusCodeHaproxyBucket, statusCodePrimaryHaproxyBucket]);
        process.stdout.write('buzz!');
      }

      if (connections && connections.length) {
        var frontendConnections = connections.split('/')[1],
          backendConnections = connections.split('/')[2],
          frontendConnectionsBucket = bucket('connections.frontend.all'),
          backendConnectionsBucket = bucket(['connections.backend', nodeserver.replace('node-servers/', '')].join('.'));
        metrics().gauge(frontendConnectionsBucket, +frontendConnections);
        metrics().gauge(backendConnectionsBucket, +backendConnections);
        process.stdout.write('buzz!');
      }
    });
    console.log('');
    wait(log.nextForwardToken, run);
  });
}

function wait(token, cb) {
  setTimeout(function() {
    cb(token);
  }, 3000);
}

console.log('starting the saws...');
return run();
