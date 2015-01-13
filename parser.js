var parse = require('haproxy-log-parse');
var parser = {};

parser.parseLine = function(message) {
  var results = parse(message);
  var parsed = {};
  if (!results) {
    parsed.error = 'skipping, event has too few fields';
    return parsed;
  }

  parsed.date = new Date(message.substr(0, 15)).getTime(),
  parsed.statusCode = results.status_code;
  parsed.nodeserver = results.backend_name + '/' + results.server_name;
  parsed.backend = results.backend_name
  parsed.backendServer = results.server_name;
  parsed.haproxy = results.process_name + '.' + results.pid;
  parsed.frontendConnections = results.feconn.toString();
  parsed.backendConnections = results.beconn.toString();
  parsed.totalRequestTime = results.tq.toString();
  parsed.totalResponseTime = results.tr.toString();
  parsed.totalTime = results.tt.toString();

  return parsed;
};

module.exports = parser;
