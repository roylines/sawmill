var parser = {};

parser.parseLine = function(message) {
  var results = _parseLine(message);
  var parsed = {};
  if (!results) {
    parsed.error = 'skipping, event has too few fields';
    return parsed;
  }

  parsed.date = results.date.getTime();
  parsed.statusCode = results.status_code;
  parsed.nodeserver = results.backend_name + '/' + results.server_name;
  parsed.backend = results.backend_name;
  parsed.backendServer = results.server_name.split('-')[0];
  parsed.haproxy = results.process_name + '.' + results.pid;
  parsed.frontendConnections = results.feconn.toString();
  parsed.backendConnections = results.beconn.toString();
  parsed.totalRequestTime = results.tq.toString();
  parsed.totalResponseTime = results.tr.toString();
  parsed.totalTime = results.tt.toString();

  return parsed;
};

module.exports = parser;

var fields = {
    'date' : function (date) { return new Date(date); }
    , 'host' : String
    , 'process_name' : String
    , 'pid' : parseInt
    , 'client_ip' : String
    , 'client_port' : parseInt
    , 'accept_date' : function (date) { return new Date(date) ;}
    , 'frontend_name' : String
    , 'backend_name' : String
    , 'server_name' : String
    , 'tq' : parseInt
    , 'tw' : parseInt
    , 'tc' : parseInt
    , 'tr' : parseInt
    , 'tt' : parseInt
    , 'status_code' : String
    , 'bytes_read' : parseInt
    , 'captured_request_cookie' : String
    , 'captured_response_cookie' : String
    , 'terminiation_state' : String
    , 'actconn' : parseInt
    , 'feconn' : parseInt
    , 'beconn' : parseInt
    , 'srv_conn' : parseInt
    , 'retries' : parseInt
    , 'srv_queue' : parseInt
    , 'backend_queue' : parseInt
    , 'method' : String
    , 'request' : String
    , 'version' : String
};

var keys = Object.keys(fields);

function _parseLine (str) {
    var reg = /^(\w+\s+\d+\s+\S+)\s+(\S+)\s+(\S+)\[(\d+)\]:\s+(\S+):(\d+)\s+\[(\S+)\]\s+(\S+)\s+(\S+)\/(\S+)\s+(\S+)\/(\S+)\/(\S+)\/(\S+)\/(\S+)\s+(\S+)\s+(\S+) *(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\/(\S+)\/(\S+)\/(\S+)\/(\S+)\s+(\S+)\/(\S+)\s+"(\S+)\s+([^"]+)\s+(\S+)" *$/gi
        , obj
        , matches
        ;

    matches = reg.exec(str);
    
    if (!matches) {
        return null;
    }

    obj = {};
    matches.forEach(function (val, ix) {
        if (ix === 0 ) return;

        obj[keys[ix - 1]] = fields[keys[ix - 1]](val);
    });

    return obj;
}
