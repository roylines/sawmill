var expect = require('chai').expect,
    parser = require('../parser'),
    fixture = require('./fixture.json');

describe('parseLine', function () {
  it('should not error', function() { 
    parser.parseLine('');
  });

  it('should return correct values', function () {
    var line =  "Jan 13 10:21:38 localhost haproxy[29285]: 78.54.251.74:53867 [13/Jan/2015:10:20:36.708] www-https~ node-servers/node1-10.28.183.88 60988/0/2/384/61374 200 168 - - ---- 10/9/2/0/0 0/0 \"POST /api/1/traffic HTTP/1.1\"";
    var expected = {
      date: new Date('Jan 13 10:21:38').getTime(),
      statusCode: "200",
      haproxy: "haproxy.29285",
      nodeserver: "node-servers/node1-10.28.183.88",
      frontendConnections: "9",
      backendConnections: "2",
      backend: "node-servers",
      backendServer: "node1",
      totalTime: "61374",
      totalRequestTime: "60988",
      totalResponseTime: "384"
    };
    var result = parser.parseLine(line);
    expect(result).to.deep.equal(expected);
  });

  it('should return correct values with http', function () {
    var line =  "Jan 13 10:21:38 localhost haproxy[29285]: 78.54.251.74:53867 [13/Jan/2015:10:20:36.708] www-http~ node-servers/node1-10.28.183.88 60988/0/2/384/61374 200 168 - - ---- 10/9/2/0/0 0/0 \"POST /api/1/traffic HTTP/1.1\"";
    var expected = {
      date: new Date('Jan 13 10:21:38').getTime(),
      statusCode: "200",
      haproxy: "haproxy.29285",
      nodeserver: "node-servers/node1-10.28.183.88",
      frontendConnections: "9",
      backendConnections: "2",
      backend: "node-servers",
      backendServer: "node1",
      totalTime: "61374",
      totalRequestTime: "60988",
      totalResponseTime: "384"
    };
    var result = parser.parseLine(line);
    expect(result).to.deep.equal(expected);
  });

  it('should return errors', function () {
    var line =  "Jan 13 10:21:38 localhost [29285]: 78.54.251.74:53867 [13/Jan/2015:10:20:36.708] www-https~ node-servers/node1-10.28.183.88 60988/0/2/384/61374 200 168 - - ---- 10/9/2/0/0 0/0 \"POST /api/1/traffic HTTP/1.1\"";
    var expected = {
      error: "skipping, event has too few fields",
    };

    var result = parser.parseLine(line);
    expect(result).to.deep.equal(expected);
  });

  it('should return error when there is no enough events', function () {
    var line =  "Jan 13 10:21:38 localhost haproxy[29285]: 200 168 - - ---- 10/9/2/0/0 0/0 \"POST /api/1/traffic HTTP/1.1\"";
    var expected = {
      error: "skipping, event has too few fields",
    };

    var result = parser.parseLine(line);
    expect(result).to.deep.equal(expected);
  });
  
  it('should return error when message is empty', function () {
    var line =  "";
    var expected = {
      error: "skipping, event has too few fields",
    };

    var result = parser.parseLine(line);
    expect(result).to.deep.equal(expected);
  });
  
  it('should return error when there is no valid log', function () {
    var line =  "Jan 13 10:22:01 ip-10-138-42-169 CROND[13348]: (root) CMD (source /root/.bashrc; cd /root/elemez-loadbalancer-config; ./discover >> /var/log/node-servers-discover.log 2>&1)";
    var expected = {
      error: "skipping, event has too few fields",
    };

    var result = parser.parseLine(line);
    expect(result).to.deep.equal(expected);
  });
});
