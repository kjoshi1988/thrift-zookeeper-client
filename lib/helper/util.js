var clusterPool = require('node-pool-cluster');
var _ = require('underscore');
var thrift = require('thrift');
var TIMEOUT_MESSAGE = "ZooKeeper-pool: Connection timeout";
var CLOSE_MESSAGE = "ZooKeeper-pool: Connection closed";
var zookeeper = require('node-zookeeper-client');

var serverSetHelper = {};

/**
 * Create a key for storing details the object in the cache.
 * Key is created as a combination of zookeeper url, path and service name
 *
 * @param {Object} zkProps, Object containing zooKeeper server ip, port and path
 * @param {string} serviceName, service name to be fetched
 * @return {string}, cache key
 */
serverSetHelper.getServiceClientCacheKey = function (zkProps, serviceName) {
    return zkProps.zkUrl + "~" + zkProps.zkPath + "~" + serviceName;
};


/**
 * Creates a cluster pool of a service.
 *
 * @param {String} clusterName, name of the cluster(service)
 * @param {Object} opts, pool factory options, similar to generic pool
 * @return {Object} returns cluster instance
 */
serverSetHelper.initCluster = function (clusterName, opts) {
    return clusterPool.initCluster({
        name: clusterName,
        destroy: function (connection) {
            return connection.end();
        },
        validate: function (connection) {
            return !connection.__ended;
        },
        max: opts.max || 3,
        min: opts.min || 0,
        refreshIdle: opts.refreshIdle || true,
        reapIntervalMillis: opts.reapIntervalMillis || 1000,
        idleTimeoutMillis: opts.idleTimeoutMillis || 20000
    }, true);
};


/**
 * Creates a connection to thrift service running on the specified host.
 *
 * @param {String} serviceName, Name of the service to be fetched
 * @param {Object} serverInfo, info regarding server, its ip and port
 * @param {async} async, to enable TFramed protocol
 * @param {Function} callBack, function which is called on successful connection or when error occurs.
 * @return {EventEmitter|*}
 */
serverSetHelper.createThriftConn = function (serviceName, serverInfo, async, callBack) {
    callBack = _.once(callBack);
    console.log("[Node-Conn-%s]%s:%d", serviceName, serverInfo.host, serverInfo.port);
    var connection = thrift.createConnection(serverInfo.host, serverInfo.port, {
        transport: (!!async ? thrift.TFramedTransport : thrift.TBufferedTransport),
        protocol: thrift.TBinaryProtocol
    });
    connection.__ended = false;
    connection.on("connect", function () {
        connection.connection.setKeepAlive(true);
        return callBack(null, connection);
    });
    connection.on("error", function (err) {
        connection.__ended = true;
        return callBack(err);
    });
    connection.on("close", function () {
        connection.__ended = true;
        return callBack(new Error(CLOSE_MESSAGE));
    });
    return connection.on("timeout", function () {
        connection.__ended = true;
        return callBack(new Error(TIMEOUT_MESSAGE));
    });
};

/**
 * It fetches details of all the running server instances(ip and port) of the specified service.
 *
 * @param {Object} zkProps, Object containing zooKeeper server ip, port and path
 * @param {String} serviceName, Name of the service to be fetched
 * @param {String} node, member node of the service containing information of server ip and port
 * @param {Object} cluster, cluster of pool for specified service
 * @param {async} async, to enable TFramed protocol
 * @param cb
 */
serverSetHelper.getNodeDetails = function(zkProps, serviceName, node, cluster, async, cb){
    var _pool, zkClient = this;
    zkClient.getData(zkProps.zkPath + "/nodeStatus/" + serviceName + "/" + node,
        function (event) {
            if (zookeeper.Event.NODE_DELETED === event || zookeeper.Event.NODE_DATA_CHANGED === event) {
                cluster.removePool(_pool);
                serverSetHelper.getNodeDetails().call(zkClient, zkProps, serviceName, node, cluster, async);
            }
        },
        function (err, data) {
            var _nodeDetails = JSON.parse(data.toString());
            var _host, _port;
            if (_nodeDetails.status == "ALIVE") {
                _host = _nodeDetails["serviceEndpoint"]["host"];
                _port = _nodeDetails["serviceEndpoint"]["port"];
                _pool = cluster.addPool(function (callBack) {
                    return serverSetHelper.createThriftConn(serviceName, {host: _host, port: _port}, async, callBack);
                });
            }
            !!cb && cb();
        }
    );
};

module.exports = serverSetHelper;