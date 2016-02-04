var clusterPool = require('node-pool-cluster');
var _ = require('underscore');
var thrift = require('thrift');
var TIMEOUT_MESSAGE = "ZooKeeper-pool: Connection timeout";
var CLOSE_MESSAGE = "ZooKeeper-pool: Connection closed";

var serverSetHelper = {};

/**
 *
 * @param zkProps
 * @param serviceName
 * @return {string}
 */
serverSetHelper.getServiceClientCacheKey = function (zkProps, serviceName) {
    return zkProps.zkUrl + "~" + zkProps.zkPath + "~" + serviceName;
};


/**
 *
 *
 * @param clusterName
 * @param opts
 * @return {*|Object}
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
        idleTimeoutMillis: opts.idleTimeoutMillis || 20000
    });
};

/**
 *
 * @param serviceName
 * @param serverInfo
 * @param callBack
 * @return {EventEmitter|*}
 */
serverSetHelper.createThriftConn = function (serviceName, serverInfo, callBack) {
    callBack = _.once(callBack);
    console.log("[Node-Conn-%s]%s:%d", serviceName, serverInfo.host, serverInfo.port);
    var connection = thrift.createConnection(serverInfo.host, serverInfo.port, {});
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
 *
 *
 * @param zkProps
 * @param serviceName
 * @param node
 * @param cluster
 * @param cb
 */
serverSetHelper.getNodeDetails = function(zkProps, serviceName, node, cluster, cb){
    var _pool, zkClient = this;
    zkClient.getData(zkProps.zkPath + "/nodeStatus/" + serviceName + "/" + node,
        function (event) {
            if (zookeeper.Event.NODE_DELETED === event || zookeeper.Event.NODE_DATA_CHANGED === event) {
                cluster.removePool(_pool);
                serverSetHelper.getNodeDetails().call(zkClient, zkProps, serviceName, node, cluster);
            }
        },
        function (err, data) {
            var _nodeDetails = JSON.parse(data.toString());
            var _host, _port;
            if (_nodeDetails.status == "ALIVE") {
                _host = _nodeDetails["serviceEndpoint"]["host"];
                _port = _nodeDetails["serviceEndpoint"]["port"];
                _pool = cluster.addPool(function (callBack) {
                    return serverSetHelper.createThriftConn(serviceName, {host: _host, port: _port}, callBack);
                });
            }
            !!cb && cb();
        }
    );
};


module.exports = serverSetHelper;