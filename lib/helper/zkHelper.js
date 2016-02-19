var zookeeper = require('node-zookeeper-client');
var async = require('async');
var serviceClientWrapper = require("./serviceClientWrapper");
var _util = require('./util');
var _initCbQueue = {}, _clusterMap = {}, _serviceClients = {}
    , _zooKeeperClient, _config = {};


var zkHelper = {};

/**
 * Sets config options, like min/max/timeout of resources in a pool.
 * Defaults opts are:
 * <pre>
 *     max: 3
 *     min: 0
 *     idleTimeoutMillis: 20000(ms)
 * </pre>
 *
 */
zkHelper.config = function (opts) {
    _config.min = opts.min || 3;
    _config.max = opts.max || 0;
    _config.idleTimeoutMillis = opts.idleTimeoutMillis || 20000;
    _config.refreshIdle = opts.refreshIdle || true;
    _config.reapIntervalMillis = opts.reapIntervalMillis || 1000;
};

/**
 * This method creates thrift service client. It first checks, whether a client is already in cache or not.
 * If service client is already there in cache, then the callback is called with service client as argument,
 * otherwise it creates a new client.
 *
 *
 * @param {Object} zkProps, Object containing zooKeeper server ip, port and path
 * @param {String} serviceName, Name of the service to be fetched
 * @param {Object} serviceClient, thrift client for the service
 * @param {Function} callBack, this will be called, after creation of thrift service client.
 * @param {async} async, to enable TFramed protocol
 */
zkHelper.getService = function (zkProps, serviceName, serviceClient, callBack, async) {
    var _serviceClient = _serviceClients[_util.getServiceClientCacheKey(zkProps, serviceName)];
    if (!!_serviceClient) {
        callBack(_serviceClient);
    } else {
        zkHelper.createServiceClient(zkProps, serviceName, serviceClient, async, callBack);
    }
};


/**
 * Fetches service details hosted at the specified path.
 * It also adds watcher on the node, which will be triggered when service node is either deleted, modified or more
 * children are added.
 * Until the children details are fetched and client is created, all the request(for the same service client) callbacks
 * are queued.
 *
 * @param {Object} zkProps, Object containing zooKeeper server ip, port and path
 * @param {String} serviceName, Name of the service to be fetched
 * @param {Object} serviceClient, thrift client for the service
 * @param {async} async, to enable TFramed protocol
 * @param {Function} callBack
 */
zkHelper.createServiceClient = function (zkProps, serviceName, serviceClient, async, callBack) {
    var _cacheKey = _util.getServiceClientCacheKey(zkProps, serviceName);
    var _cbQueue = _initCbQueue[_cacheKey];
    if (!!_cbQueue) {
        !!callBack && _cbQueue.push(callBack);
    } else {
        _cbQueue = [];
        _initCbQueue[_cacheKey] = _cbQueue;
        !!callBack && _cbQueue.push(callBack);
        callZookeeper(zkProps.zkUrl, function (zkClient) {
            zkClient.getChildren(zkProps.zkPath + "/nodeStatus/" + serviceName,
                zkWatcher.bind(zkClient, zkProps, serviceName, serviceClient, async),
                zkProcessChildren.bind(zkClient, zkProps, serviceName, serviceClient, async)
            );
        });
    }
};


/**
 * Creates zookeeper connection client.
 *
 * @param {String} zkUrl, zookeeper server url
 * @param {Function} callBack, this is called after connection with zookeeper is established.
 */
function callZookeeper(zkUrl, callBack) {
    if (!_zooKeeperClient || (!!_zooKeeperClient.getState() && (
            _zooKeeperClient.getState() === zookeeper.State.EXPIRED ||
            _zooKeeperClient.getState() === zookeeper.State.AUTH_FAILED ||
            _zooKeeperClient.getState() === zookeeper.State.DISCONNECTED
        ))) {
        _zooKeeperClient = zookeeper.createClient(zkUrl);
        _zooKeeperClient.on('state', function (state) {
            if (state === zookeeper.State.EXPIRED || state === zookeeper.State.DISCONNECTED) {
                _zooKeeperClient = undefined;
            }
        });
        _zooKeeperClient.once("connected", function () {
            callBack(_zooKeeperClient);
        });
        _zooKeeperClient.connect();
    } else {
        callBack(_zooKeeperClient);
    }
}

/**
 * Watched event on the service node, which is triggered when the node is either deleted, modified, created or
 * children are changed.
 *
 * @param {Object} zkProps, Object containing zooKeeper server ip, port and path
 * @param {String} serviceName, Name of the service to be fetched
 * @param {Object} serviceClient, thrift client for the service
 * @param {async} async, to enable TFramed protocol
 * @param {Object} event, see @{zookeeper.Event}
 */
function zkWatcher(zkProps, serviceName, serviceClient, async, event) {
    if (zookeeper.Event.NODE_DELETED === event || zookeeper.Event.NODE_DATA_CHANGED === event
        || zookeeper.Event.NODE_CREATED === event || zookeeper.Event.NODE_CHILDREN_CHANGED === event) {
        zkHelper.createServiceClient(zkProps.zkUrl, serviceName, serviceClient, async, null);
    }
}

/**
 * Fetches children node for a service.
 * A cluster of pool is created for the service. For each child, a pool is added to the cluster.
 * Before fetching children details, if any previous cluster is drained, i.e. all the pools in the cluster
 * are drained gracefully.
 *
 * @param {Object} zkProps, Object containing zooKeeper server ip, port and path
 * @param {String} serviceName, Name of the service to be fetched
 * @param {Object} serviceClient, thrift client for the service
 * @param {Boolean} _async, to enable TFramed protocol
 * @param {Object} err, zookeeper connection error
 * @param {Array} children, all the running nodes of service
 */
function zkProcessChildren(zkProps, serviceName, serviceClient, _async, err, children) {
    var _cacheKey = _util.getServiceClientCacheKey(zkProps, serviceName);
    var _cluster = _clusterMap[_cacheKey];
    var zkClient = this;
    if (!!_cluster) {
        _cluster.clear();
        delete _clusterMap[_cacheKey];
    }
    if (!!err) {
        clearCbQueue(_cacheKey, undefined, err);
        return;
    }
    _cluster = _util.initCluster(serviceName, _config);
    var _nodeDetailsFn = [];
    children.forEach(function (node) {
        _nodeDetailsFn.push(_util.getNodeDetails.bind(zkClient, zkProps, serviceName, node, _cluster, _async));
    });
    async.parallel(_nodeDetailsFn, function () {
        var _serviceClientClone = serviceClientWrapper(serviceClient, _cluster);
        _serviceClients[_cacheKey] = _serviceClientClone;
        clearCbQueue(_cacheKey, _serviceClientClone, err);
    });
}

/**
 * Clears all the callbacks added to the queue.
 * Callback is invoked with arguments serviceClient and err object, if any.
 *
 * @param {String} cacheKey, cache key for call back array.
 * @param {Object} serviceClient, thrift service client
 * @param {Object} err, err, if any while connecting via zookeeper
 */
function clearCbQueue(cacheKey, serviceClient, err) {
    var cbQueue = _initCbQueue[cacheKey];
    while (cbQueue.length != 0) cbQueue.splice(0, 1)[0](serviceClient, err);
    delete _initCbQueue[cacheKey];
}

module.exports = zkHelper;