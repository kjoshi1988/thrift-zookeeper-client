var zookeeper = require('node-zookeeper-client');
var async = require('async');
var serviceClientWrapper = require("./serviceClientWrapper");
var _util = require('./util.js');
var _initCbQueue = {}, _clusterMap = {}, _serviceClients = {}
    , _zooKeeperClient, _config = {};


var zkHelper = {};

zkHelper.config = function(){
};

/**
 *
 *
 * @param zkProps
 * @param serviceName
 * @param serviceClient
 * @param callBack
 */
zkHelper.createServiceClient = function (zkProps, serviceName, serviceClient, callBack) {
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
                zkWatcher.bind(zkClient, zkProps, serviceName, serviceClient),
                zkProcessChildren.bind(zkClient, zkProps, _cbQueue, serviceName, serviceClient)
            );
        });
    }
};

/**
 *
 * @param zkProps
 * @param serviceName
 * @return {*}
 */
zkHelper.getClientFromCache = function(zkProps, serviceName){
    return _serviceClients[_util.getServiceClientCacheKey(zkProps, serviceName)];
};

/**
 *
 * @param zkUrl
 * @param callBack
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
 *
 *
 * @param zkProps
 * @param serviceName
 * @param serviceClient
 * @param event
 */
function zkWatcher(zkProps, serviceName, serviceClient, event) {
    if (zookeeper.Event.NODE_DELETED === event || zookeeper.Event.NODE_DATA_CHANGED === event
        || zookeeper.Event.NODE_CREATED === event || zookeeper.Event.NODE_CHILDREN_CHANGED === event) {
        createServiceClient(zkProps.zkUrl, serviceName, serviceClient, null, true);
    }
}

/**
 *
 *
 * @param zkProps
 * @param cbQueue
 * @param serviceName
 * @param serviceClient
 * @param err
 * @param children
 */
function zkProcessChildren(zkProps, cbQueue, serviceName, serviceClient, err, children) {
    var _cacheKey = _util.getServiceClientCacheKey(zkProps, serviceName);
    var _cluster = _clusterMap[_cacheKey];
    var zkClient = this;
    if (!!_cluster) {
        _cluster.drainAll();
        delete _clusterMap[_cacheKey];
    } else {
        _cluster = _util.initCluster(serviceName, _config);
        var _nodeDetailsFn = [];
        children.forEach(function (node) {
            _nodeDetailsFn.push(_util.getNodeDetails.bind(zkClient, zkProps, serviceName, node, _cluster));
        });
        async.parallel(_nodeDetailsFn, function () {
            var _serviceClientClone = serviceClientWrapper(serviceClient, _cluster);
            _serviceClients[_cacheKey] = _serviceClientClone;
            while (cbQueue.length != 0) cbQueue.splice(0, 1)[0](_serviceClientClone);
            delete _initCbQueue[_cacheKey];
        });
    }
}

module.exports = zkHelper;