var zkHelper = require("./helper/zkHelper");

var factory = {};

factory.config = zkHelper.config;

factory.getService = function (zkProps, serviceName, serviceClient, callBack) {
    var _serviceClient = zkHelper.getClientFromCache(zkProps, serviceName);
    if (!!_serviceClient) {
        callBack(_serviceClient);
    } else {
        zkHelper.createServiceClient(zkProps, serviceName, serviceClient, callBack);
    }
};

module.exports = factory;
