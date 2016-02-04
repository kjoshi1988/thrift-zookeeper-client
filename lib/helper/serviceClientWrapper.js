var __slice = [].slice;
var _ = require("underscore");
_.mixin(require("underscore.deep"));
var thrift = require('thrift');
var TIMEOUT_MESSAGE = "ZooKeeper-pool: Connection timeout";
var CLOSE_MESSAGE = "ZooKeeper-pool: Connection closed";

/**
 * Error handlers
 *
 * @param cb
 * @return {{error: Function, timeout: Function, close: Function}}
 */
function getErrorHandlers(cb){
    return {
        error: function(err){
            return cb(err);
        },
        timeout: function(){
            return cb(new Error(TIMEOUT_MESSAGE));
        },
        close: function(){
            return cb(new Error(CLOSE_MESSAGE));
        }
    };
}

/**
 * Binds listeners to the connection client.
 *
 * @param connection
 * @param errorHandlers
 * @return {EventEmitter|*}
 */
function addListeners(connection, errorHandlers) {
    connection.on("error", errorHandlers.error);
    connection.on("close", errorHandlers.close);
    return connection.on("timeout", errorHandlers.timeout);
}

/**
 * Removes listeners added to connection client, after getting response from the client
 *
 * @param connection
 * @param errorHandlers
 * @return {EventEmitter|*}
 */
function removeListeners(connection, errorHandlers) {
    connection.removeListener("error", errorHandlers.error);
    connection.removeListener("close", errorHandlers.close);
    return connection.removeListener("timeout", errorHandlers.timeout);
}

/**
 * Releases all pool resources acquired after processing the request.
 * It also removes all the added listeners to the connection client.
 * After releasing all the resources, it then calls the callback provided by the user.
 *
 * @param callback
 * @param errorHandlers, all the handlers(error, close & timeout) binded to connection.
 * @param pool, one of the generic pools from cluster
 * @param err, error object if any error occur while acquiring resource.
 * @param resp, resp from the server
 * @return {*}
 */
function releaseResources(callback, errorHandlers, pool, err, resp){
    var connection = this;
    removeListeners(connection, errorHandlers);
    pool.release(connection);
    return callback(err, resp);
}

/**
 *
 *
 * @param cluster
 * @param fn
 * @param methodName
 * @return {Function}
 */
function wrapperFn(cluster, fn, methodName) {
    var serviceClient = this;
    return function () {
        var args; //arguments to be passed to the service client method
        var cb; //callback that will be called after getting response from the client.

        //converting implicit argument as to pass it to client method and scraping callback
        if (arguments.length >= 2) {
            args = __slice.call(arguments, 0, arguments.length - 1);
            cb = arguments[arguments.length - 1];
        } else {
            args = [];
            cb = arguments[0];
        }
        if (!cluster) {
            return cb(new Error("Connection pooling: Not able to find any cluster"));
        }

        //acquiring pool from cluster with min waiting queue
        cluster.acquire(function (err, connection, pool) {
            if(!!err)
                return cb(err);

            var thriftClient;
            var errorHandlers = getErrorHandlers(cb);

            addListeners(connection, errorHandlers);
            thriftClient = thrift.createClient(serviceClient, connection);
            args.push(releaseResources.bind(connection, cb, errorHandlers, pool));
            return thriftClient[methodName].apply(thriftClient, args);
        });
    }
}

/**
 *
 *
 * @param serviceClient
 * @param cluster
 * @return {*}
 */
module.exports = function (serviceClient, cluster) {
    //creating a clone of client
    var serviceClientClone = _.clone(serviceClient.Client.prototype);

    /*wrapping client method with our wrapper function so a to initiate pooling when
     *method is invoked
     * */
    return _.mapValues(serviceClientClone, wrapperFn.bind(serviceClient, cluster));
};