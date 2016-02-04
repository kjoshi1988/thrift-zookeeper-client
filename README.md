# About

**Pooling thrift connection with zookeeper in distributed environment.**

A module that integrates zookeeper with thrift interfaces with connection pooling logic to make them more robust.

## Installation

```
$ npm install thrift-zookeeper-client
```

## Usage

```javascript
var thrift = require('thrift'),
  myServiceClient = require('./gen-nodejs/Service'),
  thriftZkClient = require('thrift-zookeeper-client'),
  zkProps = {
      zkUrl:"localhost:2181",
      zkPath: "/com/test/services/qa"
  };
  
  thriftZkClient.getService(zkProps, "myService", myServiceClient, function(thriftMyServiceClient){
        //callback      
  });
  
```
