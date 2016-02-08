# About

**Pooling thrift connection with zookeeper in distributed environment.**

A module that integrates zookeeper with thrift interfaces with connection pooling to make them more robust.
For pooling different server instances of a service, it uses [Node pool cluster](https://www.npmjs.com/package/node-pool-cluster).
[Node zookeeper client](https://www.npmjs.com/package/node-zookeeper-client) is used to connect to zookeeper and 
[Thrift](https://www.npmjs.com/package/thrift) for creating thrift connections.

## Table of contents

* [Installation](#installation)
* [Usage](#usage)
* [Config Option](#config-option)
* [History](#history)
* [License](#license) (The MIT License)

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

## Config Option

You can control some of the pooling options, similar to generic node pool.

```
  var thrift = require('thrift'),
    myServiceClient = require('./gen-nodejs/Service'),
    thriftZkClient = require('thrift-zookeeper-client'),
    zkProps = {
        zkUrl:"localhost:2181",
        zkPath: "/com/test/services/qa"
    };
    
    thriftZkClient.config({
        max: 5 //max connections in a pool, default is 3,
        min: 1 //min connections in a pool, default is 0,
        idleTimeoutMillis: 15000 //max milliseconds a resource can go unused before it should be destroyed (default 20000)
        refreshIdle: true, //boolean that specifies whether idle resources at or below the min threshold
                           //should be destroyed/re-created.  optional (default=true)
        reapIntervalMillis: 2000 //frequency to check for idle resources (default 1000),
    });
    
    thriftZkClient.getService(zkProps, "myService", myServiceClient, function(thriftMyServiceClient){
          //callback      
    });
```


## History

The history has been moved to the [CHANGELOG](ChangeLog.md)

## License

The MIT License (MIT)

Copyright (c) 2016 Kapil Joshi  <<kjoshi1988@gmail.com>>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.