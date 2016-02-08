var zkHelper = require("./helper/zkHelper");

var factory = {};

factory.config = zkHelper.config;

factory.getService = zkHelper.getService;

module.exports = factory;
