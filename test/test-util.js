'use strict';

var p = require('hw-promise')
  , logger = require('hw-logger')
  , ohm = require('../lib/ohm')
  , log = logger.log
  , that;

that = {
  cleanStore: function () {
    var redisCli = ohm.cliAsync;
    return redisCli.keysAsync(ohm.toHash('*'))
      .then(function (keys) {
        logger.enabledLevels.trace && log.trace('purging keys :', keys);
        return p.map(keys, redisCli.delAsync.bind(redisCli));
      });
  }
};

exports = module.exports = that;