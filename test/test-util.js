'use strict';

var p = require('hw-promise')
  , logger = require('hw-logger')
  , ohm = require('../lib/ohm')
  , log = logger.log
  , that;

that = {
  cleanStore: function () {
    return ohm.cli.keysAsync(ohm.toHash('*'))
      .then(function (keys) {
        logger.enabledLevels.trace && log.trace('purging keys :', keys);
        return p.map(keys, ohm.cli.delAsync.bind(ohm.cli));
      });
  }
};

exports = module.exports = that;