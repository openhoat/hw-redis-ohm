const p = require('hw-promise')
  , logger = require('hw-logger')
  , ohm = require('../lib/ohm')
  , log = logger.log;

const that = {
  cleanStore: () => ohm.cli.keysAsync(ohm.toHash('*'))
    .then(keys => {
      logger.enabledLevels.trace && log.trace('purging keys :', keys);
      return p.map(keys, key => ohm.cli.delAsync(key));
    })
};

exports = module.exports = that;