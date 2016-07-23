const Promise = require('bluebird');
const logger = require('hw-logger');
const ohm = require('../lib/ohm');
const log = logger.log;

const that = {
  cleanStore: () => ohm.cli.keysAsync(ohm.toHash('*'))
    .then(keys => {
      logger.enabledLevels.trace && log.trace('purging keys :', keys);
      return Promise.map(keys, key => ohm.cli.delAsync(key));
    })
};

exports = module.exports = that;