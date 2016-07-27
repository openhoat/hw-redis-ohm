'use strict';

const util = require('util');
const _ = require('lodash');

class OhmError extends Error {
  constructor(message, extra) {
    super(message);
    this.name = this.constructor.name;
    if (typeof extra === 'undefined' && typeof message === 'object') {
      this.extra = message;
      this.message = this.message && this.message !== '' ? this.message : JSON.stringify(this.extra);
    } else {
      this.message = this.message && this.message !== '' ? this.message : message;
      this.extra = extra || this.extra;
    }
    this.message = this.message || '';
  }

  toString() {
    return util.format('%s: %s', this.name, this.message);
  }
}

class UnsupportedOhmError extends OhmError {
  constructor(...args) {
    super(...args);
    this.message = _.get(this, 'extra.cmd') ?
      util.format('unsupported operation "%s" error', this.extra.cmd) :
      'unknown unsupported operation error';
  }
}

class RedisOhmError extends OhmError {
  constructor(...args) {
    super(...args);
    this.message = _.get(this, 'extra.redisError') ?
      util.format('redis error "%s"', this.extra.redisError) :
      'unknown redis error';
  }
}

class EntityError extends OhmError {
  constructor(...args) {
    super(...args);
    this.message = _.get(this, 'extra.type') ?
      util.format('entity "%s" error', this.extra.type) :
      'unknown entity error';
  }
}

class EntitySchemaNotFoundError extends EntityError {
  constructor(...args) {
    super(...args);
    this.message = _.get(this, 'extra.schema') ?
      util.format('entity schema "%s" not found', this.extra.schema) :
      'unknown entity schema not found';
  }
}
class EntityConflictError extends EntityError {
  constructor(...args) {
    super(...args);
    this.message = this.extra ?
      util.format('entity "%s" conflict for "%s" with value "%s"',
        this.extra.type,
        this.extra.attrName,
        this.extra.attrValue
      ) :
      'unknown entity conflict';
  }
}

class EntityNotFoundError extends EntityError {
  constructor(...args) {
    super(...args);
    this.message = this.extra ?
      util.format('entity "%s" not found for "%s" with value "%s"',
        this.extra.type,
        this.extra.attrName,
        this.extra.attrValue
      ) :
      'unknown entity not found';
  }
}

class EntityValidationError extends EntityError {
  constructor(...args) {
    super(...args);
    if (_.get(this, 'extra.schemaErrors')) {
      this.message = util.format('entity "%s" validation failed because of schema error "%s"',
        this.extra.type,
        this.extra.schemaErrors.map(el => el.desc).join(',')
      );
    } else if (_.get(this, 'extra.attrName')) {
      this.message = util.format('entity "%s" validation failed for "%s" with value "%s"',
        this.extra.type,
        this.extra.attrName,
        this.extra.attrValue
      );
    } else {
      this.message = 'unknown entity validation failed';
    }
  }
}

const errors = {
  OhmError,
  UnsupportedOhmError,
  RedisOhmError,
  EntityError,
  EntitySchemaNotFoundError,
  EntityConflictError,
  EntityNotFoundError,
  EntityValidationError,
  reset: () => {
    for (const key in errors) {
      if (!errors.hasOwnProperty(key)) {
        continue;
      }
      const match = key.match(/^throw[A-Z][a-z]+.*$/);
      if (match) {
        delete errors[key];
      }
    }
  },
  init: () => {
    errors.reset();
    for (const key in errors) {
      if (!errors.hasOwnProperty(key)) {
        continue;
      }
      const match = key.match(/(.*)Error$/);
      if (!match) {
        continue;
      }
      const errorClassName = key;
      const suffix = match[1];
      errors[`throw${suffix}`] = (...args) => {
        throw new errors[errorClassName](...args);
      };
    }
  }
};

exports = module.exports = errors;
