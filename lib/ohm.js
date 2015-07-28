'use strict';

var util = require('util')
  , redis = require('redis')
  , _ = require('lodash')
  , JaySchema = require('jayschema')
  , e = require('hw-error')
  , p = require('hw-promise')
  , hat = require('hat')
  , rack = hat.rack()
  , logger = require('hw-logger')
  , log = logger.log
  , js = new JaySchema()
  , validate = p.promisify(js.validate, js)
  , Entity, ohm;

ohm = {
  config: {
    host: process.env['REDIS_HOST'] || '127.0.0.1',
    port: process.env['REDIS_PORT'] ? parseInt(process.env['REDIS_PORT']) : 6379,
    db: 0,
    prefix: 'hw-redis-ohm',
    idsHashPrefix: 'id',
    idxHashPrefix: 'idx',
    schemaMetaPrefix: 'meta',
    entityClassLazyInit: true
  },
  entityClasses: {},
  patterns: {
    id: '^[0-9]+$'
  },
  incrementalIdSchema: {
    type: 'string',
    pattern: '^[0-9]+$'
  },
  incrementalNullableIdSchema: {
    type: ['string', 'null'],
    pattern: '^[0-9]+$'
  },
  init: function (opt) {
    _.extend(ohm.config, _.omit(opt, 'schemas'));
    opt = opt || {};
    ohm.schemas = {};
    _.forIn(opt.schemas, function (schemaSpec, name) {
      var schemaMeta, schemaName, defaultSchema, schema, schemaId, linkNames, dbDefaults;
      schemaMeta = schemaSpec[ohm.config.schemaMetaPrefix];
      if (!schemaMeta) {
        return;
      }
      schemaName = _.camelCase(name);
      defaultSchema = {properties: {}};
      if (schemaMeta.links) {
        linkNames = schemaMeta.links.map(function (link) {
          return link.as;
        });
        schemaMeta.links.forEach(function (link) {
          if (link.type === 'hasMany') {
            defaultSchema.properties[link.as] = {
              type: 'array',
              items: ohm.incrementalNullableIdSchema
            };
          } else {
            defaultSchema.properties[link.as] = ohm.incrementalNullableIdSchema;
          }
        });
      }
      defaultSchema[ohm.config.schemaMetaPrefix] = {
        operations: {db: {}}
      };
      schema = _.defaultsDeep(defaultSchema, schemaSpec);
      ohm.schemas[schemaName] = schema;
      schemaId = ohm.getSchemaId(schemaName);
      if (typeof schema.properties[schemaId] === 'undefined') {
        schema.properties[schemaId] = ohm.incrementalIdSchema;
      }
      dbDefaults = {
        new: {
          excludeProperties: [schemaId]
        },
        save: {
          required: [schemaId],
          excludeProperties: linkNames,
          minProperties: 2
        },
        get: {}
      };
      schemaMeta = schema[ohm.config.schemaMetaPrefix];
      _.defaultsDeep(schemaMeta.operations.db, dbDefaults);
      _.forIn(schemaMeta.operations, function (value, type) {
        _.forIn(value, function (value, operation) {
          var defaultProps;
          defaultProps = {
            title: util.format(schema.title, type, operation),
            type: schema.type
          };
          if (value.includeProperties) {
            defaultProps.properties = _.pick(schema.properties, value.includeProperties);
            delete value.includeProperties;
          } else {
            defaultProps.properties = _.cloneDeep(schema.properties);
          }
          if (value.excludeProperties) {
            defaultProps.properties = _.omit(schema.properties, value.excludeProperties);
            delete value.excludeProperties;
          }
          _.defaultsDeep(value, defaultProps);
          if (value.extraProperties) {
            value.properties = _.cloneDeep(value.properties);
            _.merge(value.properties, value.extraProperties);
            delete value.extraProperties;
          }
        });
      });
    });
    Entity = require('./entity');
    if (ohm.config.entityClassLazyInit) {
      _.forIn(ohm.schemas, function (schema, name) {
        var entityClassName, entityClass;
        entityClass = ohm.getEntityClass(name);
        entityClassName = _.camelCase(name);
        entityClassName = entityClassName.substring(0, 1).toUpperCase() + entityClassName.substring(1);
        if (typeof ohm[entityClassName] === 'undefined') {
          logger.enabledLevels.debug && log.debug('entityClassName :', entityClassName);
          ohm[entityClassName] = entityClass;
        } else {
          logger.enabledLevels.debug && log.debug('ohm already has a property named "%s" : cannot define entity class', entityClassName);
        }
      });
    }
    ohm.cli = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
    ohm.initialized = true;
  },
  start: function (opt, cb) {
    if (ohm.started) {
      return p.resolve(false);
    }
    if (typeof cb === 'undefined' && typeof opt === 'function') {
      cb = opt;
      opt = null;
    }
    if (!ohm.initialized) {
      ohm.init(opt);
    }
    cb = cb || _.noop;
    ohm.cliAsync = p.promisifyAll(ohm.cli);
    return ohm.cliAsync.selectAsync(ohm.config.db)
      .then(function () {
        ohm.started = true;
        logger.enabledLevels.info && log.info('redis ohm client started');
        return true;
      })
      .nodeify(cb);
  },
  stop: function (cb) {
    cb = cb || _.noop;
    return p.do(
      function () {
        if (!ohm.started) {
          return false;
        }
        return (typeof ohm.cliAsync.quitAsync === 'function' ? ohm.cliAsync.quitAsync() : p.resolve())
          .then(function () {
            ohm.started = false;
            ohm.initialized = false;
            logger.enabledLevels.info && log.info('redis ohm client stopped');
            return true;
          });
      })
      .nodeify(cb);
  },
  createEntityClass: function (schemaName) {
    var defaults, clazz, schema;
    clazz = function () {
      Entity.prototype.constructor.apply(this, arguments);
    };
    _.forIn(Entity, function (value, key) {
      clazz[key] = value;
    });
    defaults = {
      constructor: clazz,
      type: {
        configurable: true,
        enumerable: true,
        value: schemaName,
        writable: true
      }
    };
    clazz.prototype = Object.create(Entity.prototype, defaults);
    schema = ohm.getSchema(schemaName);
    clazz.type = schemaName;
    (function defineIndexes() {
      clazz.indexes = {};
      if (schema.indexes) {
        schema.indexes.forEach(function (index) {
          if (Array.isArray(index.name)) {
            clazz.indexes[index.name.join(',')] = {
              name: index.name,
              unique: index.unique,
              key: ohm.toHash(ohm.config.idxHashPrefix, schemaName, index.name, _.fill(Array(index.name.length), '%s')),
              getValue: index.getValue
            };
          } else {
            clazz.indexes[index.name] = {
              name: index.name,
              unique: index.unique,
              key: ohm.toHash(ohm.config.idxHashPrefix, schemaName, index.name, '%s'),
              getValue: index.getValue
            };
          }
        });
      }
    })();
    (function defineLinks() {
      clazz.links = {};
      if (schema.links) {
        schema.links.forEach(function (link) {
          var reverseSchema, reverseLink;
          reverseSchema = ohm.getSchema(link.target);
          if (reverseSchema && reverseSchema.links) {
            reverseLink = _.first(_.filter(reverseSchema.links, {target: schemaName, foreignKey: link.as}));
          }
          clazz.links[link.as] = {
            as: link.as,
            target: link.target,
            unique: link.type === 'hasOne',
            key: ohm.toHash(ohm.config.idxHashPrefix, schemaName, link.as, '%s')
          };
          if (reverseLink) {
            clazz.links[link.as].reverseKey = ohm.toHash(ohm.config.idxHashPrefix, link.target, reverseLink.as, '%s');
            clazz.links[link.as].reverseUnique = reverseLink.type === 'hasOne';
          }
        });
      }
    })();
    (function defineIsObject() {
      var schema = ohm.getSchema(clazz.type, 'save')
        , idName = ohm.getSchemaId(clazz.type)
        , withoutArgs
        , dataProps;
      withoutArgs = [Object.keys(schema.properties)];
      withoutArgs = withoutArgs.concat(Object.keys(clazz.links));
      withoutArgs.push(idName);
      dataProps = _.without.apply(_, withoutArgs);
      clazz.isObject = dataProps.length > 1 || _.first(dataProps) !== 'value';
    })();
    return clazz;
  },
  exec: function (cmd, k, values) {
    var args, promise;
    args = Array.prototype.slice.call(arguments);
    args.shift();
    promise = p.do(function () {
      return ohm.cliAsync[cmd + 'Async'].apply(ohm.cliAsync, args);
    });
    if (logger.enabledLevels.redis) {
      promise = promise
        .then(function (result) {
          log.redis('%s %s%s :', cmd.toUpperCase(), k, values ? ' ' + JSON.stringify(values) : '', JSON.stringify(result));
          return result;
        })
        .catch(function (error) {
          log.redis('Error %s %s%s', cmd.toUpperCase(), k, values ? ' ' + JSON.stringify(values) : '');
          log.redis('error stack :', error instanceof Error ? error.stack : error);
          throw error;
        });
    }
    return promise;
  },
  filterProperties: function (data, namespace, type, op) {
    var schema, propFilter, result;
    if (typeof op === 'undefined' && typeof type !== 'undefined') {
      op = type;
      type = 'db';
    }
    namespace = _.camelCase(namespace);
    schema = ohm.getSchema(namespace, type, op);
    if (!schema || !schema.properties) {
      logger.enabledLevels.debug && log.debug('properties for "%s/%s/%s" not found : ignore filtering', namespace, type, op);
      return data;
    }
    propFilter = Object.keys(schema.properties);
    if (!propFilter.length) {
      return data;
    }
    ohm.setSchemaDefaults(data, namespace, type, op);
    logger.enabledLevels.trace && log.trace('filtering instance with "%s/%s/%s"', namespace, type, op);
    result = _.pick(data, propFilter);
    return result;
  },
  fromRedis: function (entity, namespace, operation) {
    var schema = ohm.getSchema(namespace, operation)
      , type, result;
    result = {};
    _.forIn(entity, function (value, key) {
      if (!schema.properties[key]) {
        return;
      }
      type = schema.properties[key].type;
      if (Array.isArray(type)) {
        type = _.first(type);
      }
      switch (type) {
        case 'object':
        case 'array':
          result[key] = JSON.parse(value);
          break;
        case 'boolean':
          result[key] = value === 'true';
          break;
        case 'number':
          result[key] = parseFloat(value);
          break;
        case 'integer':
          result[key] = parseInt(value);
          break;
        default:
          result[key] = value;
          break;
      }
    });
    return result;
  },
  generateId: function (type) {
    if (type === 'date') {
      return (function () {
        function rnd() {
          return Math.floor(Math.random() * 1e9).toString(36);
        }

        return (+new Date()).toString(36) + rnd() + rnd();
      })();
    }
    return rack();
  },
  getEntityClass: function (schemaName) {
    if (!ohm.entityClasses[schemaName]) {
      ohm.entityClasses[schemaName] = ohm.createEntityClass(schemaName);
    }
    return ohm.entityClasses[schemaName];
  },
  getSchema: function (namespace, type, op) {
    var key, schema;
    if (typeof namespace === 'undefined') {
      return ohm.schemas;
    }
    if (typeof op === 'undefined' && typeof type !== 'undefined') {
      op = type;
      type = 'db';
    }
    namespace = _.camelCase(namespace);
    key = _.compact([namespace, ohm.config.schemaMetaPrefix, type && ['operations', type].join('.'), type && op]).join('.');
    schema = _.get(ohm.schemas, key);
    if (!schema) {
      e.throw('INTERNAL', util.format('schema "%s" not found', _.compact(arguments).join('/')));
    }
    return schema;
  },
  getSchemaId: function (namespace, type) {
    return ohm.getSchema(namespace, type).id || 'id';
  },
  setSchemaDefaults: function (data, namespace, type, op) {
    var schema;
    if (typeof data !== 'object') {
      return;
    }
    namespace = _.camelCase(namespace);
    schema = ohm.getSchema(namespace, type, op);
    if (!schema) {
      logger.enabledLevels.debug && log.debug('schema for "%s/%s/%s" not found : ignore defaults', namespace, type, op);
      return data;
    }
    logger.enabledLevels.debug && log.debug('setting defaults with schema "%s/%s/%s"', namespace, type, op);
    _.forIn(schema.properties, function (value, key) {
      if (value.hasOwnProperty('default') && typeof data[key] === 'undefined') {
        data[key] = typeof value.default === 'function' ? value.default(data) : value.default;
      }
    });
    return data;
  },
  toHash: function (o) {
    var hashes, result;
    if (arguments.length > 1) {
      hashes = Array.prototype.slice.call(arguments);
    } else if (typeof o === 'string') {
      hashes = o.split(':');
    }
    hashes.splice(0, 0, ohm.config.prefix);
    result = _.compact(hashes.map(function (item) {
      return Array.isArray(item) ? item.join(':') : item;
    })).join(':');
    return result;
  },
  toRedis: function (data, namespace) {
    var schema = ohm.getSchema(namespace, 'save')
      , type, result;
    result = {};
    _.forIn(data, function (value, key) {
      if (!schema.properties[key]) {
        return;
      }
      type = schema.properties[key].type;
      if (Array.isArray(type)) {
        type = _.first(type);
      }
      switch (type) {
        case 'object':
        case 'array':
          result[key] = JSON.stringify(value);
          break;
        case 'boolean':
          result[key] = value;
          break;
        default:
          result[key] = value ? value.toString() : null;
          break;
      }
    });
    if (Object.keys(result).length === 1 && typeof result.value !== 'undefined') {
      return result.value;
    }
    return result;
  },
  validateSchema: function (data, namespace, type, op, cb) {
    var schema;
    cb = cb || _.noop;
    if (typeof op === 'undefined' && typeof type !== 'undefined') {
      op = type;
      type = 'db';
    }
    namespace = _.camelCase(namespace);
    schema = ohm.getSchema(namespace, type, op);
    if (!schema) {
      logger.enabledLevels.debug && log.debug('schema for "%s/%s/%s" not found : ignore validation', namespace, type, op);
      return p.resolve(false);
    }
    logger.enabledLevels.debug && log.debug('validating instance with schema "%s/%s/%s"', namespace, type, op);
    ohm.filterProperties(data, namespace, type, op);
    return validate(data, schema).nodeify(cb);
  }
};

exports = module.exports = ohm;