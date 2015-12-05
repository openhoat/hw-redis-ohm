'use strict';

/*jshint -W098: false*/

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
  , validate = p.promisify(js.validate, {context: js})
  , Entity, ohm;

p.promisifyAll(redis.RedisClient.prototype);
p.promisifyAll(redis.Multi.prototype);

ohm = {
  config: {
    host: process.env['REDIS_HOST'] || '127.0.0.1',
    port: process.env['REDIS_PORT'] ? parseInt(process.env['REDIS_PORT']) : 6379,
    db: 0,
    prefix: 'hw-redis-ohm',
    idsHashPrefix: 'id',
    idxHashPrefix: 'idx',
    schemaMetaPrefix: 'meta',
    entityClassLazyInit: true,
    schemas: {}
  },
  entityClasses: {},
  incrementalIdSchema: {
    type: 'string',
    pattern: '^[0-9]+$'
  },
  incrementalNullableIdSchema: {
    type: ['string', 'null'],
    pattern: '^[0-9]+$'
  },
  patterns: {
    id: '^[0-9]+$'
  },
  init: function (opt) {
    logger.enabledLevels.debug && log.debug('initializing redis ohm client');
    opt = opt || {};
    _.extend(ohm.config, opt);
    ohm.initSchemas();
    ohm.cli = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
    if (ohm.config.roHost) {
      ohm.roCli = redis.createClient(ohm.config.roPort || ohm.config.port, ohm.config.roHost, ohm.config.roOptions || ohm.config.options);
    }
    ohm.initialized = true;
    logger.enabledLevels.debug && log.debug('redis ohm client initialized');
  },
  initSchemas: function () {
    ohm.entityClasses = {};
    ohm.schemas = {};
    _.forIn(ohm.config.schemas, function (schemaSpec, schemaName) {
      var metaPrefix, meta, schema, schemaId, linkNames, key;
      logger.enabledLevels.debug && log.debug('initializing schema "%s"', schemaName);
      if (!schemaSpec[ohm.config.schemaMetaPrefix]) {
        logger.enabledLevels.debug && log.debug('schema "%s" has no meta : ignore', schemaName);
        return;
      }
      metaPrefix = ohm.config.schemaMetaPrefix;
      ohm.schemas[schemaName] = schema = _.cloneDeep(schemaSpec);
      schema.title = util.format(schema.title, 'main', 'default');
      meta = _.defaultsDeep(schema[metaPrefix], {
        operations: {db: {new: {}, save: {}, get: {}}}
      });
      _.forIn(meta.operations, function (value, type) {
        _.forIn(value, function (value, operation) {
          key = ['operations', type, operation, 'title'].join('.');
          logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', key, schemaName);
          _.set(meta, key, util.format(schemaSpec.title, type, operation));
          key = ['operations', type, operation, 'type'].join('.');
          logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', key, schemaName);
          _.set(meta, key, schemaSpec.type);
        });
      });
      schemaId = ohm.getSchemaId(schemaName);
      if (typeof schema.properties[schemaId] === 'undefined') {
        logger.enabledLevels.trace && log.trace('setting default property "%s" to schema "%s"', schemaId, schemaName);
        schema.properties[schemaId] = ohm.incrementalIdSchema;
      }
      _.forIn(schemaSpec.properties, function (property, name) {
        logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', name, schemaName);
        schema.properties[name] = schemaSpec.properties[name];
      });
      logger.enabledLevels.trace && log.trace('setting meta datas to schema "%s"', schemaName);
      if (schemaSpec[ohm.config.schemaMetaPrefix].links) {
        linkNames = schemaSpec[ohm.config.schemaMetaPrefix].links.map(function (link) {
          return link.as;
        });
        schemaSpec[ohm.config.schemaMetaPrefix].links.forEach(function (link) {
          if (typeof schema.properties[link.as] === 'undefined') {
            logger.enabledLevels.trace && log.trace('setting link property "%s" to schema "%s"', link.as, schemaName);
            if (link.type === 'hasMany') {
              schema.properties[link.as] = {
                type: 'array',
                items: ohm.incrementalNullableIdSchema
              };
            } else {
              schema.properties[link.as] = ohm.incrementalNullableIdSchema;
            }
          }
        });
      }
      key = 'operations.db.new.excludeProperties';
      if (typeof _.get(meta, key) === 'undefined') {
        logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
        _.set(meta, key, [schemaId]);
      }
      key = 'operations.db.save.required';
      if (typeof _.get(meta, key) === 'undefined') {
        logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
        _.set(meta, key, [schemaId]);
      }
      key = 'operations.db.save.excludeProperties';
      if (typeof _.get(meta, key) === 'undefined' && linkNames && linkNames.length) {
        logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
        _.set(meta, key, linkNames);
      }
      key = 'operations.db.save.minProperties';
      if (typeof _.get(meta, key) === 'undefined') {
        logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
        _.set(meta, key, 2);
      }
      _.forIn(meta.operations, function (value, type) {
        _.forIn(value, function (value, operation) {
          var key;
          key = ['operations', type, operation, 'properties'].join('.');
          if (value.includeProperties) {
            _.set(meta, key, _.pick(schema.properties, value.includeProperties));
            delete value.includeProperties;
          } else {
            _.set(meta, key, _.cloneDeep(schema.properties));
          }
          if (value.excludeProperties) {
            _.set(meta, key, _.omit(_.get(meta, key), value.excludeProperties));
            delete value.excludeProperties;
          }
          if (value.extraProperties) {
            _.merge(_.get(meta, key), value.extraProperties);
            delete value.extraProperties;
          }
        });
      });
    });
    Entity = require('./entity');
    if (ohm.config.entityClassLazyInit) {
      _.forIn(ohm.schemas, function (schema, name) {
        logger.enabledLevels.debug && log.debug('registering entity class for schema "%s"', name);
        ohm.getEntityClass(name);
      });
    }
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
    return ohm.cli.selectAsync(ohm.config.db)
      .then(function () {
        if (ohm.roCli) {
          return ohm.roCli.selectAsync(ohm.config.db);
        }
      })
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
        return (typeof ohm.cli.quitAsync === 'function' ? ohm.cli.quitAsync() : p.resolve())
          .then(function () {
            if (ohm.roCli) {
              return (typeof ohm.roCli.quitAsync === 'function' ? ohm.roCli.quitAsync() : p.resolve());
            }
          })
          .then(function () {
            if (!ohm.subCli) {
              return;
            }
            return p.map(Object.keys(ohm.subCli), function (channel) {
              logger.enabledLevels.debug && log.debug('unsubscribe channel %s', channel);
              return ohm.unsubscribe(channel);
            });
          })
          .then(function () {
            ohm.started = false;
            ohm.initialized = false;
            logger.enabledLevels.info && log.info('redis ohm client stopped');
            return true;
          });
      })
      .nodeify(cb);
  },
  createLocalMulti: function (multi) {
    return multi || ohm.multi();
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
    // TODO : http://redis.io/commands/command#examples
    var args, promise;
    args = Array.prototype.slice.call(arguments);
    args.shift();
    _.remove(args, _.isUndefined);
    if (typeof ohm.cli[cmd + 'Async'] !== 'function') {
      e.throwServiceUnavailable('unsupported operation :', cmd);
    }
    promise = ohm.cli[cmd + 'Async'].apply(ohm.cli, args);
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
  execMulti: function (multi, cmd, k, values) {
    var args;
    args = Array.prototype.slice.call(arguments);
    args.shift();
    args.shift();
    _.remove(args, _.isUndefined);
    if (typeof multi[cmd] !== 'function') {
      e.throwServiceUnavailable('unsupported operation :', cmd);
    }
    return new p(function (resolve) {
      resolve(multi[cmd].apply(multi, args));
    });
  },
  multi: function () {
    return ohm.cli.multi();
  },
  processLocalMulti: function (localMulti, multi, result) {
    var isLocal=typeof multi!=='undefined'||localMulti !== multi;
    log.warn('localMulti :', typeof localMulti);
    log.warn('multi :', typeof multi);
    log.warn('isLocal :', isLocal);
    return isLocal? ohm.processMulti(localMulti) : result;
  },
  processMulti: function (multi) {
    var promise, queue;
    promise = multi.execAsync();
    queue = multi.queue.toArray();
    if (logger.enabledLevels.redis) {
      promise = promise
        .then(function (result) {
          queue.forEach(function (item, index) {
            var cmd = item[0]
              , k = item[1]
              , values;
            item.shift();
            item.shift();
            values = item;
            log.redis('%s %s%s :', cmd.toUpperCase(), k, values ? ' ' + JSON.stringify(values) : '', JSON.stringify(result[index]));
          });
          return result;
        })
        .catch(function (error) {
          log.redis('Error in %s', queue);
          log.redis('error stack :', error instanceof Error ? error.stack : error);
          throw error;
        });
    }
    return promise;
  },
  publish: function (channel, message) {
    return p.do(function () {
      ohm.pubCli = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
      return new p(function (resolve) {
        ohm.pubCli.publish(channel, message);
        resolve();
      });
    });
  },
  subscribe: function (channel, cb) {
    var subCli;
    return p.do(function () {
      ohm.subCli = ohm.subCli || {};
      subCli = ohm.subCli[channel] = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
      return new p(function (resolve) {
        subCli.on('subscribe', function (channel, count) {
          resolve([channel, count]);
        });
        subCli.on('message', cb);
        subCli.subscribe(channel);
      });
    });
  },
  unsubscribe: function (channel) {
    var subCli;
    return p.do(function () {
      subCli = ohm.subCli[channel];
      subCli.unsubscribe();
      subCli.end();
      delete ohm.subCli[channel];
    });
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
    var entityClassName, entityClass;
    entityClassName = _.camelCase(schemaName);
    entityClassName = entityClassName.substring(0, 1).toUpperCase() + entityClassName.substring(1);
    entityClass = ohm.entityClasses[schemaName];
    if (!entityClass) {
      entityClass = ohm.entityClasses[schemaName] = ohm.entityClasses[entityClassName] = ohm.createEntityClass(schemaName);
    }
    return entityClass;
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