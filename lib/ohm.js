const util = require('util')
  , assert = require('assert')
  , redis = require('redis')
  , _ = require('lodash')
  , JaySchema = require('jayschema')
  , p = require('hw-promise')
  , hat = require('hat')
  , rack = hat.rack()
  , logger = require('hw-logger')
  , log = logger.log
  , js = new JaySchema()
  , e = require('./errors')
  , validate = p.promisify(js.validate, {context: js});

let Entity;

p.promisifyAll(redis.RedisClient.prototype);
p.promisifyAll(redis.Multi.prototype);

const ohm = {
  config: {
    host: process.env['REDIS_HOST'] || '127.0.0.1',
    port: process.env['REDIS_PORT'] ? parseInt(process.env['REDIS_PORT']) : 6379,
    db: 0,
    prefix: 'hw-redis-ohm',
    idsHashPrefix: 'id',
    idxHashPrefix: 'idx',
    schemaMetaPrefix: 'meta',
    schemas: {},
    parser: 'hiredis',
    entityClassLazyInit: true
  },
  e,
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
  init: (opt = {}) => {
    logger.enabledLevels.debug && log.debug('initializing redis ohm client');
    _.extend(ohm.config, opt);
    e.init();
    ohm.initSchemas();
    ohm.initialized = true;
    logger.enabledLevels.debug && log.debug('redis ohm client initialized');
  },
  initSchemas: () => {
    ohm.entityClasses = {};
    ohm.schemas = {};
    _.forIn(ohm.config.schemas, (schemaSpec, schemaName) => {
      let linkNames, key;
      logger.enabledLevels.debug && log.debug('initializing schema "%s"', schemaName);
      if (!schemaSpec[ohm.config.schemaMetaPrefix]) {
        logger.enabledLevels.debug && log.debug('schema "%s" has no meta : ignore', schemaName);
        return;
      }
      const metaPrefix = ohm.config.schemaMetaPrefix;
      const schema = _.cloneDeep(schemaSpec);
      ohm.schemas[schemaName] = schema;
      schema.title = util.format(schema.title, 'main', 'default');
      const meta = _.defaultsDeep(schema[metaPrefix], {
        operations: {db: {new: {}, save: {}, get: {}}}
      });
      _.forIn(meta.operations, (value, type) => {
        _.forIn(value, (value, operation) => {
          key = ['operations', type, operation, 'title'].join('.');
          logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', key, schemaName);
          _.set(meta, key, util.format(schemaSpec.title, type, operation));
          key = ['operations', type, operation, 'type'].join('.');
          logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', key, schemaName);
          _.set(meta, key, schemaSpec.type);
        });
      });
      const schemaId = ohm.getSchemaId(schemaName);
      if (typeof schema.properties[schemaId] === 'undefined') {
        logger.enabledLevels.trace && log.trace('setting default property "%s" to schema "%s"', schemaId, schemaName);
        schema.properties[schemaId] = ohm.incrementalIdSchema;
      }
      _.forIn(schemaSpec.properties, (property, name) => {
        logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', name, schemaName);
        schema.properties[name] = schemaSpec.properties[name];
      });
      logger.enabledLevels.trace && log.trace('setting meta datas to schema "%s"', schemaName);
      if (schemaSpec[ohm.config.schemaMetaPrefix].links) {
        linkNames = schemaSpec[ohm.config.schemaMetaPrefix].links.map(link => link.as);
        schemaSpec[ohm.config.schemaMetaPrefix].links.forEach(link => {
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
      _.forIn(meta.operations, (value, type) => {
        _.forIn(value, (value, operation) => {
          const key = ['operations', type, operation, 'properties'].join('.');
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
      _.forIn(ohm.schemas, (schema, name) => {
        logger.enabledLevels.debug && log.debug('registering entity class for schema "%s"', name);
        ohm.getEntityClass(name);
      });
    }
  },
  start: (opt, cb) => {
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
    ohm.cli = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
    if (ohm.config.roHost) {
      ohm.roCli = redis.createClient(ohm.config.roPort || ohm.config.port, ohm.config.roHost, ohm.config.roOptions || ohm.config.options);
    }
    return ohm.cli.selectAsync(ohm.config.db)
      .then(() => {
        if (ohm.roCli) {
          return ohm.roCli.selectAsync(ohm.config.db);
        }
      })
      .then(() => {
        ohm.started = true;
        logger.enabledLevels.info && log.info('redis ohm client started');
        return true;
      })
      .nodeify(cb);
  },
  stop: (cb = _.noop) => p.do(
    () => {
      if (!ohm.started) {
        return false;
      }
      return p.do(
        () => {
          if (!ohm.pubCli) {
            return;
          }
          logger.enabledLevels.debug && log.debug('destroying pub cli');
          return ohm.unpublish();
        },
        () => {
          if (!ohm.subCli) {
            return;
          }
          return p.map(Object.keys(ohm.subCli), channel => {
            logger.enabledLevels.debug && log.debug('unsubscribe channel %s', channel);
            return ohm.unsubscribe(channel);
          });
        },
        () => {
          logger.enabledLevels.debug && log.debug('closing cli');
          return ohm.cli.quitAsync();
        },
        () => {
          if (ohm.roCli) {
            logger.enabledLevels.debug && log.debug('closing readonly cli');
            return ohm.roCli.quitAsync();
          }
        },
        () => {
          ohm.started = false;
          ohm.initialized = false;
          logger.enabledLevels.info && log.info('redis ohm client stopped');
          return true;
        });
    })
    .nodeify(cb),
  createLocalMulti: multi => multi || ohm.multi(),
  createEntityClass: schemaName => {
    /*
     const clazz = function () {
     Entity.prototype.constructor.apply(this, arguments);
     };
     _.forIn(Entity, (value, key) => {
     clazz[key] = value;
     });
     const defaults = {
     constructor: clazz,
     type: {
     configurable: true,
     enumerable: true,
     value: schemaName,
     writable: true
     }
     };
     clazz.prototype = Object.create(Entity.prototype, defaults);
     */
    const schema = ohm.getSchema(schemaName);
    const clazz = class extends Entity {
      static get name() {
        return 'Entity' + schemaName.substring(0, 1).toUpperCase() + _.camelCase(schemaName.substring(1));
      }

      static get type() {
        return schemaName;
      }
    };
    //clazz.type = schemaName;
    const defineIndexes = () => {
      clazz.indexes = {};
      if (schema.indexes) {
        schema.indexes.forEach(index => {
          if (Array.isArray(index.name)) {
            clazz.indexes[index.name.join(',')] = {
              name: index.name,
              unique: index.unique,
              key: ohm.toHash(ohm.config.idxHashPrefix, schemaName, index.name, _.fill(new Array(index.name.length), '%s')),
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
    };
    defineIndexes();
    const defineLinks = () => {
      clazz.links = {};
      if (schema.links) {
        schema.links.forEach(link => {
          let reverseLink;
          const reverseSchema = ohm.getSchema(link.target);
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
    };
    defineLinks();
    const defineIsObject = () => {
      const saveSchema = ohm.getSchema(clazz.type, 'save')
        , idName = ohm.getSchemaId(clazz.type);
      let withoutArgs = [Object.keys(saveSchema.properties)];
      withoutArgs = withoutArgs.concat(Object.keys(clazz.links));
      withoutArgs.push(idName);
      const dataProps = _.without(...withoutArgs);
      clazz.isObject = dataProps.length > 1 || _.first(dataProps) !== 'value';
    };
    defineIsObject();
    return clazz;
  },
  exec: (cmd, ...args) => {
    // TODO : http://redis.io/commands/command#examples
    _.remove(args, _.isUndefined);
    const k = args[1];
    const values = args[2];
    return p.do(() => {
      let promise;
      if (typeof ohm.cli[cmd + 'Async'] !== 'function') {
        e.throwUnsupportedOhm({cmd});
      }
      promise = ohm.cli[cmd + 'Async'](...args);
      if (logger.enabledLevels.redis) {
        promise = promise
          .then(result => {
            log.redis('%s %s%s :', cmd.toUpperCase(), k, values ? ' ' + JSON.stringify(values) : '', JSON.stringify(result));
            return result;
          });
      }
      return promise.catch(err => {
        if (logger.enabledLevels.redis) {
          log.redis('Error %s %s%s', cmd.toUpperCase(), k, values ? ' ' + JSON.stringify(values) : '');
          log.redis('error stack :', err instanceof Error ? err.stack : err);
        }
        e.throwRedisOhm({redisError: err});
      });
    });
  },
  execMulti: (multi, cmd, ...args) => {
    _.remove(args, _.isUndefined);
    return new p(resolve => {
      if (typeof multi[cmd] !== 'function') {
        e.throwUnsupportedOhm({cmd});
      }
      resolve(multi[cmd](...args));
    });
  },
  multi: () => ohm.cli.multi(),
  processLocalMulti: (localMulti, multi, result) => {
    const isLocal = typeof multi !== 'undefined' || localMulti !== multi;
    return p.do(() => {
      if (result === localMulti || result === multi) {
        if (isLocal) {
          return ohm.processMulti(localMulti).return();
        }
      } else {
        return isLocal ? ohm.processMulti(localMulti).return(result) : result;
      }
    });
  },
  processMulti: multi => {
    let promise = multi.execAsync();
    const queue = multi.queue.toArray();
    if (logger.enabledLevels.redis) {
      promise = promise
        .then(result => {
          queue.forEach((item, index) => {
            const cmd = item.command
              , k = _.first(item.args)
              , values = _.tail(item.args);
            log.redis('%s %s%s :', cmd.toUpperCase(), k, values ? ' ' + JSON.stringify(values) : '', JSON.stringify(result[index]));
          });
          return result;
        })
        .catch(error => {
          log.redis('Error in %s', JSON.stringify(queue));
          log.redis('error stack :', error.stack || error);
          throw error;
        });
    }
    return promise.catch(err => {
      e.throwRedisOhm({redisError: err});
    });
  },
  publish: (channel, message) => p.do(() => {
    if (!ohm.pubCli) {
      ohm.pubCli = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
    }
    return ohm.pubCli.publishAsync(channel, message);
  }),
  unpublish: () => p.do(
    () => {
      if (!ohm.pubCli) {
        return;
      }
      return p.do(
        () => ohm.pubCli.quitAsync(),
        () => {
          delete ohm.pubCli;
        }
      );
    }),
  subscribe: (channel, cb) => p.do(() => {
    ohm.subCli = ohm.subCli || {};
    const subCli = ohm.subCli[channel] = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
    return new p(resolve => {
      subCli.on('subscribe', (channel, count) => {
        resolve([channel, count]);
      });
      subCli.on('message', cb);
      subCli.subscribe(channel);
    });
  }),
  unsubscribe: channel => p.do(
    () => {
      const subCli = ohm.subCli[channel];
      if (!subCli) {
        return;
      }
      return p.do(
        () => subCli.unsubscribeAsync(),
        () => subCli.quitAsync(),
        () => {
          delete ohm.subCli[channel];
        });
    }),
  filterProperties: (data, namespace, type, op) => {
    if (typeof op === 'undefined' && typeof type !== 'undefined') {
      op = type;
      type = 'db';
    }
    namespace = _.camelCase(namespace);
    const schema = ohm.getSchema(namespace, type, op);
    if (!schema || !schema.properties) {
      logger.enabledLevels.debug && log.debug('properties for "%s/%s/%s" not found : ignore filtering', namespace, type, op);
      return data;
    }
    const propFilter = Object.keys(schema.properties);
    if (!propFilter.length) {
      return data;
    }
    ohm.setSchemaDefaults(data, namespace, type, op);
    logger.enabledLevels.trace && log.trace('filtering instance with "%s/%s/%s"', namespace, type, op);
    const result = _.pick(data, propFilter);
    return result;
  },
  fromRedis: (entity, namespace, operation) => {
    const schema = ohm.getSchema(namespace, operation);
    let type;
    const result = {};
    _.forIn(entity, (value, key) => {
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
  generateId: type => {
    if (type === 'date') {
      const rnd = () =>Math.floor(Math.random() * 1e9).toString(36);
      return (+new Date()).toString(36) + rnd() + rnd();
    }
    return rack();
  },
  getEntityClass: schemaName => {
    assert(typeof schemaName === 'string', `expected a string but got ${schemaName}`);
    const entityClassName = schemaName.substring(0, 1).toUpperCase() + _.camelCase(schemaName).substring(1);
    let entityClass = ohm.entityClasses[schemaName];
    if (!entityClass) {
      entityClass = ohm.entityClasses[schemaName] = ohm.entityClasses[entityClassName] = ohm.createEntityClass(schemaName);
    }
    return entityClass;
  },
  getSchema: (namespace, type, op) => {
    if (typeof namespace === 'undefined') {
      return ohm.schemas;
    }
    if (typeof op === 'undefined' && typeof type !== 'undefined') {
      op = type;
      type = 'db';
    }
    namespace = _.camelCase(namespace);
    const key = _.compact([namespace, ohm.config.schemaMetaPrefix, type && ['operations', type].join('.'), type && op]).join('.');
    const schema = _.get(ohm.schemas, key);
    if (!schema) {
      e.throwEntitySchemaNotFound({namespace, type, op});
    }
    return schema;
  },
  getSchemaId: (namespace, type) => ohm.getSchema(namespace, type).id || 'id',
  setSchemaDefaults: (data, namespace, type, op) => {
    if (typeof data !== 'object') {
      return;
    }
    namespace = _.camelCase(namespace);
    const schema = ohm.getSchema(namespace, type, op);
    if (!schema) {
      logger.enabledLevels.debug && log.debug('schema for "%s/%s/%s" not found : ignore defaults', namespace, type, op);
      return data;
    }
    logger.enabledLevels.debug && log.debug('setting defaults with schema "%s/%s/%s"', namespace, type, op);
    _.forIn(schema.properties, (value, key) => {
      if (value.hasOwnProperty('default') && typeof data[key] === 'undefined') {
        data[key] = typeof value.default === 'function' ? value.default(data) : value.default;
      }
    });
    return data;
  },
  toHash: (...hashes) => {
    if (hashes.length === 1) {
      hashes = hashes[0].split(':');
    }
    hashes.splice(0, 0, ohm.config.prefix);
    const result = _.compact(hashes.map(item => Array.isArray(item) ? item.join(':') : item)).join(':');
    return result;
  },
  toRedis: (data, namespace) => {
    const schema = ohm.getSchema(namespace, 'save');
    const result = {};
    _.forIn(data, (value, key) => {
      if (!schema.properties[key]) {
        return;
      }
      let type = schema.properties[key].type;
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
  validateSchema: (data, namespace, type, op, cb = _.noop) => p.do(
    () => {
      if (typeof op === 'undefined' && typeof type !== 'undefined') {
        op = type;
        type = 'db';
      }
      namespace = _.camelCase(namespace);
      const schema = ohm.getSchema(namespace, type, op);
      if (!schema) {
        logger.enabledLevels.debug && log.debug('schema for "%s/%s/%s" not found : ignore validation', namespace, type, op);
        return false;
      }
      logger.enabledLevels.debug && log.debug('validating instance with schema "%s/%s/%s"', namespace, type, op);
      ohm.filterProperties(data, namespace, type, op);
      return validate(data, schema);
    })
    .nodeify(cb)
};

exports = module.exports = ohm;