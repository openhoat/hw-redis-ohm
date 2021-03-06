'use strict';

const Promise = require('bluebird');
const util = require('util');
const assert = require('assert');
const redis = require('redis');
const _ = require('lodash');
const JaySchema = require('jayschema');
const uuid = require('node-uuid');
const logger = require('hw-logger');
const log = logger.log;
const js = new JaySchema();
const e = require('./errors');
const validate = Promise.promisify(js.validate, {context: js});

Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

const idPattern = '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
const idSchema = {type: 'string', pattern: idPattern};
const idNullableSchema = {type: ['string', 'null'], pattern: idPattern};

let Entity;

const ohm = {
  config: {
    host: process.env['REDIS_HOST'] || '127.0.0.1',
    port: process.env['REDIS_PORT'] ? parseInt(process.env['REDIS_PORT']) : 6379,
    db: 0,
    prefix: 'hw-redis-ohm',
    idxHashPrefix: 'idx',
    schemaMetaPrefix: 'meta',
    schemas: {},
    parser: 'hiredis',
    entityClassLazyInit: true
  },
  e,
  entityClasses: {},
  idPattern,
  idSchema,
  idNullableSchema,
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
      if (!schemaSpec[ohm.config.schemaMetaPrefix]) {
        logger.enabledLevels.debug && log.debug('schema "%s" has no meta : ignore', schemaName);
        return;
      }
      logger.enabledLevels.debug && log.debug('initializing schema "%s"', schemaName);
      const metaPrefix = ohm.config.schemaMetaPrefix;
      const schema = _.cloneDeep(schemaSpec);
      ohm.schemas[schemaName] = schema;
      schema.title = util.format(schema.title, 'main', 'default');
      const meta = _.defaultsDeep(schema[metaPrefix], {
        operations: {db: {new: {}, save: {}, get: {}}}
      });
      _.forIn(meta.operations, (value, type) => {
        _.forIn(value, (value, operation) => {
          {
            const key = ['operations', type, operation, 'title'].join('.');
            logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', key, schemaName);
            _.set(meta, key, util.format(schemaSpec.title, type, operation));
          }
          {
            const key = ['operations', type, operation, 'type'].join('.');
            logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', key, schemaName);
            _.set(meta, key, schemaSpec.type);
          }
        });
      });
      const schemaId = ohm.getSchemaId(schemaName);
      if (typeof schema.properties[schemaId] === 'undefined') {
        logger.enabledLevels.trace && log.trace('setting default property "%s" to schema "%s"', schemaId, schemaName);
        schema.properties[schemaId] = idSchema;
      }
      _.forIn(schemaSpec.properties, (property, name) => {
        logger.enabledLevels.trace && log.trace('setting property "%s" to schema "%s"', name, schemaName);
        schema.properties[name] = schemaSpec.properties[name];
      });
      logger.enabledLevels.trace && log.trace('setting meta datas to schema "%s"', schemaName);
      if (schemaSpec[ohm.config.schemaMetaPrefix].links) {
        schemaSpec[ohm.config.schemaMetaPrefix].links.forEach(link => {
          if (typeof schema.properties[link.as] === 'undefined') {
            logger.enabledLevels.trace && log.trace('setting link property "%s" to schema "%s"', link.as, schemaName);
            if (link.type === 'hasMany') {
              schema.properties[link.as] = {
                type: 'array',
                items: idSchema
              };
            } else {
              schema.properties[link.as] = idNullableSchema;
            }
          }
        });
      }
      /*key = 'operations.db.new.excludeProperties';
       if (typeof _.get(meta, key) === 'undefined') {
       logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
       _.set(meta, key, [schemaId]);
       }*/
      {
        const key = 'operations.db.save.required';
        if (typeof _.get(meta, key) === 'undefined') {
          logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
          _.set(meta, key, [schemaId]);
        }
      }
      /*key = 'operations.db.save.excludeProperties';
       if (typeof _.get(meta, key) === 'undefined' && linkNames && linkNames.length) {
       logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
       _.set(meta, key, linkNames);
       }*/
      {
        const key = 'operations.db.save.minProperties';
        if (typeof _.get(meta, key) === 'undefined') {
          logger.enabledLevels.trace && log.trace('setting meta "%s" to schema "%s"', key, schemaName);
          _.set(meta, key, 2);
        }
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
  start: (opt, cb) => Promise.resolve()
    .then(() => {
      if (typeof cb === 'undefined' && typeof opt === 'function') {
        cb = opt;
        opt = null;
      }
      cb = cb || _.noop;
      return Promise.resolve()
        .then(() => {
          if (ohm.started) {
            return false;
          }
          if (!ohm.initialized) {
            ohm.init(opt);
          }
          ohm.cli = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
          if (ohm.config.roHost) {
            ohm.roCli = redis
              .createClient(
                ohm.config.roPort || ohm.config.port,
                ohm.config.roHost,
                ohm.config.roOptions || ohm.config.options
              );
          }
          return new Promise(resolve => ohm.cli.once('connect', resolve))
            .then(() => ohm.cli.selectAsync(ohm.config.db))
            .then(() => {
              if (ohm.roCli) {
                return ohm.roCli.selectAsync(ohm.config.db);
              }
            })
            .then(() => {
              ohm.started = true;
              logger.enabledLevels.info && log.info('redis ohm client started');
              return true;
            });
        })
        .asCallback(cb);
    }),
  stop: (cb = _.noop) => Promise.resolve()
    .then(() => {
      if (!ohm.started) {
        return false;
      }
      return Promise.resolve()
        .then(() => {
          if (!ohm.pubCli) {
            return;
          }
          logger.enabledLevels.debug && log.debug('destroying pub cli');
          return ohm.unpublish();
        })
        .then(() => {
          if (!ohm.subCli) {
            return;
          }
          return Promise.map(Object.keys(ohm.subCli), channel => {
            logger.enabledLevels.debug && log.debug('unsubscribe channel %s', channel);
            return ohm.unsubscribe(channel);
          });
        })
        .then(() => {
          logger.enabledLevels.debug && log.debug('closing cli');
          return ohm.cli.quitAsync();
        })
        .then(() => {
          if (ohm.roCli) {
            logger.enabledLevels.debug && log.debug('closing readonly cli');
            return ohm.roCli.quitAsync();
          }
        })
        .then(() => {
          ohm.started = false;
          ohm.initialized = false;
          logger.enabledLevels.info && log.info('redis ohm client stopped');
          return true;
        });
    })
    .asCallback(cb),
  createLocalMulti: multi => multi || ohm.multi(),
  createEntityClass: schemaName => {
    const schema = ohm.getSchema(schemaName);
    const clazz = class extends Entity {
      static get name() {
        return 'Entity' + schemaName.substring(0, 1).toUpperCase() + _.camelCase(schemaName.substring(1));
      }

      static get type() {
        return schemaName;
      }
    };
    const defineIndexes = () => {
      clazz.indexes = {};
      if (schema.indexes) {
        schema.indexes.forEach(index => {
          if (Array.isArray(index.name)) {
            clazz.indexes[index.name.join(',')] = {
              name: index.name,
              unique: index.unique,
              key: ohm
                .toHash(
                  ohm.config.idxHashPrefix,
                  schemaName,
                  index.name,
                  new Array(index.name.length).fill('%s')
                ),
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
            reverseLink = _.first(reverseSchema.links
              .filter(el => el.target === schemaName && el.foreignKey === link.as)
            );
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
    const k = args[0];
    const values = args[1];
    return Promise.resolve()
      .then(() => {
        let promise;
        if (typeof ohm.cli[cmd + 'Async'] !== 'function') {
          e.throwUnsupportedOhm({cmd});
        }
        promise = ohm.cli[cmd + 'Async'](...args);
        if (logger.enabledLevels.redis) {
          promise = promise
            .then(result => {
              log.redis('%s %s%s :',
                cmd.toUpperCase(),
                k,
                values ? ' ' + JSON.stringify(values) : '',
                JSON.stringify(result)
              );
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
    return new Promise(resolve => {
      if (typeof multi[cmd] !== 'function') {
        e.throwUnsupportedOhm({cmd});
      }
      resolve(multi[cmd](...args));
    });
  },
  multi: () => ohm.cli.multi(),
  processLocalMulti: (localMulti, multi, result) => {
    const isLocal = typeof multi !== 'undefined' || localMulti !== multi;
    return Promise.resolve()
      .then(() => {
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
            log.redis('%s %s%s :',
              cmd.toUpperCase(),
              k,
              values ? ' ' + JSON.stringify(values) : '',
              JSON.stringify(result[index])
            );
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
      e.throwRedisOhm({redisError: err.errors || err});
    });
  },
  publish: (channel, message) => Promise.resolve()
    .then(() => {
      if (!ohm.pubCli) {
        ohm.pubCli = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
      }
      return ohm.pubCli.publishAsync(channel, message);
    }),
  unpublish: () => Promise.resolve()
    .then(() => {
      if (!ohm.pubCli) {
        return;
      }
      return ohm.pubCli.quitAsync()
        .then(() => {
          delete ohm.pubCli;
        });
    }),
  subscribe: (channel, cb) => Promise.resolve()
    .then(() => {
      ohm.subCli = ohm.subCli || {};
      const subCli = ohm.subCli[channel] = redis.createClient(ohm.config.port, ohm.config.host, ohm.config.options);
      return new Promise(resolve => {
        subCli.on('subscribe', (channel, count) => {
          resolve([channel, count]);
        });
        subCli.on('message', cb);
        subCli.subscribe(channel);
      });
    }),
  unsubscribe: channel => Promise.resolve()
    .then(() => {
      const subCli = ohm.subCli[channel];
      if (!subCli) {
        return;
      }
      return subCli.unsubscribeAsync()
        .then(() => subCli.quitAsync())
        .then(() => {
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
      logger.enabledLevels.debug && log.debug('properties for "%s/%s/%s" not found : ignore filtering',
        namespace,
        type,
        op
      );
      return data;
    }
    const propFilter = Object.keys(schema.properties);
    if (!propFilter.length) {
      return data;
    }
    ohm.setSchemaDefaults(data, namespace, type, op);
    logger.enabledLevels.trace && log.trace('filtering instance with "%s/%s/%s"', namespace, type, op);
    return _.pick(data, propFilter);
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
  generateId: () => uuid.v4(),
  getEntityClass: schemaName => {
    assert(typeof schemaName === 'string', `expected a string but got ${schemaName}`);
    const entityClassName = schemaName.substring(0, 1).toUpperCase() + _.camelCase(schemaName).substring(1);
    let entityClass = ohm.entityClasses[schemaName];
    if (!entityClass) {
      entityClass = ohm.createEntityClass(schemaName);
      ohm.entityClasses[entityClassName] = entityClass;
      ohm.entityClasses[schemaName] = entityClass;
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
    const key = _.compact([
      namespace,
      ohm.config.schemaMetaPrefix,
      type && ['operations', type].join('.'),
      type && op
    ]).join('.');
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
    return _.compact(hashes.map(item => (Array.isArray(item) ? item.join(':') : item))).join(':');
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
          if (value === null) {
            result[key] = null;
          } else {
            result[key] = JSON.stringify(value);
          }
          break;
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
  validateSchema: (data, namespace, type, op, cb = _.noop) => Promise.resolve()
    .then(() => {
      if (typeof op === 'undefined' && typeof type !== 'undefined') {
        op = type;
        type = 'db';
      }
      namespace = _.camelCase(namespace);
      const schema = ohm.getSchema(namespace, type, op);
      if (!schema) {
        logger.enabledLevels.debug && log.debug('schema for "%s/%s/%s" not found : ignore validation',
          namespace,
          type,
          op
        );
        return false;
      }
      logger.enabledLevels.debug && log.debug('validating instance with schema "%s/%s/%s"', namespace, type, op);
      ohm.filterProperties(data, namespace, type, op);
      return validate(data, schema);
    })
    .asCallback(cb)
};

exports = module.exports = ohm;
