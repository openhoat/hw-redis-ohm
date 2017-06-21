'use strict';

const Promise = require('bluebird');
const util = require('util');
const _ = require('lodash');
const logger = require('hw-logger');
const ohm = require('./ohm');
const log = logger.log;

class Entity {
  constructor(opt = {}) {
    this.value = opt;
  }

  static toString() {
    return 'class Entity ' + this.name + ' : ' + this.type;
  }

  get type() {
    return this.constructor.type;
  }

  toString() {
    return this.name + ' : ' + JSON.stringify(this, null, 2);
  }

  static create(value) {
    const clazz = this;
    return new clazz(Object.assign({}, value));
  }

  static delete(id) {
    const clazz = this;
    return clazz.load(id)
      .then(entity => entity.delete());
  }

  static findByIndex(name, value, sort) {
    const clazz = this;
    const type = clazz.type;
    logger.enabledLevels.trace && log.trace('searching entity "%s" matching index "%s" with "%s"', type, name, value);
    return clazz.findIdsByIndex(name, value)
      .then(result => Promise.map(result, clazz.load.bind(clazz)))
      .then(result => {
        if (!sort) {
          return result;
        }
        return _.sortBy(result, `value.${sort}`);
      });
  }

  static findIdsByIndex(name, value) {
    const clazz = this;
    const type = clazz.type;
    return Promise.resolve()
      .then(() => {
        logger.enabledLevels.debug && log.debug('searching %s entity ID with index "%s" matching "%s"',
          type,
          name,
          value
        );
        const index = clazz.indexes && clazz.indexes[name];
        if (!index) {
          logger.enabledLevels.debug && log.debug('no index "%s" for entity "%s"', name, type);
          return;
        }
        const k = util.format.apply(null, Array.isArray(value) ? [index.key].concat(value) : [index.key, value]);
        return ohm.exec(index.unique ? 'get' : 'smembers', k)
          .then(result => {
            if (!result || !result.length) {
              return [];
            }
            return Array.isArray(result) ? result : [result];
          });
      })
      .then(result => {
        if (result) {
          return result;
        }
        logger.enabledLevels.debug && log.debug('searching %s entity ID with link "%s" matching "%s"',
          type,
          name,
          value
        );
        const link = clazz.links && clazz.links[name];
        if (!link) {
          logger.enabledLevels.debug && log.debug('no link "%s" for entity "%s"', name, type);
          return [];
        }
        const k = util.format.apply(null, Array.isArray(value) ? [link.key].concat(value) : [link.key, value]);
        return ohm.exec(link.reverseUnique ? 'get' : 'smembers', k)
          .then(result => {
            if (!result) {
              return [];
            }
            return Array.isArray(result) ? result.sort() : [result];
          });
      });
  }

  static findIdByIndex(name, value) {
    const clazz = this;
    return clazz.findIdsByIndex(name, value).then(ids => _.head(ids));
  }

  static getId() {
    const clazz = this;
    const type = clazz.type;
    return ohm.getSchemaId(type);
  }

  static list(sort) {
    const clazz = this;
    const type = clazz.type;
    return Promise.resolve()
      .then(() => {
        logger.enabledLevels.debug && log.debug('loading "%s" entities', type);
        return ohm.exec('keys', ohm.toHash(type, '*'))
          .then(keys => Promise.map(keys, k => {
            const id = _.last(k.split(':'));
            return clazz.load(id);
          }));
      })
      .then(result => {
        if (!sort) {
          return result;
        }
        return _.sortBy(result, `value.${sort}`);
      });
  }

  static load(id) {
    const clazz = this;
    const type = clazz.type;
    let entity, k;
    logger.enabledLevels.debug && log.debug('loading "%s" entity from #%s', type, id);
    return Promise.resolve()
      .then(() => {
        k = ohm.toHash(type, id);
        return ohm.exec(clazz.isObject ? 'hgetall' : 'get', k)
          .then(result => {
            if (!result) {
              ohm.e.throwEntityNotFound({type, attrName: 'id', attrValue: id});
            }
            if (!clazz.isObject) {
              result = {id, value: result};
            }
            result = ohm.fromRedis(result, type, 'get');
            entity = clazz.create(result);
          });
      })
      .then(() => {
        if (clazz.isObject) {
          return;
        }
        return ohm.exec('ttl', k)
          .then(ttl => {
            entity.value.ttl = ttl;
          });
      })
      .then(() => entity.loadLinks())
      .then(() => {
        entity.value = ohm.filterProperties(entity.value, type, 'get');
        return entity;
      });
  }

  static setDefaults(data, operation) {
    const type = this.type;
    ohm.setSchemaDefaults(data, type, 'db', operation);
  }

  static update(value, multi) {
    const clazz = this;
    return Promise.resolve().then(() => clazz.create(value).update(multi));
  }

  static saveOrUpdate(value, multi) {
    const clazz = this;
    const type = this.type;
    const entity = clazz.create(value);
    const localMulti = ohm.createLocalMulti(multi);
    return Promise.resolve()
      .then(() => {
        if (!entity.getId()) {
          ohm.e.throwEntityNotFound();
        }
        return entity.update(localMulti);
      })
      .catch(ohm.e.EntityNotFoundError, notFoundErr => {
        logger.enabledLevels.debug && log.debug('entity "%s" does not exists : trying to save it', type);
        return entity.save(localMulti)
          .catch(ohm.e.EntityConflictError, conflictErr => clazz
            .findIdsByIndex(conflictErr.extra.attrName, conflictErr.extra.attrValue)
            .then(ids => _.head(ids))
            .then(id => {
              if (!id) {
                throw conflictErr;
              }
              logger.enabledLevels.debug && log.debug('entity "%s" already exists : trying to update it', type);
              entity.value[clazz.getId()] = id;
              return entity.update(localMulti);
            })
          );
      });
  }

  getId() {
    return this.value[this.constructor.getId()];
  }

  checkIndexes() {
    const entity = this;
    let type, id;
    return Promise.resolve()
      .then(() => {
        type = entity.type;
        id = entity.getId();
        logger.enabledLevels.debug && log.debug('checking indexes for entity "%s" #%s', type, id);
        return entity.iterateIndexes((index, name, k) => {
          if (!index.unique) {
            return;
          }
          return ohm.exec('exists', k)
            .then(result => result && ohm.exec('get', k)
              .then(result => {
                if (result !== id) {
                  const nameValue = name.indexOf(',') !== -1 ?
                    name.split(',').map(item => entity.value[item]).join(',') :
                    entity.value[name];
                  ohm.e.throwEntityConflict({type, attrName: name, attrValue: nameValue});
                }
              })
            );
        });
      })
      .then(() => {
        logger.enabledLevels.debug && log.debug('checking links for entity "%s" #%s', type, id);
        return entity.iterateLinks((link, name, k, reverseK, value) => {
          if (!link.unique && !link.reverseUnique) {
            return;
          }
          return Promise.resolve()
            .then(() => Promise.resolve()
              .then(() => {
                if (link.reverseUnique) {
                  return ohm.exec('exists', k)
                    .then(result => result && ohm.exec('get', k)
                      .then(result => {
                        if (result !== id) {
                          ohm.e.throwEntityConflict({type, attrName: name, attrValue: value});
                        }
                      })
                    );
                }
              })
            )
            .then(() => {
              if (!reverseK) {
                return;
              }
              return Promise.resolve()
                .then(() => {
                  if (link.unique) {
                    return ohm.exec('exists', reverseK)
                      .then(result => result && ohm.exec('get', reverseK)
                        .then(result => {
                          if (result !== value) {
                            ohm.e.throwEntityConflict({type, attrName: name, attrValue: value});
                          }
                        })
                      );
                  }
                });
            });
        });
      });
  }

  delete(multi) {
    const entity = this;
    let id, type;
    const localMulti = ohm.createLocalMulti(multi);
    return Promise.resolve()
      .then(() => {
        type = entity.type;
        id = entity.getId();
        if (typeof id === 'undefined') {
          ohm.e.throwEntityValidation({type, attrName: 'id', attrValue: id});
        }
        logger.enabledLevels.debug && log.debug('deleting "%s" entity #%s', type, id);
      })
      .then(() => entity.removeLinks(localMulti)
        .then(() => entity.removeIndexes(localMulti))
      )
      .then(() => {
        const k = ohm.toHash(type, id);
        return ohm.execMulti(localMulti, 'del', k);
      })
      .then(() => ohm.processLocalMulti(localMulti, multi));
  }

  generateId() {
    const entity = this;
    let type, schema, idName;
    return Promise.resolve()
      .then(() => {
        type = entity.type;
        schema = ohm.getSchema(type);
        idName = ohm.getSchemaId(type);
        logger.enabledLevels.debug && log.debug('generating ID for entity type "%s"', type);
        if (typeof entity[idName] !== 'undefined') {
          return;
        }
        entity.value[idName] = ohm.generateId(schema.idGenerator);
      });
  }

  getSchema(operation) {
    return ohm.getSchema(this.type, operation);
  }

  iterateIndexes(iterator) {
    const entity = this;
    let clazz;
    return Promise.resolve()
      .then(() => {
        clazz = entity.constructor;
        if (!clazz.indexes) {
          return;
        }
        return Promise.each(Object.keys(clazz.indexes), name => {
          const index = clazz.indexes[name];
          if (typeof index.getValue === 'function') {
            return Promise.resolve()
              .then(() => index.getValue(entity)
                .catch(ohm.e.EntityNotFoundError, () => {
                })
              )
              .then(k => {
                if (typeof k === 'undefined' || k === null || (Array.isArray(k) && !k.length)) {
                  return;
                }
                return iterator(index, name, k);
              });
          }
          return Promise.resolve()
            .then(() => {
              if (name.indexOf(',') !== -1) {
                return name.split(',').map(name => entity.value[name]);
              } else {
                return entity.value[name];
              }
            })
            .then(value => {
              if (typeof value === 'undefined' || value === null || (Array.isArray(value) && !value.length)) {
                return;
              }
              const k = util.format.apply(null, Array.isArray(value) ? [index.key].concat(value) : [index.key, value]);
              return iterator(index, name, k);
            });
        });
      });
  }

  iterateLinks(withValue, iterator) {
    const entity = this;
    let clazz, id;
    if (typeof iterator === 'undefined' && typeof withValue === 'function') {
      iterator = withValue;
      withValue = true;
    }
    withValue = !!withValue;
    return Promise.resolve()
      .then(() => {
        clazz = entity.constructor;
        id = entity.getId();
        if (!clazz.links) {
          return;
        }
        return Promise.each(Object.keys(clazz.links), name => {
          const link = clazz.links[name];
          const value = entity.value[name];
          if (withValue) {
            if (typeof value === 'undefined' || value === null || (Array.isArray(value) && !value.length)) {
              return;
            }
            return Promise.each(Array.isArray(value) ? value : [value], value => {
              const k = util.format(link.key, value);
              const reverseK = link.reverseKey && util.format(link.reverseKey, id);
              return iterator(link, name, k, reverseK, value);
            });
          }
          return Promise.resolve()
            .then(() => {
              const k = util.format(link.reverseKey, id);
              return iterator(link, name, k);
            });
        });
      });
  }

  loadLinks() {
    const entity = this;
    return Promise.resolve()
      .then(() => {
        const type = entity.type;
        const id = entity.getId();
        logger.enabledLevels.debug && log.debug('loading links for entity "%s" #%s', type, id);
        return entity.iterateLinks(false, (link, name, k) => ohm
          .exec(link.unique ? 'get' : 'smembers', k)
          .then(result => {
            entity.value[name] = result;
          })
        );
      });
  }

  removeIndexes(multi) {
    const entity = this;
    const localMulti = ohm.createLocalMulti(multi);
    return Promise.resolve()
      .then(() => {
        const clazz = entity.constructor;
        const type = entity.type;
        const id = entity.getId();
        if (!clazz.indexes) {
          return;
        }
        logger.enabledLevels.debug && log.debug('removing indexes for entity "%s" #%s', type, id);
        return entity
          .iterateIndexes((index, name, k) => {
            if (index.unique) {
              return ohm.execMulti(localMulti, 'del', k);
            } else {
              return ohm.execMulti(localMulti, 'srem', k, id);
            }
          })
          .then(() => ohm.processLocalMulti(localMulti, multi));
      });
  }

  removeLinks(multi) {
    const entity = this;
    const localMulti = ohm.createLocalMulti(multi);
    return Promise.resolve()
      .then(() => {
        const type = entity.type;
        const id = entity.getId();
        logger.enabledLevels.debug && log.debug('removing links for entity "%s" #%s', type, id);
        return entity
          .iterateLinks((link, name, k, reverseK, value) => Promise.resolve()
            .then(() => {
              if (link.reverseUnique) {
                return ohm.execMulti(localMulti, 'del', k);
              } else {
                return ohm.execMulti(localMulti, 'srem', k, id);
              }
            })
            .then(() => {
              if (!reverseK) {
                return;
              }
              if (link.unique) {
                return ohm.execMulti(localMulti, 'del', reverseK);
              } else {
                return ohm.execMulti(localMulti, 'srem', reverseK, value);
              }
            })
          )
          .then(() => ohm.processLocalMulti(localMulti, multi));
      });
  }

  save(multi) {
    const entity = this;
    const localMulti = ohm.createLocalMulti(multi);
    let clazz, type;
    return Promise.resolve()
      .then(() => {
        clazz = entity.constructor;
        type = entity.type;
        logger.enabledLevels.debug && log.debug('saving new "%s" entity', type);
        return entity.validate('new');
      })
      .then(() => {
        if (typeof entity.getId() !== 'undefined') {
          return;
        }
        return entity.generateId();
      })
      .then(() => entity.checkIndexes())
      .then(() => entity.saveIndexes(localMulti)
        .then(() => entity.saveLinks(localMulti))
      )
      .then(() => {
        const result = ohm.filterProperties(entity.value, type, 'save');
        const k = ohm.toHash(type, entity.getId());
        let ignoreProps = Object.keys(clazz.links);
        if (!clazz.isObject) {
          ignoreProps = ignoreProps.concat(ohm.getSchemaId(type));
        }
        let data = ohm.toRedis(_.omit(result, ignoreProps), type);
        if (typeof data === 'object') {
          data = _(data).omitBy(_.isUndefined).omitBy(_.isNull).value();
        }
        return ohm.execMulti(localMulti, clazz.isObject ? 'hmset' : 'set', k, data)
          .then(() => {
            if (entity.ttl) {
              result.ttl = entity.ttl;
              if (entity.ttl === -1) {
                return ohm.execMulti(localMulti, 'persist', k, entity.ttl);
              }
              return ohm.execMulti(localMulti, 'expire', k, entity.ttl);
            }
          });
      })
      .then(() => ohm.processLocalMulti(localMulti, multi))
      .then(() => entity.loadLinks())
      .then(() => {
        entity.value = ohm.filterProperties(entity.value, type, 'get');
        return entity;
      });
  }

  saveIndexes(multi) {
    const entity = this;
    const localMulti = ohm.createLocalMulti(multi);
    return Promise.resolve()
      .then(() => {
        const clazz = entity.constructor;
        const type = entity.type;
        const id = entity.getId();
        if (!clazz.indexes) {
          return;
        }
        logger.enabledLevels.debug && log.debug('saving indexes for entity "%s" #%s', type, id);
        return entity
          .iterateIndexes((index, name, k) => {
            if (index.unique) {
              return ohm.execMulti(localMulti, 'set', k, id)
                .then(() => {
                  const ttl = entity.ttl || index.ttl;
                  if (ttl) {
                    if (ttl === -1) {
                      return ohm.execMulti(localMulti, 'persist', k);
                    }
                    return ohm.execMulti(localMulti, 'expire', k, ttl);
                  }
                });
            } else {
              return ohm.execMulti(localMulti, 'sadd', k, id);
            }
          })
          .then(() => ohm.processLocalMulti(localMulti, multi));
      });
  }

  saveLinks(multi) {
    const entity = this;
    const localMulti = ohm.createLocalMulti(multi);
    return Promise.resolve()
      .then(() => {
        const type = entity.type;
        const id = entity.getId();
        logger.enabledLevels.debug && log.debug('saving links for entity "%s" #%s', type, id);
        return entity
          .iterateLinks((link, name, k, reverseK, value) => Promise.resolve()
            .then(() => {
              if (link.reverseUnique) {
                return ohm.execMulti(localMulti, 'set', k, id)
                  .then(() => {
                    const ttl = entity.ttl || link.ttl;
                    if (ttl) {
                      if (ttl === -1) {
                        return ohm.execMulti(localMulti, 'persist', k);
                      }
                      return ohm.execMulti(localMulti, 'expire', k, ttl);
                    }
                  });
              } else {
                return ohm.execMulti(localMulti, 'sadd', k, id);
              }
            })
            .then(() => {
              if (!reverseK) {
                return;
              }
              if (link.unique) {
                return ohm.execMulti(localMulti, 'set', reverseK, value)
                  .then(() => {
                    const ttl = entity.ttl || link.ttl;
                    if (ttl) {
                      if (ttl === -1) {
                        return ohm.execMulti(localMulti, 'persist', k);
                      }
                      return ohm.execMulti(localMulti, 'expire', k, ttl);
                    }
                  });
              } else {
                return ohm.execMulti(localMulti, 'sadd', reverseK, value);
              }
            })
          )
          .then(() => ohm.processLocalMulti(localMulti, multi));
      });
  }

  update(multi) {
    const entity = this;
    const localMulti = ohm.createLocalMulti(multi);
    let type, clazz, id, existing;
    return Promise.resolve()
      .then(() => {
        type = entity.type;
        clazz = entity.constructor;
        id = entity.getId();
        logger.enabledLevels.debug && log.debug('updating "%s" entity  #%s', type, id);
        return entity.validate('save');
      })
      .then(() => clazz.load(id)
        .then(result => {
          existing = result;
        })
      )
      .then(() => existing.removeLinks(localMulti)
        .then(() => existing.removeIndexes(localMulti))
      )
      .then(() => entity.checkIndexes()
        .catch(err => existing.saveLinks(localMulti)
          .then(() => existing.saveIndexes(localMulti))
          .finally(() => {
            throw err;
          })
        )
      )
      .then(() => {
        _.defaults(entity.value, existing.value);
        return entity.saveIndexes(localMulti)
          .then(() => entity.saveLinks(localMulti));
      })
      .then(() => {
        const result = ohm.filterProperties(entity.value, type, 'save');
        const k = ohm.toHash(type, entity.getId());
        let ignoreProps = Object.keys(clazz.links);
        if (!clazz.isObject) {
          ignoreProps = ignoreProps.concat(ohm.getSchemaId(type));
        }
        let data = ohm.toRedis(_.omit(result, ignoreProps), type);
        if (typeof data === 'object') {
          data = _.omitBy(data, _.isUndefined);
        }
        return Promise.resolve()
          .then(() => {
            if (!clazz.isObject) {
              return;
            }
            const nullProps = Object.keys(_.pickBy(data, _.isNull));
            data = _.omitBy(data, _.isNull);
            if (nullProps && nullProps.length) {
              return ohm.execMulti(localMulti, 'hdel', k, nullProps);
            }
          })
          .then(() => ohm.execMulti(localMulti, clazz.isObject ? 'hmset' : 'set', k, data))
          .then(() => {
            if (entity.ttl) {
              if (entity.ttl === -1) {
                return ohm.execMulti(localMulti, 'persist', k);
              }
              return ohm.execMulti(localMulti, 'expire', k, entity.ttl);
            }
          })
          .then(() => ohm.processLocalMulti(localMulti, multi))
          .then(() => {
            entity.value = ohm.filterProperties(entity.value, type, 'get');
            return entity;
          });
      });
  }

  validate(...args) {
    const entity = this;
    return ohm.validateSchema(entity.value, entity.type, args)
      .catch(err => {
        ohm.e.throwEntityValidation({type: entity.type, schemaErrors: err});
      });
  }
}

exports = module.exports = Entity;
