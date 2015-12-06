'use strict';

var util = require('util')
  , _ = require('lodash')
  , e = require('hw-error')
  , p = require('hw-promise')
  , logger = require('hw-logger')
  , ohm = require('./ohm')
  , log = logger.log;

function Entity(opt) {
  this.value = opt || {};
}

Entity.create = function (value) {
  var clazz = this
    , entity;
  entity = new clazz(value);
  return entity;
};

Entity.delete = function (id) {
  var clazz = this;
  return clazz.load(id)
    .then(function (entity) {
      return entity.delete();
    });
};

Entity.findByIndex = function (name, value, sort) {
  var clazz = this
    , type = clazz.type;
  logger.enabledLevels.trace && log.trace('searching entity "%s" matching index "%s" with "%s"', type, name, value);
  return clazz.findIdsByIndex(name, value)
    .then(function (result) {
      return p.map(result, clazz.load.bind(clazz))
        .then(function (result) {
          return _.sortBy(result, 'value.' + (sort || ohm.getSchemaId(type)));
        });
    });
};

Entity.findIdsByIndex = function (name, value) {
  var clazz = this
    , type = clazz.type;
  return p.do(
    function () {
      var index, k;
      logger.enabledLevels.debug && log.debug('searching %s entity ID with index "%s" matching "%s"', type, name, value);
      index = clazz.indexes && clazz.indexes[name];
      if (!index) {
        logger.enabledLevels.debug && log.debug('no index "%s" for entity "%s"', name, type);
        return;
      }
      k = util.format.apply(null, Array.isArray(value) ? [index.key].concat(value) : [index.key, value]);
      return ohm.exec(index.unique ? 'get' : 'smembers', k)
        .then(function (result) {
          if (!result || !result.length) {
            return [];
          }
          return Array.isArray(result) ? result : [result];
        });
    },
    function (result) {
      var link, k;
      if (result) {
        return result;
      }
      logger.enabledLevels.debug && log.debug('searching %s entity ID with link "%s" matching "%s"', type, name, value);
      link = clazz.links && clazz.links[name];
      if (!link) {
        logger.enabledLevels.debug && log.debug('no link "%s" for entity "%s"', name, type);
        return [];
      }
      k = util.format.apply(null, Array.isArray(value) ? [link.key].concat(value) : [link.key, value]);
      return ohm.exec(link.reverseUnique ? 'get' : 'smembers', k)
        .then(function (result) {
          if (!result) {
            return [];
          }
          return Array.isArray(result) ? result.sort() : [result];
        });
    });
};

Entity.getId = function () {
  var clazz = this
    , type = clazz.type;
  return ohm.getSchemaId(type);
};

Entity.list = function (sort) {
  var clazz = this
    , type = clazz.type;
  return p.do(
    function () {
      logger.enabledLevels.debug && log.debug('loading "%s" entities', type);
      return ohm.exec('keys', ohm.toHash(type, '*'))
        .then(function (keys) {
          return p.map(keys, function (k) {
            var id;
            id = _.last(k.split(':'));
            return clazz.load(id);
          });
        });
    },
    function (result) {
      return _.sortBy(result, 'value.' + (sort || ohm.getSchemaId(type)));
    });
};

Entity.load = function (id) {
  var clazz = this
    , type = clazz.type
    , entity, k;
  logger.enabledLevels.debug && log.debug('loading "%s" entity from #%s', type, id);
  return p.do(
    function () {
      k = ohm.toHash(type, id);
      return ohm.exec(clazz.isObject ? 'hgetall' : 'get', k)
        .then(function (result) {
          if (!result) {
            e.throw('NOT_FOUND', util.format('entity "%s" #%s not found', type, id), {id: id});
          }
          if (!clazz.isObject) {
            result = {id: id, value: result};
          }
          result = ohm.fromRedis(result, type, 'get');
          entity = clazz.create(result);
        });
    },
    function () {
      if (clazz.isObject) {
        return;
      }
      return ohm.exec('ttl', k)
        .then(function (ttl) {
          entity.value.ttl = ttl;
        });
    },
    function () {
      return entity.loadLinks();
    },
    function () {
      entity.value = ohm.filterProperties(entity.value, type, 'get');
      return entity;
    });
};

Entity.setDefaults = function (data, operation) {
  var type = this.type;
  ohm.setSchemaDefaults(data, type, 'db', operation);
};

Entity.update = function (value) {
  var clazz = this
    , entity;
  return p.do(function () {
    entity = clazz.create(value);
    return entity.update();
  });
};

Entity.prototype.checkIndexes = function () {
  var entity = this
    , clazz, type, schema, id;
  return p.do(
    function () {
      clazz = entity.getClass();
      type = entity.type;
      schema = entity.getSchema();
      id = entity.getId();
      logger.enabledLevels.debug && log.debug('checking indexes for entity "%s" #%s', type, id);
      return entity.iterateIndexes(function (index, name, k) {
        if (!index.unique) {
          return;
        }
        return ohm.exec('exists', k).then(function (result) {
          var nameValue;
          if (result) {
            nameValue = name.indexOf(',') !== -1 ? name.split(',').map(function (item) {
              return entity.value[item];
            }).join(',') : entity.value[name];
            e.throw('CONFLICT', util.format('%s "%s" already exists', name, nameValue), {name: name, value: nameValue});
          }
        });
      });
    },
    function () {
      logger.enabledLevels.debug && log.debug('checking links for entity "%s" #%s', type, id);
      return entity.iterateLinks(function (link, name, k, reverseK, value) {
        if (!link.unique && !link.reverseUnique) {
          return;
        }
        return p.do(
          function checkLinkToEntity() {
            return p.do(
              function () {
                if (link.reverseUnique) {
                  return ohm.exec('exists', k);
                } else {
                  return ohm.exec('sismember', k, id);
                }
              },
              function (result) {
                if (result) {
                  e.throw('CONFLICT', util.format('%s "%s" already exists', name, value), {name: name, value: value});
                }
              });
          },
          function checkLinkFromEntity() {
            if (!reverseK) {
              return;
            }
            return p.do(
              function () {
                if (link.unique) {
                  return ohm.exec('exists', reverseK);
                } else {
                  return ohm.exec('sismember', reverseK, id);
                }
              },
              function (result) {
                if (result) {
                  e.throw('CONFLICT', util.format('%s "%s" already exists', name, value), {name: name, value: value});
                }
              });
          });
      });
    });
};

Entity.prototype.delete = function (multi) {
  var entity = this
    , localMulti, id, type, clazz;
  localMulti = ohm.createLocalMulti.apply(null, arguments);
  return p.do(
    function () {
      id = entity.getId();
      if (typeof id === 'undefined') {
        e.throw('BAD_FORMAT', 'entity "%s" has no %s property', type, id, {type: type, name: 'id'});
      }
      type = entity.type;
      clazz = entity.getClass();
      logger.enabledLevels.debug && log.debug('deleting "%s" entity #%s', type, id);
    },
    function removeRefs() {
      return entity.removeLinks(localMulti)
        .then(entity.removeIndexes.bind(entity, localMulti));
    },
    function () {
      var k;
      k = ohm.toHash(type, id);
      return ohm.execMulti(localMulti, 'del', k);
    },
    ohm.processLocalMulti.bind(null, localMulti, multi)
  );
};

Entity.prototype.generateId = function () {
  var entity = this
    , type, schema, idName;
  return p.do(function () {
    type = entity.type;
    schema = ohm.getSchema(type);
    idName = ohm.getSchemaId(type);
    logger.enabledLevels.debug && log.debug('generating ID for entity type "%s"', type);
    if (typeof entity[idName] !== 'undefined') {
      return;
    }
    if (schema.idGenerator !== null) {
      if (typeof schema.idGenerator === 'function') {
        return schema.idGenerator()
          .then(function (id) {
            entity.value[idName] = id.toString();
          });
      }
      if (schema.idGenerator === 'increment') {
        return ohm.exec('incr', ohm.toHash(ohm.config.idsHashPrefix, type/*_.kebabCase(type)*/))
          .then(function (id) {
            entity.value[idName] = id.toString();
          });
      }
      if (schema.idGenerator === 'date') {
        entity.value[idName] = ohm.generateId('date');
        return;
      }
      entity.value[idName] = ohm.generateId();
    }
  });
};

Entity.prototype.getId = function () {
  return this.value[this.getClass().getId()];
};

Entity.prototype.getClass = function () {
  return ohm.getEntityClass(this.type);
};

Entity.prototype.getSchema = function (operation) {
  return ohm.getSchema(this.type, operation);
};

Entity.prototype.iterateIndexes = function (iterator) {
  var entity = this
    , clazz;
  return p.do(function () {
    clazz = entity.getClass();
    if (!clazz.indexes) {
      return;
    }
    return p.forIn(clazz.indexes, function (index, name) {
      if (typeof index.getValue === 'function') {
        return p.do(
          function () {
            return index.getValue(entity)
              .catch(function (err) {
                if (err.name !== 'NOT_FOUND') {
                  throw err;
                }
              });
          },
          function (k) {
            if (typeof k === 'undefined' || k === null || (Array.isArray(k) && !k.length)) {
              return;
            }
            return iterator(index, name, k);
          });
      }
      return p.do(
        function () {
          if (name.indexOf(',') !== -1) {
            return name.split(',').map(function (name) {
              return entity.value[name];
            });
          } else {
            return entity.value[name];
          }
        },
        function (value) {
          var k;
          if (typeof value === 'undefined' || value === null || (Array.isArray(value) && !value.length)) {
            return;
          }
          k = util.format.apply(null, Array.isArray(value) ? [index.key].concat(value) : [index.key, value]);
          return iterator(index, name, k);
        });
    });
  });
};

Entity.prototype.iterateLinks = function (withValue, iterator) {
  var entity = this
    , clazz, id;
  if (typeof iterator === 'undefined' && typeof withValue === 'function') {
    iterator = withValue;
    withValue = true;
  }
  withValue = !!withValue;
  return p.do(function () {
    clazz = entity.getClass();
    id = entity.getId();
    if (!clazz.links) {
      return;
    }
    return p.forIn(clazz.links, function (link, name) {
      var value;
      value = entity.value[name];
      if (withValue) {
        if (typeof value === 'undefined' || value === null || (Array.isArray(value) && !value.length)) {
          return;
        }
        return p.each(Array.isArray(value) ? value : [value], function (value) {
          var k, reverseK;
          k = util.format.call(null, link.key, value);
          if (link.reverseKey) {
            reverseK = util.format.call(null, link.reverseKey, id);
          }
          return iterator(link, name, k, reverseK, value);
        });
      }
      return p.do(function () {
        var k = util.format.call(null, link.reverseKey, id);
        return iterator(link, name, k);
      });
    });
  });
};

Entity.prototype.loadLinks = function () {
  var entity = this
    , type, id;
  return p.do(function () {
    type = entity.type;
    id = entity.getId();
    logger.enabledLevels.debug && log.debug('loading links for entity "%s" #%s', type, id);
    return entity.iterateLinks(false, function (link, name, k) {
      return p.do(function loadLinksFromEntity() {
        return ohm.exec(link.unique ? 'get' : 'smembers', k)
          .then(function (result) {
            entity.value[name] = result;
          });
      });
    });
  });
};

Entity.prototype.removeIndexes = function (multi) {
  var entity = this
    , localMulti, clazz, type, schema, id;
  localMulti = ohm.createLocalMulti.apply(null, arguments);
  return p.do(function () {
    clazz = entity.getClass();
    type = entity.type;
    schema = entity.getSchema();
    id = entity.getId();
    if (!clazz.indexes) {
      return;
    }
    logger.enabledLevels.debug && log.debug('removing indexes for entity "%s" #%s', type, id);
    return entity
      .iterateIndexes(function (index, name, k) {
        if (index.unique) {
          return ohm.execMulti(localMulti, 'del', k);
        } else {
          return ohm.execMulti(localMulti, 'srem', k, id);
        }
      })
      .then(ohm.processLocalMulti.bind(null, localMulti, multi));
  });
};

Entity.prototype.removeLinks = function (multi) {
  var entity = this
    , localMulti, type, id;
  localMulti = ohm.createLocalMulti.apply(null, arguments);
  return p.do(function () {
    type = entity.type;
    id = entity.getId();
    logger.enabledLevels.debug && log.debug('removing links for entity "%s" #%s', type, id);
    return entity
      .iterateLinks(function (link, name, k, reverseK, value) {
        return p.do(
          function removeLinkToEntity() {
            if (link.reverseUnique) {
              return ohm.execMulti(localMulti, 'del', k);
            } else {
              return ohm.execMulti(localMulti, 'srem', k, id);
            }
          },
          function removeLinkFromEntity() {
            if (!reverseK) {
              return;
            }
            if (link.unique) {
              return ohm.execMulti(localMulti, 'del', reverseK);
            } else {
              return ohm.execMulti(localMulti, 'srem', reverseK, value);
            }
          });
      })
      .then(ohm.processLocalMulti.bind(null, localMulti, multi));
  });
};

Entity.prototype.save = function (multi) {
  var entity = this
    , localMulti, clazz, type, id;
  localMulti = ohm.createLocalMulti.apply(null, arguments);
  return p.do(
    function check() {
      clazz = entity.getClass();
      type = entity.type;
      id = entity.getId();
      logger.enabledLevels.debug && log.debug('saving new "%s" entity', type);
      return p.do(function check() {
        return ohm.validateSchema(entity.value, type, 'new')
          .catch(function (err) {
            e.throw('BAD_FORMAT', {schemaErrors: err});
          });
      });
    },
    function generateId() {
      return entity.generateId();
    },
    function checkIndexesUnicity() {
      return entity.checkIndexes();
    },
    function updateRefs() {
      return entity.saveIndexes(localMulti)
        .then(entity.saveLinks.bind(entity, localMulti));
    },
    function saveEntity() {
      var result, ignoreProps, data, k;
      result = ohm.filterProperties(entity.value, type, 'save');
      k = ohm.toHash(type, entity.getId());
      ignoreProps = Object.keys(clazz.links);
      if (!clazz.isObject) {
        ignoreProps = ignoreProps.concat(ohm.getSchemaId(type));
      }
      data = ohm.toRedis(_.omit(result, ignoreProps), type);
      if (typeof data === 'object') {
        data = _.pick(data, function (value) {
          return value !== null;
        });
      }
      return ohm.execMulti(localMulti, clazz.isObject ? 'hmset' : 'set', k, data)
        .then(function () {
          if (entity.ttl) {
            result.ttl = entity.ttl;
            return ohm.execMulti(localMulti, 'expire', k, entity.ttl);
          }
        });
    },
    ohm.processLocalMulti.bind(null, localMulti, multi),
    function () {
      return entity.loadLinks();
    },
    function () {
      entity.value = ohm.filterProperties(entity.value, type, 'get');
      return entity;
    }
  );
};

Entity.prototype.saveIndexes = function (multi) {
  var entity = this
    , localMulti, clazz, type, id;
  localMulti = ohm.createLocalMulti.apply(null, arguments);
  return p.do(function () {
    clazz = entity.getClass();
    type = entity.type;
    id = entity.getId();
    if (!clazz.indexes) {
      return;
    }
    logger.enabledLevels.debug && log.debug('saving indexes for entity "%s" #%s', type, id);
    return entity
      .iterateIndexes(function (index, name, k) {
        if (index.unique) {
          return ohm.execMulti(localMulti, 'set', k, id)
            .then(function () {
              if (entity.ttl || index.ttl) {
                return ohm.execMulti(localMulti, 'expire', k, entity.ttl || index.ttl);
              }
            });
        } else {
          return ohm.execMulti(localMulti, 'sadd', k, id);
        }
      })
      .then(ohm.processLocalMulti.bind(null, localMulti, multi));
  });
};

Entity.prototype.saveLinks = function (multi) {
  var entity = this
    , localMulti, type, id;
  localMulti = ohm.createLocalMulti.apply(null, arguments);
  return p.do(function () {
    type = entity.type;
    id = entity.getId();
    logger.enabledLevels.debug && log.debug('saving links for entity "%s" #%s', type, id);
    return entity
      .iterateLinks(function (link, name, k, reverseK, value) {
        return p.do(
          function saveLinkToEntity() {
            if (link.reverseUnique) {
              return ohm.execMulti(localMulti, 'set', k, id)
                .then(function () {
                  if (entity.ttl || link.ttl) {
                    return ohm.execMulti(localMulti, 'expire', k, entity.ttl || link.ttl);
                  }
                });
            } else {
              return ohm.execMulti(localMulti, 'sadd', k, id);
            }
          },
          function saveLinkFromEntity() {
            if (!reverseK) {
              return;
            }
            if (link.unique) {
              return ohm.execMulti(localMulti, 'set', reverseK, value)
                .then(function () {
                  if (entity.ttl || link.ttl) {
                    return ohm.execMulti(localMulti, 'expire', k, entity.ttl || link.ttl);
                  }
                });
            } else {
              return ohm.execMulti(localMulti, 'sadd', reverseK, value);
            }
          });
      })
      .then(ohm.processLocalMulti.bind(null, localMulti, multi));
  });
};

Entity.prototype.update = function (multi) {
  var entity = this
    , localMulti, type, clazz, id, existing;
  localMulti = ohm.createLocalMulti.apply(null, arguments);
  return p.do(
    function check() {
      type = entity.type;
      clazz = entity.getClass();
      id = entity.getId();
      logger.enabledLevels.debug && log.debug('updating "%s" entity  #%s', type, id);
      return ohm.validateSchema(entity.value, type, 'save')
        .catch(function (err) {
          e.throw('BAD_FORMAT', {schemaErrors: err});
        });
    },
    function () {
      return clazz.load(id)
        .then(function (result) {
          existing = result;
        });
    },
    function removeRefs() {
      return existing.removeLinks(localMulti)
        .then(existing.removeIndexes.bind(existing, localMulti));
    },
    function updateRefs() {
      _.defaults(entity.value, existing.value);
      return entity.saveIndexes(localMulti)
        .then(entity.saveLinks.bind(entity, localMulti));
    },
    function saveEntity() {
      var result, ignoreProps, data, k;
      result = ohm.filterProperties(entity.value, type, 'save');
      k = ohm.toHash(type, entity.getId());
      ignoreProps = Object.keys(clazz.links);
      if (!clazz.isObject) {
        ignoreProps = ignoreProps.concat(ohm.getSchemaId(type));
      }
      data = ohm.toRedis(_.omit(result, ignoreProps), type);
      if (typeof data === 'object') {
        data = _.pick(data, function (value) {
          return value !== null;
        });
      }
      return p.do(
        function () {
          return ohm.execMulti(localMulti, clazz.isObject ? 'hmset' : 'set', k, data);
        },
        function () {
          if (entity.ttl) {
            return ohm.execMulti(localMulti, 'expire', k, entity.ttl);
          }
        },
        ohm.processLocalMulti.bind(null, localMulti, multi),
        function () {
          entity.value = ohm.filterProperties(entity.value, type, 'get');
          return entity;
        }
      );
    });
};

Entity.prototype.validate = function (operation) {
  return ohm.validateSchema(this.value, this.type, operation);
};

exports = module.exports = Entity;