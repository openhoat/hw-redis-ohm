const chai = require('chai')
  , expect = chai.expect
  , _ = require('lodash')
  , util = require('util')
  , p = require('hw-promise')
  , logger = require('hw-logger')
  , ohm = require('../lib/ohm')
  , tUtil = require('./test-util');
//, log = logger.log

describe('hw-redis-ohm', () => {

  before(() => {
    logger.registerLevels({redis: 6});
  });

  describe('ohm life cycle', () => {

    it('should start and stop', () => p.do(
      () => ohm.start().then(result => {
        expect(result).to.be.true;
      }),
      () => tUtil.cleanStore()
        .finally(() => ohm.stop())
    ));

    it('should start with callback', done => {
      ohm.start((err, result) => {
        expect(err).to.not.be.ok;
        expect(result).to.be.true;
        ohm.stop((err, result) => {
          expect(err).to.not.be.ok;
          expect(result).to.be.true;
          done();
        });
      });
    });

    it('should try to stop when not started', done => {
      ohm.stop((err, result) => {
        expect(err).to.not.be.ok;
        expect(result).to.be.false;
        done();
      });
    });

    it('should not start twice', () => p.do(
      () => ohm.start().then(result => {
        expect(result).to.be.true;
      }),
      () => tUtil.cleanStore(),
      () => ohm.start()
        .then(result => {
          expect(result).to.be.false;
        })
        .finally(() => ohm.stop())
    ));

  });

  describe('ohm features', () => {

    before(() => p.do(
      () => ohm.start(),
      () => tUtil.cleanStore()
    ));

    after(() => p.do(
      () => ohm.stop()
    ));

    afterEach(() => tUtil.cleanStore());

    describe('exec', () => {

      it('should execute redis command', () => {
        const key = ohm.toHash('hello')
          , value = 'world';
        return p.do(
          () => ohm.exec('set', key, value),
          result => {
            expect(result).to.equal('OK');
          },
          () => ohm.exec('get', key),
          result => {
            expect(result).to.equal(value);
          });
      });

      it('should fail to execute unsupported redis command', () => {
        const key = ohm.toHash('hello')
          , value = 'world';
        return ohm.exec('unknowncommand', key, value)
          .catch(err => {
            expect(err).to.be.an.instanceof(ohm.e.UnsupportedOhmError);
            expect(err).to.have.property('name', 'UnsupportedOhmError');
            expect(err).to.have.property('message', 'unsupported operation "unknowncommand" error');
            expect(err.toString()).to.equal('UnsupportedOhmError: unsupported operation "unknowncommand" error');
          });
      });

      it('should fail to execute hmset with a boolean', () => {
        const key = ohm.toHash('hello');
        return ohm.exec('hmset', key, true)
          .catch(err => {
            expect(err).to.be.an.instanceof(ohm.e.RedisOhmError);
            expect(err).to.have.property('name', 'RedisOhmError');
            expect(err).to.have.property('message', 'redis error "ReplyError: ERR wrong number of arguments for \'hmset\' command"');
            expect(err.toString()).to.equal('RedisOhmError: redis error "ReplyError: ERR wrong number of arguments for \'hmset\' command"');
          });
      });

      it('should fail to execute unsupported redis multi command', () => {
        const multi = ohm.multi()
          , key = ohm.toHash('hello')
          , value = 'world';
        return ohm.execMulti(multi, 'unknowncommand', key, value)
          .catch(err => {
            expect(err).to.be.an.instanceof(ohm.e.UnsupportedOhmError);
            expect(err).to.have.property('name', 'UnsupportedOhmError');
            expect(err).to.have.property('message', 'unsupported operation "unknowncommand" error');
            expect(err.toString()).to.equal('UnsupportedOhmError: unsupported operation "unknowncommand" error');
          });
      });

      it('should fail to execute multi hmset with a boolean', () => {
        const multi = ohm.multi()
          , key = ohm.toHash('hello');
        return ohm.execMulti(multi, 'hmset', key, true)
          .then(() => ohm.processMulti(multi))
          .catch(err => {
            expect(err).to.be.an.instanceof(ohm.e.RedisOhmError);
            expect(err).to.have.property('name', 'RedisOhmError');
            expect(err).to.have.property('message', 'redis error "ReplyError: EXECABORT Transaction discarded because of previous errors."');
            expect(err).to.have.deep.property('extra.redisError.errors[0].message', 'ERR wrong number of arguments for \'hmset\' command');
            expect(err.toString()).to.equal('RedisOhmError: redis error "ReplyError: EXECABORT Transaction discarded because of previous errors."');
          });
      });

    });

    describe('transaction', () => {
      const keys = [ohm.toHash('hello'), ohm.toHash('foo')]
        , values = ['world', 'bar'];

      it('should execute multi', () => {
        const multi = ohm.multi();
        return p.do(
          () => p.map(keys, (key, index) => ohm.execMulti(multi, 'set', key, values[index])),
          results => {
            expect(results).to.be.an('array').of.length(values.length);
            return ohm.processMulti(multi);
          },
          results => {
            expect(results).to.be.an('array').of.length(values.length);
            results.forEach(result => {
              expect(result).to.equal('OK');
            });
          },
          () => p.map(keys, key => ohm.execMulti(multi, 'get', key)),
          results => {
            expect(results).to.be.an('array').of.length(values.length);
            return ohm.processMulti(multi);
          },
          results => {
            expect(results).to.be.an('array').of.length(values.length);
            results.forEach((result, index) => {
              expect(result).to.equal(values[index]);
            });
          });
      });

    });

    describe('pub sub', () => {
      const subChannels = ['sub1', 'sub2']
        , messages = [['hello', 'world'], ['foo', 'bar']];
      it('should subscribe and publish', () => {
        const counters = _.fill(Array(subChannels.length), 0);
        return p.do(
          () => p.map(counters, (counter, index) => new p(resolve => {
            ohm
              .subscribe(subChannels[index], (channel, message) => {
                expect(message).to.equal(messages[index][counters[index]++]);
                resolve();
              })
              .spread((channel, count) => {
                expect(channel).to.equal(subChannels[index]);
                expect(count).to.equal(1);
              })
              .then(() => p.map(messages[index], message => ohm.publish(subChannels[index], message)));
          }))
          ,
          () => p.each(subChannels, subChannel => ohm.unsubscribe(subChannel)
            .then(() => ohm.unsubscribe(subChannel))
          ),
          () => ohm.unpublish(),
          () => ohm.unpublish()
        );
      });
    });

  });

  describe('schemas', () => {
    const schemas = {
      group: {
        title: 'Group JSON schema',
        type: 'object',
        properties: {
          value: {type: 'string'}
        },
        meta: {
          idGenerator: 'increment',
          indexes: [{name: 'value', unique: true}],
          links: [{
            type: 'hasMany',
            target: 'contact',
            as: 'contactIds',
            foreignKey: 'groupIds'
          }],
          operations: {
            db: {
              new: {
                required: ['value']
              }
            }
          }
        }
      },
      contact: {
        title: 'Contact JSON schema',
        type: 'object',
        properties: {
          firstname: {type: 'string'},
          lastname: {type: 'string'},
          username: {type: 'string'},
          password: {type: 'string'},
          email: {type: 'string', format: 'email'}
        },
        meta: {
          idGenerator: 'increment',
          indexes: [
            {name: 'email', unique: true},
            {name: 'lastname'}
          ],
          links: [{
            type: 'hasMany',
            target: 'group',
            as: 'groupIds',
            foreignKey: 'contactIds'
          }, {
            type: 'hasMany',
            target: 'contact',
            as: 'friendIds',
            foreignKey: 'friendIds'
          }, {
            type: 'hasOne',
            target: 'dog',
            as: 'dogId',
            foreignKey: 'masterId',
            unique: true
          }],
          operations: {
            db: {
              new: {
                required: ['username', 'password', 'email']
              },
              get: {
                excludeProperties: ['password']
              }
            }
          }
        }
      },
      dog: {
        title: 'Dog JSON schema',
        type: 'object',
        properties: {
          value: {type: 'string'}
        },
        meta: {
          idGenerator: 'increment',
          indexes: [{name: 'value', unique: true}],
          links: [{
            type: 'hasOne',
            target: 'contact',
            as: 'masterId',
            foreignKey: 'dogId',
            unique: true
          }],
          operations: {
            db: {
              new: {
                includeProperties: ['value'],
                extraProperties: {
                  description: {type: 'string'}
                },
                required: ['value']
              }
            }
          }
        }
      },
      version: {
        type: 'string'
      }
    };

    before(() => p.do(
      () => ohm.start({schemas}),
      () => tUtil.cleanStore()
    ));

    after(() => p.do(
      () => ohm.stop()
    ));

    afterEach(() => tUtil.cleanStore());

    it('should have schemas', () => {
      expect(ohm.schemas).to.be.ok;
      expect(ohm.schemas).to.have.property('group').that.eql({
        title: 'Group JSON schema main default',
        type: 'object',
        properties: {
          id: {type: 'string', pattern: '^[0-9]+$'},
          value: {type: 'string'},
          contactIds: {
            type: 'array',
            items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
          }
        },
        meta: {
          idGenerator: 'increment',
          indexes: [{name: 'value', unique: true}],
          links: [{type: 'hasMany', target: 'contact', as: 'contactIds', foreignKey: 'groupIds'}],
          operations: {
            db: {
              new: {
                required: ['value'],
                title: 'Group JSON schema db new',
                type: 'object',
                properties: {
                  contactIds: {
                    type: 'array',
                    items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
                  },
                  value: {type: 'string'}
                }
              },
              save: {
                required: ['id'],
                minProperties: 2,
                title: 'Group JSON schema db save',
                type: 'object',
                properties: {
                  value: {type: 'string'},
                  id: {type: 'string', pattern: '^[0-9]+$'}
                }
              },
              get: {
                title: 'Group JSON schema db get',
                type: 'object',
                properties: {
                  contactIds: {
                    type: 'array',
                    items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
                  },
                  value: {type: 'string'},
                  id: {type: 'string', pattern: '^[0-9]+$'}
                }
              }
            }
          }
        }
      });
      expect(ohm.schemas).to.have.property('contact').that.eql({
        title: 'Contact JSON schema main default',
        type: 'object',
        properties: {
          firstname: {type: 'string'},
          lastname: {type: 'string'},
          username: {type: 'string'},
          password: {type: 'string'},
          email: {type: 'string', format: 'email'},
          id: {type: 'string', pattern: '^[0-9]+$'},
          groupIds: {
            type: 'array',
            items: {
              type: ['string', 'null'],
              pattern: '^[0-9]+$'
            }
          },
          friendIds: {
            type: 'array',
            items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
          },
          dogId: {
            type: ['string', 'null'],
            pattern: '^[0-9]+$'
          }
        },
        meta: {
          idGenerator: 'increment',
          indexes: [{name: 'email', unique: true}, {name: 'lastname'}],
          links: [{
            type: 'hasMany',
            target: 'group',
            as: 'groupIds',
            foreignKey: 'contactIds'
          }, {
            type: 'hasMany',
            target: 'contact',
            as: 'friendIds',
            foreignKey: 'friendIds'
          }, {
            type: 'hasOne',
            target: 'dog',
            as: 'dogId',
            foreignKey: 'masterId',
            unique: true
          }],
          operations: {
            db: {
              new: {
                required: ['username', 'password', 'email'],
                title: 'Contact JSON schema db new',
                type: 'object',
                properties: {
                  firstname: {type: 'string'},
                  lastname: {type: 'string'},
                  username: {type: 'string'},
                  password: {type: 'string'},
                  email: {type: 'string', format: 'email'},
                  groupIds: {
                    type: 'array',
                    items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
                  },
                  friendIds: {
                    type: 'array',
                    items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
                  },
                  dogId: {
                    type: ['string', 'null'],
                    pattern: '^[0-9]+$'
                  }
                }
              },
              get: {
                title: 'Contact JSON schema db get',
                type: 'object',
                properties: {
                  firstname: {type: 'string'},
                  lastname: {type: 'string'},
                  username: {type: 'string'},
                  email: {type: 'string', format: 'email'},
                  id: {type: 'string', pattern: '^[0-9]+$'},
                  groupIds: {
                    type: 'array',
                    items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
                  },
                  friendIds: {
                    type: 'array',
                    items: {type: ['string', 'null'], pattern: '^[0-9]+$'}
                  },
                  dogId: {type: ['string', 'null'], pattern: '^[0-9]+$'}
                }
              },
              save: {
                title: 'Contact JSON schema db save',
                type: 'object',
                required: ['id'],
                minProperties: 2,
                properties: {
                  firstname: {type: 'string'},
                  lastname: {type: 'string'},
                  username: {type: 'string'},
                  password: {type: 'string'},
                  email: {type: 'string', format: 'email'},
                  id: {type: 'string', pattern: '^[0-9]+$'}
                }
              }
            }
          }
        }
      });
      expect(ohm.schemas).to.have.property('dog').that.eql({
        title: 'Dog JSON schema main default',
        type: 'object',
        properties: {
          value: {type: 'string'},
          id: {type: 'string', pattern: '^[0-9]+$'},
          masterId: {
            type: ['string', 'null'],
            pattern: '^[0-9]+$'
          }
        },
        meta: {
          idGenerator: 'increment',
          indexes: [{name: 'value', unique: true}],
          links: [{
            type: 'hasOne',
            target: 'contact',
            as: 'masterId',
            foreignKey: 'dogId',
            unique: true
          }],
          operations: {
            db: {
              new: {
                required: ['value'],
                title: 'Dog JSON schema db new',
                type: 'object',
                properties: {
                  value: {type: 'string'},
                  description: {type: 'string'}
                }
              },
              save: {
                title: 'Dog JSON schema db save',
                type: 'object',
                required: ['id'],
                minProperties: 2,
                properties: {
                  value: {type: 'string'},
                  id: {type: 'string', pattern: '^[0-9]+$'}
                }
              },
              get: {
                title: 'Dog JSON schema db get',
                type: 'object',
                properties: {
                  value: {type: 'string'},
                  id: {type: 'string', pattern: '^[0-9]+$'},
                  masterId: {
                    type: ['string', 'null'],
                    pattern: '^[0-9]+$'
                  }
                }
              }
            }
          }
        }
      });
      expect(ohm.schemas).to.not.have.property('version');
    });

    it('should get dog schema', () => {
      const schema = ohm.getSchema('dog');
      expect(schema).to.be.ok;
      expect(schema).to.have.property('idGenerator');
      expect(schema).to.have.property('indexes').that.is.an('array');
      expect(schema).to.have.property('links').that.is.an('array');
      expect(schema).to.have.property('operations');
    });

    it('should fail to get unknown schema', () => {
      expect(ohm.getSchema.bind(ohm, 'unknown')).to.throw(ohm.e.EntitySchemaNotFoundError);
    });

    describe('entities', () => {

      it('should create, save, read and delete entities', () => {
        const groups =
          [
            {value: 'vip'},
            {value: 'admin'}
          ], contacts =
          [
            {username: 'johndoe', password: 'secret', firstname: 'john', lastname: 'doe', email: 'john@doe.com'},
            {username: 'janedoe', password: 'secret', firstname: 'jane', lastname: 'doe', email: 'jane@doe.com'}
          ], dogs =
          [
            {value: 'rex'}
          ]
          , groupEntities = []
          , contactEntities = []
          , dogEntities = [];
        return p.do(
          () => p.map(groups, value => {
            const entity = ohm.entityClasses.Group.create(value);
            groupEntities.push(entity);
            return entity.save().then(result => {
              expect(result).to.eql(entity);
              expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
            });
          }),
          () => p.map(groupEntities, groupEntity => ohm.entityClasses.Group.load(groupEntity.getId())
            .then(result => {
              groupEntity.value.contactIds = result.value.contactIds;
              expect(result).to.eql(groupEntity);
            })
          ),
          () => {
            const groupEntity = groupEntities[0];
            groupEntity.value.value = 'VIP';
            return ohm.entityClasses.Group.update(groupEntity.value).then(result => {
              expect(result).to.eql(groupEntity);
            });
          },
          () => {
            contacts.forEach((contact, index) => {
              const groupIds = [];
              for (let i = 0; i < index + 1; i++) {
                groupIds.push(groupEntities[i].value.id);
              }
              contact.groupIds = groupIds;
            });
          },
          () => p.map(contacts, value => {
            const entity = ohm.entityClasses.Contact.create(value);
            contactEntities.push(entity);
            return entity.save().then(result => {
              expect(result).to.eql(entity);
              expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
            });
          }),
          () => {
            const entity = ohm.entityClasses.Contact.create(contacts[0]);
            return new p(resolve => {
              entity.save().nodeify(err => {
                expect(err).to.have.property('name', 'EntityConflictError');
                expect(err).to.have.deep.property('extra.type', 'contact');
                expect(err).to.have.deep.property('extra.attrName', 'email');
                expect(err).to.have.deep.property('extra.attrValue', contacts[0].email);
                expect(err.toString()).to.equal(util.format('EntityConflictError: entity "contact" conflict for "email" with value "%s"', contacts[0].email));
                resolve();
              });
            });
          },
          () => p.map(dogs, (value, index) => {
            const entity = ohm.entityClasses.Dog.create(value);
            entity.value.masterId = contactEntities[index].getId();
            dogEntities.push(entity);
            return entity.save().then(result => {
              contactEntities[index].value.dogId = result.getId();
              expect(result).to.eql(entity);
              expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
            });
          }),
          () => {
            const entity = ohm.entityClasses.Dog.create({value: 'ted', masterId: contactEntities[0].getId()});
            return new p(resolve => {
              entity.save().nodeify(err => {
                expect(err).to.have.property('name', 'EntityConflictError');
                expect(err).to.have.deep.property('extra.type', 'dog');
                expect(err).to.have.deep.property('extra.attrName', 'masterId');
                expect(err).to.have.deep.property('extra.attrValue', contactEntities[0].getId());
                expect(err.toString()).to.equal(util.format('EntityConflictError: entity "dog" conflict for "masterId" with value "%s"', contactEntities[0].getId()));
                resolve();
              });
            });
          },
          () => new p(resolve => {
            ohm.entityClasses.Group.load('badid').nodeify(err => {
              expect(err).to.have.property('name', 'EntityNotFoundError');
              expect(err).to.have.deep.property('extra.type', 'group');
              expect(err).to.have.deep.property('extra.attrName', 'id');
              expect(err).to.have.deep.property('extra.attrValue', 'badid');
              expect(err.toString()).to.equal('EntityNotFoundError: entity "group" not found for "id" with value "badid"');
              resolve();
            });
          }),
          () => p.map(groupEntities, groupEntity => ohm.entityClasses.Group.load(groupEntity.getId())
            .then(result => {
              groupEntity.value.contactIds = result.value.contactIds;
              expect(result).to.eql(groupEntity);
            })
          ),
          () => ohm.entityClasses.Group.list('id').then(result => {
            expect(result).to.eql(groupEntities);
          }),
          () => {
            const entity = _.first(contactEntities);
            return ohm.entityClasses.Contact.load(entity.getId()).then(result => {
              expect(result).to.eql(entity);
            });
          },
          () => ohm.entityClasses.Contact.findByIndex('dogId', dogEntities[0].getId()).then(result => {
            expect(result).to.be.an('array').of.length(1);
          }),
          () => ohm.entityClasses.Contact.findByIndex('unknown', 'hello').then(result => {
            expect(result).to.be.an('array').of.length(0);
          }),
          () => ohm.entityClasses.Contact.findByIndex('email', 'unknown@doe.com').then(result => {
            expect(result).to.be.an('array').of.length(0);
          }),
          () => ohm.entityClasses.Contact.findByIndex('email', 'john@doe.com').then(result => {
            expect(result).to.be.an('array').of.length(1);
            expect(_.first(result)).to.eql(_.first(contactEntities));
          }),
          () => ohm.entityClasses.Contact.findByIndex('lastname', 'doe').then(result => {
            expect(result).to.be.an('array').of.length(2);
          }),
          () => ohm.entityClasses.Contact.findByIndex('groupIds', groupEntities[0].getId()).then(result => {
            expect(result).to.be.an('array').of.length(2);
            expect(_.first(result)).to.eql(contactEntities[0]);
            expect(result[1]).to.eql(contactEntities[1]);
          }),
          () => ohm.entityClasses.Contact.findByIndex('groupIds', 'badid').then(result => {
            expect(result).to.be.an('array').of.length(0);
          }),
          () => ohm.entityClasses.Contact.findByIndex('groupIds', groupEntities[1].getId()).then(result => {
            expect(result).to.be.an('array').of.length(1);
            expect(_.first(result)).to.eql(contactEntities[1]);
          }),
          () => new p(resolve => {
            ohm.entityClasses.Group.delete('badid').nodeify(err => {
              expect(err).to.have.property('name', 'EntityNotFoundError');
              expect(err).to.have.deep.property('extra.type', 'group');
              expect(err).to.have.deep.property('extra.attrName', 'id');
              expect(err).to.have.deep.property('extra.attrValue', 'badid');
              expect(err.toString()).to.equal('EntityNotFoundError: entity "group" not found for "id" with value "badid"');
              resolve();
            });
          }),
          () => p.map(groupEntities, entity => ohm.entityClasses.Group.delete(entity.getId())),
          () => p.map(contactEntities, entity => ohm.entityClasses.Contact.delete(entity.getId())),
          () => new p(resolve => {
            delete groupEntities[0].value.id;
            groupEntities[0].delete().nodeify(err => {
              expect(err).to.have.property('name', 'EntityValidationError');
              expect(err).to.have.deep.property('extra.type', 'group');
              expect(err).to.have.deep.property('extra.attrName', 'id');
              expect(err).to.have.deep.property('extra.attrValue').that.is.undefined;
              expect(err.toString()).to.equal('EntityValidationError: entity "group" validation failed for "id" with value "undefined"');
              resolve();
            });
          }),
          () => p.map(dogEntities, entity => ohm.entityClasses.Dog.delete(entity.getId()))
        );
      });

      it('should save contact with optional attrs', () => {
        const entity = ohm.entityClasses.Contact.create({
          username: 'undoe',
          password: 'secret',
          email: 'un@doe.com',
          firstname: ''
        });
        return entity.save().then(result => ohm.entityClasses.Contact.load(result.getId())
          .then(result => {
            expect(result).to.have.property('value');
            expect(result.value).to.not.have.property('firstname');
          }));
      });

    });

  });

});