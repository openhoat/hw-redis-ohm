'use strict';

const Promise = require('bluebird');
const chai = require('chai');
const _ = require('lodash');
const util = require('util');
const logger = require('hw-logger');
const ohm = require('../lib/ohm');
const tUtil = require('./test-util');
const expect = chai.expect;
//const log = logger.log;

describe('hw-redis-ohm', () => {

  before(() => {
    logger.registerLevels({redis: 6});
  });

  describe('ohm life cycle', () => {

    it('should start and stop', () => ohm.start()
      .then(result => {
        expect(result).to.be.true;
      })
      .then(() => tUtil.cleanStore())
      .finally(() => ohm.stop())
    );

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

    it('should not start twice', () => ohm.start()
      .then(result => {
        expect(result).to.be.true;
      })
      .then(() => tUtil.cleanStore())
      .then(() => ohm.start())
      .then(result => {
        expect(result).to.be.false;
      })
      .finally(() => ohm.stop())
    );

  });

  describe('ohm features', () => {

    before(() => ohm.start()
      .then(() => tUtil.cleanStore())
    );

    after(() => ohm.stop());

    afterEach(() => tUtil.cleanStore());

    describe('exec', () => {

      it('should execute redis command', () => {
        const key = ohm.toHash('hello')
          , value = 'world';
        return ohm.exec('set', key, value)
          .then(result => {
            expect(result).to.equal('OK');
          })
          .then(() => ohm.exec('get', key))
          .then(result => {
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
            expect(err).to.have.property('message',
              'redis error "ReplyError: ERR wrong number of arguments for \'hmset\' command"'
            );
            expect(err.toString()).to
              .equal('RedisOhmError: redis error "ReplyError: ERR wrong number of arguments for \'hmset\' command"');
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
            expect(err).to.have.property('message',
              'redis error "ReplyError: EXECABORT Transaction discarded because of previous errors."'
            );
            expect(err).to.have.deep.property('extra.redisError.errors[0].message',
              'ERR wrong number of arguments for \'hmset\' command'
            );
            expect(err.toString()).to
              .equal('RedisOhmError: redis error ' +
                '"ReplyError: EXECABORT Transaction discarded because of previous errors."'
              );
          });
      });

    });

    describe('transaction', () => {
      const keys = [ohm.toHash('hello'), ohm.toHash('foo')]
        , values = ['world', 'bar'];

      it('should execute multi', () => {
        const multi = ohm.multi();
        return Promise.map(keys, (key, index) => ohm.execMulti(multi, 'set', key, values[index]))
          .then(results => {
            expect(results).to.be.an('array').of.length(values.length);
            return ohm.processMulti(multi);
          })
          .then(results => {
            expect(results).to.be.an('array').of.length(values.length);
            results.forEach(result => {
              expect(result).to.equal('OK');
            });
          })
          .then(() => Promise.map(keys, key => ohm.execMulti(multi, 'get', key)))
          .then(results => {
            expect(results).to.be.an('array').of.length(values.length);
            return ohm.processMulti(multi);
          })
          .then(results => {
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
        const counters = new Array(subChannels.length).fill(0);
        return Promise.map(counters, (counter, index) =>
            new Promise(resolve => {
              ohm
                .subscribe(subChannels[index], (channel, message) => {
                  expect(message).to.equal(messages[index][counters[index]++]);
                  resolve();
                })
                .spread((channel, count) => {
                  expect(channel).to.equal(subChannels[index]);
                  expect(count).to.equal(1);
                })
                .then(() => Promise.map(messages[index], message => ohm.publish(subChannels[index], message)));
            })
          )
          .then(() => Promise.each(subChannels, subChannel => ohm.unsubscribe(subChannel)
            .then(() => ohm.unsubscribe(subChannel))
          ))
          .then(() => ohm.unpublish())
          .then(() => ohm.unpublish()
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
          firstname: {type: ['string', 'null']},
          lastname: {type: ['string', 'null']},
          username: {type: 'string'},
          password: {type: 'string'},
          email: {type: 'string', format: 'email'}
        },
        meta: {
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
                includeProperties: ['id', 'masterId', 'value'],
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

    before(() => ohm.start({schemas})
      .then(() => tUtil.cleanStore())
    );

    after(() => ohm.stop());

    afterEach(() => tUtil.cleanStore());

    it('should have schemas', () => {
      expect(ohm.schemas).to.be.ok;
      expect(ohm.schemas).to.have.property('group').that.eql({
        title: 'Group JSON schema main default',
        type: 'object',
        properties: {
          value: {type: 'string'},
          id: ohm.idSchema,
          contactIds: {type: 'array', items: ohm.idSchema}
        },
        meta: {
          indexes: [{name: 'value', unique: true}],
          links: [{type: 'hasMany', target: 'contact', as: 'contactIds', foreignKey: 'groupIds'}],
          operations: {
            db: {
              new: {
                required: ['value'],
                title: 'Group JSON schema db new',
                type: 'object',
                properties: {
                  value: {type: 'string'},
                  id: ohm.idSchema,
                  contactIds: {type: 'array', items: ohm.idSchema}
                }
              },
              save: {
                required: ['id'],
                minProperties: 2,
                title: 'Group JSON schema db save',
                type: 'object',
                properties: {
                  value: {type: 'string'},
                  id: ohm.idSchema,
                  contactIds: {type: 'array', items: ohm.idSchema}
                }
              },
              get: {
                title: 'Group JSON schema db get',
                type: 'object',
                properties: {
                  value: {type: 'string'},
                  id: ohm.idSchema,
                  contactIds: {type: 'array', items: ohm.idSchema}
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
          firstname: {type: ['string', 'null']},
          lastname: {type: ['string', 'null']},
          username: {type: 'string'},
          password: {type: 'string'},
          email: {type: 'string', format: 'email'},
          id: ohm.idSchema,
          groupIds: {type: 'array', items: ohm.idSchema},
          friendIds: {type: 'array', items: ohm.idSchema},
          dogId: ohm.idNullableSchema
        },
        meta: {
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
                  firstname: {type: ['string', 'null']},
                  lastname: {type: ['string', 'null']},
                  username: {type: 'string'},
                  password: {type: 'string'},
                  email: {type: 'string', format: 'email'},
                  id: ohm.idSchema,
                  groupIds: {type: 'array', items: ohm.idSchema},
                  friendIds: {type: 'array', items: ohm.idSchema},
                  dogId: ohm.idNullableSchema
                }
              },
              get: {
                title: 'Contact JSON schema db get',
                type: 'object',
                properties: {
                  firstname: {type: ['string', 'null']},
                  lastname: {type: ['string', 'null']},
                  username: {type: 'string'},
                  email: {type: 'string', format: 'email'},
                  id: ohm.idSchema,
                  groupIds: {type: 'array', items: ohm.idSchema},
                  friendIds: {type: 'array', items: ohm.idSchema},
                  dogId: ohm.idNullableSchema
                }
              },
              save: {
                title: 'Contact JSON schema db save',
                type: 'object',
                required: ['id'],
                minProperties: 2,
                properties: {
                  firstname: {type: ['string', 'null']},
                  lastname: {type: ['string', 'null']},
                  username: {type: 'string'},
                  password: {type: 'string'},
                  email: {type: 'string', format: 'email'},
                  id: ohm.idSchema,
                  groupIds: {type: 'array', items: ohm.idSchema},
                  friendIds: {type: 'array', items: ohm.idSchema},
                  dogId: ohm.idNullableSchema
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
          id: ohm.idSchema,
          masterId: ohm.idNullableSchema
        },
        meta: {
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
                  description: {type: 'string'},
                  id: ohm.idSchema,
                  masterId: ohm.idNullableSchema
                }
              },
              save: {
                title: 'Dog JSON schema db save',
                type: 'object',
                required: ['id'],
                minProperties: 2,
                properties: {
                  value: {type: 'string'},
                  id: ohm.idSchema,
                  masterId: ohm.idNullableSchema
                }
              },
              get: {
                title: 'Dog JSON schema db get',
                type: 'object',
                properties: {
                  value: {type: 'string'},
                  id: ohm.idSchema,
                  masterId: ohm.idNullableSchema
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
      expect(schema).to.have.property('indexes').that.is.an('array');
      expect(schema).to.have.property('links').that.is.an('array');
      expect(schema).to.have.property('operations');
    });

    it('should fail to get unknown schema', () => {
      expect(() => ohm.getSchema('unknown')).to.throw(ohm.e.EntitySchemaNotFoundError);
    });

    describe('entities', () => {

      it('should create, save, read and delete entities', () => {
        const groups = [
          {value: 'vip'},
          {value: 'admin'}
        ];
        const contacts = [
          {username: 'johndoe', password: 'secret', firstname: 'john', lastname: 'doe', email: 'john@doe.com'},
          {username: 'janedoe', password: 'secret', firstname: 'jane', lastname: 'doe', email: 'jane@doe.com'}
        ];
        const dogs = [
          {value: 'rex'}
        ];
        const groupEntities = [];
        const contactEntities = [];
        const dogEntities = [];
        return Promise
          .map(groups, value => {
            const entity = ohm.entityClasses.Group.create(value);
            groupEntities.push(entity);
            return entity.save()
              .then(result => {
                expect(result).to.eql(entity);
                expect(entity.getId()).to.match(new RegExp(ohm.idPattern));
              });
          })
          .then(() => new Promise(
            resolve => {
              groupEntities[0].save().asCallback(err => {
                expect(err).to.be.an.instanceof(ohm.e.EntityConflictError);
                expect(err).to.have.deep.property('extra.type', 'group');
                expect(err).to.have.deep.property('extra.attrName', 'value');
                expect(err).to.have.deep.property('extra.attrValue', 'vip');
                resolve();
              });
            })
          )
          .then(() => Promise
            .map(groupEntities, groupEntity => ohm.entityClasses.Group
              .load(groupEntity.getId())
              .then(result => {
                groupEntity.value.contactIds = result.value.contactIds;
                expect(result).to.eql(groupEntity);
              })
            )
          )
          .then(() => {
            const groupEntity = groupEntities[0];
            groupEntity.value.value = 'VIP';
            return ohm.entityClasses.Group.update(groupEntity.value)
              .then(result => {
                expect(result).to.eql(groupEntity);
              });
          })
          .then(() => {
            contacts.forEach((contact, index) => {
              const groupIds = [];
              for (let i = 0; i < index + 1; i++) {
                groupIds.push(groupEntities[i].value.id);
              }
              contact.groupIds = groupIds;
            });
          })
          .then(() => Promise
            .map(contacts, value => {
              const entity = ohm.entityClasses.Contact.create(value);
              contactEntities.push(entity);
              return entity.save()
                .then(result => {
                  expect(result).to.eql(entity);
                  expect(entity.getId()).to.match(new RegExp(ohm.idPattern));
                });
            })
          )
          .then(() => {
            const entity = ohm.entityClasses.Contact.create(contacts[0]);
            return new Promise(resolve => {
              entity.save().asCallback(err => {
                expect(err).to.have.property('name', 'EntityConflictError');
                expect(err).to.have.deep.property('extra.type', 'contact');
                expect(err).to.have.deep.property('extra.attrName', 'email');
                expect(err).to.have.deep.property('extra.attrValue', contacts[0].email);
                expect(err.toString()).to.equal(
                  util.format('EntityConflictError: entity "contact" conflict for "email" with value "%s"',
                    contacts[0].email
                  )
                );
                resolve();
              });
            });
          })
          .then(() => Promise
            .map(dogs, (value, index) => {
              const entity = ohm.entityClasses.Dog.create(value);
              entity.value.masterId = contactEntities[index].getId();
              dogEntities.push(entity);
              return entity.save()
                .then(result => {
                  contactEntities[index].value.dogId = result.getId();
                  expect(result).to.eql(entity);
                  expect(entity.getId()).to.match(new RegExp(ohm.idPattern));
                });
            })
          )
          .then(() => {
            const entity = ohm.entityClasses.Dog.create({value: 'ted', masterId: contactEntities[0].getId()});
            return new Promise(resolve => {
              entity.save().asCallback(err => {
                expect(err).to.have.property('name', 'EntityConflictError');
                expect(err).to.have.deep.property('extra.type', 'dog');
                expect(err).to.have.deep.property('extra.attrName', 'masterId');
                expect(err).to.have.deep.property('extra.attrValue', contactEntities[0].getId());
                expect(err.toString()).to.equal(
                  util.format('EntityConflictError: entity "dog" conflict for "masterId" with value "%s"',
                    contactEntities[0].getId()
                  )
                );
                resolve();
              });
            });
          })
          .then(() => new Promise(
            resolve => {
              ohm.entityClasses.Group.load('badid').asCallback(err => {
                expect(err).to.have.property('name', 'EntityNotFoundError');
                expect(err).to.have.deep.property('extra.type', 'group');
                expect(err).to.have.deep.property('extra.attrName', 'id');
                expect(err).to.have.deep.property('extra.attrValue', 'badid');
                expect(err.toString()).to
                  .equal('EntityNotFoundError: entity "group" not found for "id" with value "badid"');
                resolve();
              });
            })
          )
          .then(() => Promise
            .map(groupEntities, groupEntity => ohm.entityClasses.Group
              .load(groupEntity.getId())
              .then(result => {
                groupEntity.value.contactIds = result.value.contactIds;
                expect(result).to.eql(groupEntity);
              })
            )
          )
          .then(() => ohm.entityClasses.Group.list('id'))
          .then(result => {
            expect(result).to.eql(_.sortBy(groupEntities, 'value.id'));
          })
          .then(() => {
            const entity = _.first(contactEntities);
            return ohm.entityClasses.Contact.load(entity.getId())
              .then(result => {
                expect(result).to.eql(entity);
              });
          })
          .then(() => ohm.entityClasses.Contact.findByIndex('dogId', dogEntities[0].getId()))
          .then(result => {
            expect(result).to.be.an('array').of.length(1);
          })
          .then(() => ohm.entityClasses.Contact.findByIndex('unknown', 'hello'))
          .then(result => {
            expect(result).to.be.an('array').of.length(0);
          })
          .then(() => ohm.entityClasses.Contact.findByIndex('email', 'unknown@doe.com'))
          .then(result => {
            expect(result).to.be.an('array').of.length(0);
          })
          .then(() => ohm.entityClasses.Contact.findByIndex('email', 'john@doe.com'))
          .then(result => {
            expect(result).to.be.an('array').of.length(1);
            expect(_.first(result)).to.eql(_.first(contactEntities));
          })
          .then(() => ohm.entityClasses.Contact.findByIndex('lastname', 'doe'))
          .then(result => {
            expect(result).to.be.an('array').of.length(2);
          })
          .then(() => ohm.entityClasses.Contact.findByIndex('groupIds', groupEntities[0].getId()))
          .then(result => {
            expect(result).to.be.an('array').of.length(2);
            result = _.sortBy(result, 'value.id');
            const expectedContacts = _.sortBy(contactEntities, 'value.id');
            expect(_.first(result)).to.eql(expectedContacts[0]);
            expect(result[1]).to.eql(expectedContacts[1]);
          })
          .then(() => ohm.entityClasses.Contact
            .findByIndex('groupIds', 'badid')
            .then(result => {
              expect(result).to.be.an('array').of.length(0);
            })
          )
          .then(() => ohm.entityClasses.Contact
            .findByIndex('groupIds', groupEntities[1].getId())
            .then(result => {
              expect(result).to.be.an('array').of.length(1);
              expect(_.first(result)).to.eql(contactEntities[1]);
            })
          )
          .then(() =>
            new Promise(resolve => {
              ohm.entityClasses.Group.delete('badid').asCallback(err => {
                expect(err).to.have.property('name', 'EntityNotFoundError');
                expect(err).to.have.deep.property('extra.type', 'group');
                expect(err).to.have.deep.property('extra.attrName', 'id');
                expect(err).to.have.deep.property('extra.attrValue', 'badid');
                expect(err.toString()).to
                  .equal('EntityNotFoundError: entity "group" not found for "id" with value "badid"');
                resolve();
              });
            })
          )
          .then(() => Promise
            .map(groupEntities, entity => ohm.entityClasses.Group.delete(entity.getId()))
          )
          .then(() => Promise
            .map(contactEntities, entity => ohm.entityClasses.Contact.delete(entity.getId()))
          )
          .then(() => new Promise(
            resolve => {
              delete groupEntities[0].value.id;
              groupEntities[0].delete().asCallback(err => {
                expect(err).to.have.property('name', 'EntityValidationError');
                expect(err).to.have.deep.property('extra.type', 'group');
                expect(err).to.have.deep.property('extra.attrName', 'id');
                expect(err).to.have.deep.property('extra.attrValue').that.is.undefined;
                expect(err.toString()).to
                  .equal('EntityValidationError: entity "group" validation failed for "id" with value "undefined"');
                resolve();
              });
            })
          )
          .then(() => Promise
            .map(dogEntities, entity => ohm.entityClasses.Dog.delete(entity.getId()))
          );
      });

      it('should save contact with optional attrs', () => {
        const entity = ohm.entityClasses.Contact.create({
          username: 'undoe',
          password: 'secret',
          email: 'un@doe.com',
          firstname: 'john'
        });
        return entity.save()
          .then(result => ohm.entityClasses.Contact.load(result.getId()))
          .then(result => {
            expect(result).to.have.property('value');
            expect(result.value).to.have.property('firstname', 'john');
            return result;
          })
          .then(entity => { // should remove firstname property
            entity.value.password = 'secret';
            entity.value.firstname = null;
            return entity.update();
          })
          .then(result => ohm.entityClasses.Contact.load(result.getId()))
          .then(result => {
            expect(result).to.have.property('value');
            expect(result.value).to.not.have.property('firstname');
            return result;
          });
      });

    });

  });

});
