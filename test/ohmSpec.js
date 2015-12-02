'use strict';

var chai = require('chai')
  , expect = chai.expect
  , _ = require('lodash')
  , p = require('hw-promise')
  , ohm = require('../lib/ohm')
  , logger = require('hw-logger')
  , log = logger.log
  , tUtil = require('./test-util');

describe('hw-redis-ohm', function () {

  before(function () {
    logger.registerLevels({redis: 6});
    logger.setLevel('redis');
    log.info('logger initialized');
  });

  describe('ohm life cycle', function () {

    it('should start and stop', function () {
      return p.do(
        function () {
          return ohm.start().then(function (result) {
            expect(result).to.be.true;
          });
        },
        function () {
          return tUtil.cleanStore();
        })
        .finally(function () {
          return ohm.stop();
        });
    });

    it('should start with callback', function (done) {
      ohm.start(function (err, result) {
        expect(err).to.not.be.ok;
        expect(result).to.be.true;
        ohm.stop(function (err, result) {
          expect(err).to.not.be.ok;
          expect(result).to.be.true;
          done();
        });
      });
    });

    it('should try to stop when not started', function (done) {
      ohm.stop(function (err, result) {
        expect(err).to.not.be.ok;
        expect(result).to.be.false;
        done();
      });
    });

    it('should not start twice', function () {
      return p.do(
        function () {
          return ohm.start().then(function (result) {
            expect(result).to.be.true;
          });
        },
        function () {
          return tUtil.cleanStore();
        },
        function () {
          return ohm.start().then(function (result) {
            expect(result).to.be.false;
          });
        })
        .finally(function () {
          return ohm.stop();
        });
    });

  });

  describe('schemas', function () {
    var schemas;

    before(function () {
      schemas = {
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
      return p.do(
        function () {
          return ohm.start({schemas: schemas});
        },
        function () {
          return tUtil.cleanStore();
        });
    });

    after(function () {
      return p.do(
        function () {
          return ohm.stop();
        });
    });

    /*afterEach(function () {
     return tUtil.cleanStore();
     });*/

    it('should return schemas', function () {
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

    describe('entities', function () {

      it('should create, save, read and delete entities', function () {
        var groups = [
            {value: 'vip'},
            {value: 'admin'}
          ]
          , contacts = [
            {username: 'johndoe', password: 'secret', firstname: 'john', lastname: 'doe', email: 'john@doe.com'},
            {username: 'janedoe', password: 'secret', firstname: 'jane', lastname: 'doe', email: 'jane@doe.com'}
          ]
          , dogs = [
            {value: 'rex'}
          ]
          , groupEntities = []
          , contactEntities = []
          , dogEntities = [];
        return p.do(
          function saveGroups() {
            return p.map(groups, function (value) {
              var entity = ohm.entityClasses.Group.create(value);
              groupEntities.push(entity);
              return entity.save().then(function (result) {
                expect(result).to.eql(entity);
                expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
              });
            });
          },
          function updateGroup() {
            var groupEntity = groupEntities[0];
            groupEntity.value.value = 'VIP';
            return ohm.entityClasses.Group.update(groupEntity.value).then(function (result) {
              expect(result).to.eql(groupEntity);
            });
          },
          function associateContactsWithGroups() {
            contacts.forEach(function (contact, index) {
              var groupIds = [], i;
              for (i = 0; i < index + 1; i++) {
                groupIds.push(groupEntities[i].value.id);
              }
              contact.groupIds = groupIds;
            });
          },
          function saveContacts() {
            return p.map(contacts, function (value) {
              var entity = ohm.entityClasses.Contact.create(value);
              contactEntities.push(entity);
              return entity.save().then(function (result) {
                expect(result).to.eql(entity);
                expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
              });
            });
          },
          function saveSameContact() {
            var entity = ohm.entityClasses.Contact.create(contacts[0]);
            return new p(function (resolve) {
              entity.save().nodeify(function (err) {
                expect(err).to.have.property('name', 'ConflictError');
                resolve();
              });
            });
          },
          function saveDogs() {
            return p.map(dogs, function (value, index) {
              var entity = ohm.entityClasses.Dog.create(value);
              entity.value.masterId = contactEntities[index].getId();
              dogEntities.push(entity);
              return entity.save().then(function (result) {
                contactEntities[index].value.dogId = result.getId();
                expect(result).to.eql(entity);
                expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
              });
            });
          },
          function saveSameDog() {
            var entity = ohm.entityClasses.Dog.create({value: 'ted', masterId: contactEntities[0].getId()});
            return new p(function (resolve) {
              entity.save().nodeify(function (err) {
                console.log('err :', err);
                expect(err).to.have.property('name', 'ConflictError');
                resolve();
              });
            });
          },
          function loadGroupFromBadId() {
            return new p(function (resolve) {
              ohm.entityClasses.Group.load('badid').nodeify(function (err) {
                expect(err).to.have.property('name', 'NotFoundError');
                resolve();
              });
            });
          },
          function loadGroup() {
            return p.map(groupEntities, function (groupEntity) {
              return ohm.entityClasses.Group.load(groupEntity.getId()).then(function (result) {
                groupEntity.value.contactIds = result.value.contactIds;
                expect(result).to.eql(groupEntity);
              });
            });
          },
          function listGroups() {
            return ohm.entityClasses.Group.list('id').then(function (result) {
              expect(result).to.eql(groupEntities);
            });
          },
          function loadContact() {
            var entity = _.first(contactEntities);
            return ohm.entityClasses.Contact.load(entity.getId()).then(function (result) {
              expect(result).to.eql(entity);
            });
          },
          function findContactOfDog() {
            return ohm.entityClasses.Contact.findByIndex('dogId', dogEntities[0].getId()).then(function (result) {
              expect(result).to.be.an('array').of.length(1);
            });
          },
          function findByContactUnknown() {
            return ohm.entityClasses.Contact.findByIndex('unknown', 'hello').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function findContactByBadEmail() {
            return ohm.entityClasses.Contact.findByIndex('email', 'unknown@doe.com').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function findContactByEmail() {
            return ohm.entityClasses.Contact.findByIndex('email', 'john@doe.com').then(function (result) {
              expect(result).to.be.an('array').of.length(1);
              expect(_.first(result)).to.eql(_.first(contactEntities));
            });
          },
          function findContactByLastname() {
            return ohm.entityClasses.Contact.findByIndex('lastname', 'doe').then(function (result) {
              expect(result).to.be.an('array').of.length(2);
            });
          },
          function findContactByGroup() {
            return ohm.entityClasses.Contact.findByIndex('groupIds', groupEntities[0].getId()).then(function (result) {
              expect(result).to.be.an('array').of.length(2);
              expect(_.first(result)).to.eql(contactEntities[0]);
              expect(_.rest(result)[0]).to.eql(contactEntities[1]);
            });
          },
          function findContactByBadGroup() {
            return ohm.entityClasses.Contact.findByIndex('groupIds', 'badid').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function findContactByOtherGroup() {
            return ohm.entityClasses.Contact.findByIndex('groupIds', groupEntities[1].getId()).then(function (result) {
              expect(result).to.be.an('array').of.length(1);
              expect(_.first(result)).to.eql(contactEntities[1]);
            });
          },
          function deleteGroupByBadId() {
            return new p(function (resolve) {
              ohm.entityClasses.Group.delete('badid').nodeify(function (err) {
                expect(err).to.have.property('name', 'NotFoundError');
                resolve();
              });
            });
          },
          function deleteGroups() {
            return p.map(groupEntities, function (entity) {
              return ohm.entityClasses.Group.delete(entity.getId()).then(function (result) {
                expect(result).to.equal(1);
              });
            });
          },
          function deleteContacts() {
            return p.map(contactEntities, function (entity) {
              return ohm.entityClasses.Contact.delete(entity.getId()).then(function (result) {
                expect(result).to.equal(1);
              });
            });
          },
          function deleteBadEntity() {
            return new p(function (resolve) {
              delete groupEntities[0].value.id;
              groupEntities[0].delete().nodeify(function (err) {
                expect(err).to.have.property('name', 'BadFormatError');
                resolve();
              });
            });
          }
        );
      });

      it('should save contact with optional attrs', function () {
        var entity = ohm.entityClasses.Contact.create({
          username: 'undoe',
          password: 'secret',
          email: 'un@doe.com',
          firstname: ''
        });
        return entity.save().then(function (result) {
          return ohm.entityClasses.Contact.load(result.getId()).then(function (result) {
            expect(result).to.have.property('value');
            expect(result.value).to.not.have.property('firstname');
          });
        });
      });

    });

  });

});