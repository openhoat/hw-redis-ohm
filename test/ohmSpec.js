'use strict';

var chai = require('chai')
  , expect = chai.expect
  , _ = require('lodash')
  , p = require('hw-promise')
  , ohm = require('../lib/ohm')
  , logger = require('hw-logger')
  , log = logger.log
  , tUtil = require('./test-util');

logger.registerLevels({redis: 6});
logger.setLevel('redis');

describe('hw-redis-ohm', function () {

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
    var schemas, Group, Contact, Dog;

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
                  required: ['email']
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
              foreignKey: 'dogIds',
              unique: true
            }],
            operations: {
              db: {
                new: {
                  required: ['value']
                }
              }
            }
          }
        }
      };
      return p.do(
        function () {
          return ohm.start({schemas: schemas});
        },
        function () {
          Group = ohm.getEntityClass('group');
          Contact = ohm.getEntityClass('contact');
          Dog = ohm.getEntityClass('dog');
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

    afterEach(function () {
      return tUtil.cleanStore();
    });

    describe('entities', function () {

      it('should create, save, read and delete entities', function () {
        var groups = [
            {value: 'vip'},
            {value: 'admin'}
          ]
          , contacts = [
            {firstname: 'john', lastname: 'doe', email: 'john@doe.com'},
            {firstname: 'jane', lastname: 'doe', email: 'jane@doe.com'}
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
              var entity = Group.create(value);
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
            return Group.update(groupEntity.value).then(function (result) {
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
              var entity = Contact.create(value);
              contactEntities.push(entity);
              return entity.save().then(function (result) {
                expect(result).to.eql(entity);
                expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
              });
            });
          },
          function saveSameContact() {
            var entity = Contact.create(contacts[0]);
            return new p(function (resolve) {
              entity.save().nodeify(function (err) {
                expect(err).to.have.property('name', 'CONFLICT');
                resolve();
              });
            });
          },
          function saveDogs() {
            return p.map(dogs, function (value, index) {
              var entity = Dog.create(value);
              entity.value.masterId = contactEntities[index].getId();
              dogEntities.push(entity);
              return entity.save().then(function (result) {
                expect(result).to.eql(entity);
                expect(entity.getId()).to.match(new RegExp(ohm.patterns.id));
              });
            });
          },
          function saveSameDog() {
            var entity = Dog.create({value: 'ted', masterId: contactEntities[0].getId()});
            return new p(function (resolve) {
              entity.save().nodeify(function (err) {
                expect(err).to.have.property('name', 'CONFLICT');
                resolve();
              });
            });
          },
          function loadGroupFromBadId() {
            return new p(function (resolve) {
              Group.load('badid').nodeify(function (err) {
                expect(err).to.have.property('name', 'NOT_FOUND');
                resolve();
              });
            });
          },
          function loadGroup() {
            return p.map(groupEntities, function (groupEntity) {
              return Group.load(groupEntity.getId()).then(function (result) {
                groupEntity.value.contactIds = result.value.contactIds;
                expect(result).to.eql(groupEntity);
              });
            });
          },
          function listGroups() {
            return Group.list('id').then(function (result) {
              expect(result).to.eql(groupEntities);
            });
          },
          function loadContact() {
            var entity = _.first(contactEntities);
            return Contact.load(entity.getId()).then(function (result) {
              expect(result).to.eql(entity);
            });
          },
          function findContactOfDog() {
            return Contact.findByIndex('dogIds', dogEntities[0].getId()).then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function findByContactUnknown() {
            return Contact.findByIndex('unknown', 'hello').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function findContactByBadEmail() {
            return Contact.findByIndex('email', 'unknown@doe.com').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function findContactByEmail() {
            return Contact.findByIndex('email', 'john@doe.com').then(function (result) {
              expect(result).to.be.an('array').of.length(1);
              expect(_.first(result)).to.eql(_.first(contactEntities));
            });
          },
          function findContactByLastname() {
            return Contact.findByIndex('lastname', 'doe').then(function (result) {
              expect(result).to.be.an('array').of.length(2);
            });
          },
          function findContactByGroup() {
            return Contact.findByIndex('groupIds', groupEntities[0].getId()).then(function (result) {
              expect(result).to.be.an('array').of.length(2);
              expect(_.first(result)).to.eql(contactEntities[0]);
              expect(_.rest(result)[0]).to.eql(contactEntities[1]);
            });
          },
          function findContactByBadGroup() {
            return Contact.findByIndex('groupIds', 'badid').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function findContactByOtherGroup() {
            return Contact.findByIndex('groupIds', groupEntities[1].getId()).then(function (result) {
              expect(result).to.be.an('array').of.length(1);
              expect(_.first(result)).to.eql(contactEntities[1]);
            });
          },
          function deleteGroupByBadId() {
            return new p(function (resolve) {
              Group.delete('badid').nodeify(function (err) {
                expect(err).to.have.property('name', 'NOT_FOUND');
                resolve();
              });
            });
          },
          function deleteGroups() {
            return p.map(groupEntities, function (entity) {
              return Group.delete(entity.getId()).then(function (result) {
                expect(result).to.equal(1);
              });
            });
          },
          function deleteContacts() {
            return p.map(contactEntities, function (entity) {
              return Contact.delete(entity.getId()).then(function (result) {
                expect(result).to.equal(1);
              });
            });
          },
          function deleteBadEntity() {
            return new p(function (resolve) {
              delete groupEntities[0].value.id;
              groupEntities[0].delete().nodeify(function (err) {
                expect(err).to.have.property('name', 'BAD_FORMAT');
                resolve();
              });
            });
          }
        );
      });

    });

  });

});