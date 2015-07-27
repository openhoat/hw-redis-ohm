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
          return ohm.start();
        },
        function () {
          return tUtil.cleanStore();
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
            name: {type: 'string'},
            description: {type: 'string'}
          },
          meta: {
            idGenerator: 'increment',
            indexes: [{name: 'name', unique: true}],
            links: [{
              type: 'hasMany',
              target: 'contact',
              as: 'contactIds',
              foreignKey: 'groupIds'
            }],
            operations: {
              db: {
                new: {
                  required: ['name']
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
            indexes: [{name: 'email', unique: true}],
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
              type: 'hasMany',
              target: 'dog',
              as: 'dogIds',
              foreignKey: 'masterId'
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
              foreignKey: 'dogIds'
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
          Group = ohm.getModelClass('group');
          Contact = ohm.getModelClass('contact');
          Dog = ohm.getModelClass('dog');
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

    describe('', function () {

      it('', function () {
        var group = Group.create({name: 'vip'});
        return group.save().then(function (result) {
          expect(result).to.eql(group);
        });
      });

    });

    xdescribe('groups', function () {

      it('should manage groups', function () {
        var groups, contacts;
        groups = [
          {name: 'vip'},
          {name: 'coders'}
        ];
        contacts = [
          {firstname: 'john', lastname: 'doe', email: 'john@doe.com'},
          {firstname: 'jane', lastname: 'doe', email: 'jane@doe.com'}
        ];
        return p.do(
          function () {
            return p.map(groups, function (group) {
              var entity;
              entity = ohm.create('group', group);
              return entity.save().then(function (result) {
                group.id = result.id;
              });
            });
          },
          function () {
            return Group.list().then(function (result) {
              expect(result).to.be.an('array').of.length(groups.length);
            });
          },
          function () {
            var groupIds = _.pluck(groups, 'id');
            contacts.map(function (contact) {
              contact.groupIds = groupIds;
            });
          },
          function () {
            return p.map(contacts, function (contact) {
              var entity;
              entity = ohm.create('contact', contact);
              return entity.save().then(function (result) {
                contact.id = result.id;
              });
            });
          },
          function () {
            return Contact.findByIndex('groupIds', '1').then(function (result) {
              expect(result).to.be.an('array').of.length(contacts.length);
            });
          },
          function () {
            return p.map(groups, function (group) {
              return Group.delete(group.id).then(function (result) {
                expect(result).to.equal(1);
              });
            });
          },
          function () {
            return p.map(contacts, function (group) {
              return Contact.delete(group.id).then(function (result) {
                expect(result).to.equal(1);
              });
            });
          });
      });

    });

    xdescribe('contacts', function () {

      it('should save contacts', function () {
        var contacts, entities;
        contacts = [
          {firstname: 'john', lastname: 'doe', email: 'john@doe.com'},
          {firstname: 'jane', lastname: 'doe', email: 'jane@doe.com'}
        ];
        return p.do(
          function () {
            entities = contacts.map(function (contact) {
              var entity;
              entity = ohm.create('contact', contact);
              expect(entity).to.be.ok;
              expect(entity).to.have.property('value').that.eql(contact);
              return entity;
            });
            return p.map(entities, function (entity) {
              return entity.save();
            });
          },
          function (result) {
            result.map(function (entity, index) {
              entities[index].value.id = entity.id;
              entities[index].value.friendIds = [result[(index + 1) % result.length].id];
            });
            return p.map(entities, function (entity) {
              return entity.update();
            });
          },
          function () {
            return p.all([
              Contact.findByIndex('email', 'john@doe.com').then(_.first),
              Contact.findByIndex('email', 'jane@doe.com').then(_.first)
            ])
              .then(function (result) {
                expect(result).to.be.an('array').of.length(2);
                result.forEach(function (item, index) {
                  expect(item).to.have.property('id');
                  expect(item).to.have.property('firstname', contacts[index].firstname);
                  expect(item).to.have.property('lastname', contacts[index].lastname);
                  expect(item).to.have.property('email', contacts[index].email);
                  expect(item).to.have.property('friendIds').that.eql([item.id === '1' ? '2' : '1']);
                  expect(item).to.have.property('dogIds').that.eql([]);
                  expect(item).to.have.property('groupIds').that.eql([]);
                });
              });
          },
          function () {
            return Contact.findByIndex('email', 'unknown@doe.com').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function () {
            return Contact.findByIndex('unknownProperty', 'hello').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function () {
            return Contact.findByIndex('friendIds', '99').then(function (result) {
              expect(result).to.be.an('array').of.length(0);
            });
          },
          function () {
            return p.map(entities, function (entity) {
              return entity.delete();
            });
          });
      });

    });

    xdescribe('dog', function () {

      it('should create,read, update, delete a dog', function () {
        var dogId, dog;
        return p.do(
          function save() {
            var entity;
            entity = ohm.create('dog', {value: 'rex'});
            return entity.save().then(function (result) {
              dogId = result.id;
            });
          },
          function () {
            return Dog.load(dogId).then(function (result) {
              expect(result).to.be.ok;
              expect(result).to.have.property('id');
              expect(result).to.have.property('value', 'rex');
              dog = result;
            });
          },
          function () {
            dog.value = 'ringo';
            return Dog.update(dog).then(function (result) {
              log.warn('result :', result);
              expect(result).to.be.ok;
              expect(result).to.have.property('id');
              expect(result).to.have.property('value', 'ringo');
              dog = result;
            });
          },
          function () {
            return Dog.load(dogId).then(function (result) {
              expect(result).to.be.ok;
              expect(result).to.have.property('id');
              expect(result).to.have.property('value', 'ringo');
              dog = result;
            });
          },
          function () {
            return Dog.delete(dogId).then(function (result) {
              expect(result).to.equal(1);
            });
          },
          function () {
            return p.fromNode(function (cb) {
              Dog.load(dogId).nodeify(function (err) {
                expect(err).to.be.an.instanceof(Error);
                expect(err).to.have.property('name', 'NOT_FOUND');
                cb();
              });
            });

          });
      });

    });

    xdescribe('contact with dogs', function () {

      it('should add a contact with a dog', function () {
        var contactId, dogId;
        return p.do(
          function saveContact() {
            var entity;
            entity = ohm.create('contact', {firstname: 'john', lastname: 'doe', email: 'john@doe.com'});
            return entity.save()
              .then(function (result) {
                contactId = result.id;
              });
          },
          function saveDog() {
            var entity;
            entity = ohm.create('dog', {value: 'rex'});
            return entity.save()
              .then(function (result) {
                dogId = result.id;
              });
          },
          function associate() {
            return Dog.load(dogId)
              .then(function (dog) {
                dog.masterId = contactId;
                return Dog.update(dog);
              });
          },
          function checkContact() {
            return Contact.load(contactId)
              .then(function (contact) {
                expect(contact).to.have.property('dogIds').that.eql([dogId]);
              });
          },
          function checkDog() {
            return Dog.load(dogId)
              .then(function (dog) {
                expect(dog).to.have.property('masterId').that.eql(contactId);
              });
          });
      });

      it('should add a contact with a group', function () {
        var contactId, groupId;
        return p.do(
          function saveContact() {
            var entity;
            entity = ohm.create('contact', {firstname: 'jane', lastname: 'doe', email: 'jane@doe.com'});
            return entity.save()
              .then(function (result) {
                contactId = result.id;
              });
          },
          function saveGroup() {
            var entity;
            entity = ohm.create('group', {name: 'VIP'});
            return entity.save()
              .then(function (result) {
                groupId = result.id;
              });
          },
          function associate() {
            return Group.load(groupId)
              .then(function (group) {
                group.contactIds = [contactId];
                return Group.update(group);
              });
          },
          function checkContact() {
            return Contact.load(contactId)
              .then(function (contact) {
                expect(contact).to.have.property('groupIds').that.eql([groupId]);
              });
          },
          function checkGroup() {
            return Group.load(groupId)
              .then(function (group) {
                expect(group).to.have.property('contactIds').that.eql([contactId]);
              });
          });
      });

    });

  });

});