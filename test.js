'use strict';

import { assert } from 'chai';
import AnySQLKeyValueStore from './src';

async function catchError(fn) {
  let err;
  try {
    await fn();
  } catch (e) {
    err = e;
  }
  return err;
}

describe('AnySQLKeyValueStore', function() {
  let store;

  before(function() {
    this.timeout(30000);
    store = new AnySQLKeyValueStore('mysql://test@localhost/test');
  });

  it('should put, get and delete an object', async function() {
    let key = ['users', 'mvila'];
    await store.put(key, { firstName: 'Manu', age: 42 });
    let user = await store.get(key);
    assert.deepEqual(user, { firstName: 'Manu', age: 42 });
    let hasBeenDeleted = await store.delete(key);
    assert.isTrue(hasBeenDeleted);
    user = await store.get(key, { errorIfMissing: false });
    assert.isUndefined(user);
    hasBeenDeleted = await store.delete(key, { errorIfMissing: false });
    assert.isFalse(hasBeenDeleted);
  });

  describe('transactions', function() {
    it('should commit the transaction when no error occurs', async function() {
      let key = ['users', 'mvila'];
      let user = { firstName: 'Manu', age: 42 };
      await store.put(key, user);
      assert.isFalse(store.insideTransaction);
      await store.transaction(async function(transaction) {
        assert.isTrue(transaction.insideTransaction);
        user = await transaction.get(key);
        assert.strictEqual(user.firstName, 'Manu');
        user.firstName = 'Vince';
        await transaction.put(key, user);
        user = await transaction.get(key);
        assert.strictEqual(user.firstName, 'Vince');
      });
      user = await store.get(key);
      assert.strictEqual(user.firstName, 'Vince');
      await store.delete(key);
    });

    it('should rollback the transaction when an error occurs', async function() {
      let key = ['users', 'mvila'];
      let user = { firstName: 'Manu', age: 42 };
      await store.put(key, user);
      assert.isFalse(store.insideTransaction);
      let err = await catchError(async function() {
        await store.transaction(async function(transaction) {
          assert.isTrue(transaction.insideTransaction);
          user = await transaction.get(key);
          assert.strictEqual(user.firstName, 'Manu');
          user.firstName = 'Vince';
          await transaction.put(key, user);
          user = await transaction.get(key);
          assert.strictEqual(user.firstName, 'Vince');
          throw new Error('something is wrong');
        });
      });
      assert.instanceOf(err, Error);
      assert.equal(err.message, 'something is wrong');
      user = await store.get(key);
      assert.strictEqual(user.firstName, 'Manu');
    });
  });

  after(async function() {
    await store.findAndDelete();
    await store.close();
  });
});
