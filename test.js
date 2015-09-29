'use strict';

import { assert } from 'chai';
import AnySQLStoreLayer from './src';

async function catchError(fn) {
  let err;
  try {
    await fn();
  } catch (e) {
    err = e;
  }
  return err;
}

describe('AnySQLStoreLayer', function() {
  let store;

  before(function() {
    store = new AnySQLStoreLayer('mysql://test@localhost/test');
  });

  it('should put, get and del an object', async function() {
    let key = ['users', 'mvila'];
    await store.put(key, { firstName: 'Manu', age: 42 });
    let user = await store.get(key);
    assert.deepEqual(user, { firstName: 'Manu', age: 42 });
    let hasBeenDeleted = await store.del(key);
    assert.isTrue(hasBeenDeleted);
    user = await store.get(key, { errorIfMissing: false });
    assert.isUndefined(user);
    hasBeenDeleted = await store.del(key, { errorIfMissing: false });
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
      await store.del(key);
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
    await store.delRange();
    await store.close();
  });
});
