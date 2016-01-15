'use strict';

let log = window.log = function(message) {
  let div = document.createElement('div');
  div.appendChild(document.createTextNode(message));
  document.body.appendChild(div);
};

log('AnySQLKeyValueStore Example');

window.addEventListener('error', function(err) {
  log(err.message);
}, false);

import { assert } from 'chai';
import AnySQLKeyValueStore from '../src';

let store = new AnySQLKeyValueStore('cordova-sqlite:example');

(async function() {
  log('=== put, get and delete an object ===');
  let key = ['users', 'mvila'];
  await store.put(key, { firstName: 'Manu', age: 42 });
  let user = await store.get(key);
  log(JSON.stringify(user));
  assert.deepEqual(user, { firstName: 'Manu', age: 42 });
  let hasBeenDeleted = await store.delete(key);
  assert.isTrue(hasBeenDeleted);
  user = await store.get(key, { errorIfMissing: false });
  assert.isUndefined(user);
  hasBeenDeleted = await store.delete(key, { errorIfMissing: false });
  assert.isFalse(hasBeenDeleted);
})().catch(function(err) {
  log(err.message);
});
