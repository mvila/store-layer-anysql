'use strict';

import assert from 'assert';
import AbstractStoreLayer from 'store-layer-abstract';
import AnySQL from 'anysql';
import setImmediatePromise from 'set-immediate-promise';

const TABLE_NAME = 'pairs';
const DEFAULT_LIMIT = 50000;
const RESPIRATION_RATE = 250;

export class AnySQLStoreLayer extends AbstractStoreLayer {
  constructor(url, options) {
    super(options);
    this.database = new AnySQL(url);
  }

  async initializeDatabase() {
    if (this._databaseHasBeenInitialized) return;
    assert(!this.insideTransaction, 'Cannot initialize the database inside a transaction');
    let definition = '`key` longblob NOT NULL, ';
    definition += '`value` longblob, ';
    definition += 'PRIMARY KEY (`key`(256))';
    let options = { errorIfExists: false };
    await this.database.createTable(TABLE_NAME, definition, options);
    this.root._databaseHasBeenInitialized = true;
  }

  async get(key, { errorIfMissing = true } = {}) {
    key = this.normalizeKey(key);
    await this.initializeDatabase();
    let sql = 'SELECT `value` FROM `pairs` WHERE `key`=?';
    let res = await this.database.query(sql, [this.encodeKey(key)]);
    if (!res.length) {
      if (errorIfMissing) throw new Error('Item not found');
      return undefined;
    }
    return this.decodeValue(res[0].value);
  }

  async put(key, value, { createIfMissing = true, errorIfExists = false } = {}) {
    key = this.normalizeKey(key);
    await this.initializeDatabase();
    let encodedKey = this.encodeKey(key);
    let encodedValue = this.encodeValue(value);
    let sql;
    if (errorIfExists) {
      sql = 'INSERT INTO `pairs` (`key`, `value`) VALUES(?,?)';
      await this.database.query(sql, [encodedKey, encodedValue]);
    } else if (createIfMissing) {
      sql = 'REPLACE INTO `pairs` (`key`, `value`) VALUES(?,?)';
      await this.database.query(sql, [encodedKey, encodedValue]);
    } else {
      sql = 'UPDATE `pairs` SET `value`=? WHERE `key`=?';
      let res = await this.database.query(sql, [encodedValue, encodedKey]);
      if (!res.affectedRows) throw new Error('Item not found');
    }
  }

  async del(key, { errorIfMissing = true } = {}) {
    key = this.normalizeKey(key);
    await this.initializeDatabase();
    let sql = 'DELETE FROM `pairs` WHERE `key`=?';
    let res = await this.database.query(sql, [this.encodeKey(key)]);
    if (!res.affectedRows && errorIfMissing) {
      throw new Error('Item not found (key=\'' + JSON.stringify(key) + '\')');
    }
    return !!res.affectedRows;
  }

  async getMany(keys, { errorIfMissing = true, returnValues = true } = {}) {
    if (!Array.isArray(keys)) throw new Error('Invalid keys (should be an array)');
    if (!keys.length) return [];
    keys = keys.map(this.normalizeKey, this);

    let iterationsCount = 0;

    await this.initializeDatabase();

    let results;
    let resultsMap = {};

    let encodedKeys = keys.map(this.encodeKey, this);
    while (encodedKeys.length) {
      let someKeys = encodedKeys.splice(0, 500); // take 500 keys
      if (someKeys.length) {
        let placeholders = '';
        for (let i = 0; i < someKeys.length; i++) {
          if (i > 0) placeholders += ',';
          placeholders += '?';
        }
        let what = returnValues ? '*' : '`key`';
        let where = '`key` IN (' + placeholders + ')';
        let sql = 'SELECT ' + what + ' FROM `pairs` WHERE ' + where;
        results = await this.database.query(sql, someKeys);
        for (let item of results) {
          let key = this.decodeKey(item.key);
          let res = { key };
          if (returnValues) res.value = this.decodeValue(item.value);
          resultsMap[key.toString()] = res;
          if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
        }
      }
    }

    results = [];
    for (let key of keys) {
      let res = resultsMap[key.toString()];
      if (res) results.push(res);
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }

    if (results.length !== keys.length && errorIfMissing) {
      throw new Error('Some items not found');
    }

    return results;
  }

  async putMany(items, {} = {}) { // eslint-disable-line
    // TODO
  }

  async delMany(key, {} = {}) { // eslint-disable-line
    // TODO
  }

  // options: prefix, start, startAfter, end, endBefore,
  //   reverse, limit, returnValues
  async getRange(options = {}) {
    options = this.normalizeKeySelectors(options);
    options = Object.assign({ limit: DEFAULT_LIMIT, returnValues: true }, options);
    let iterationsCount = 0;
    await this.initializeDatabase();
    let what = options.returnValues ? '*' : '`key`';
    let sql = 'SELECT ' + what + ' FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    sql += ' ORDER BY `key`' + (options.reverse ? ' DESC' : '');
    sql += ' LIMIT ' + options.limit;
    let items = await this.database.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    let decodedItems = [];
    for (let item of items) {
      let decodedItem = { key: this.decodeKey(item.key) };
      if (options.returnValues) decodedItem.value = this.decodeValue(item.value);
      decodedItems.push(decodedItem);
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    return decodedItems;
  }

  // options: prefix, start, startAfter, end, endBefore
  async countRange(options = {}) {
    options = this.normalizeKeySelectors(options);
    await this.initializeDatabase();
    let sql = 'SELECT COUNT(*) FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    let res = await this.database.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    if (res.length !== 1) throw new Error('Invalid result');
    if (!res[0].hasOwnProperty('COUNT(*)')) throw new Error('Invalid result');
    return res[0]['COUNT(*)'];
  }

  async delRange(options = {}) {
    options = this.normalizeKeySelectors(options);
    await this.initializeDatabase();
    let sql = 'DELETE FROM `pairs` WHERE `key` BETWEEN ? AND ?';
    let res = await this.database.query(sql, [
      this.encodeKey(options.start),
      this.encodeKey(options.end)
    ]);
    return res.affectedRows;
  }

  async transaction(fn) {
    if (this.insideTransaction) return await fn(this);
    await this.initializeDatabase();
    return await this.database.transaction(async function(databaseTransaction) {
      let transaction = Object.create(this);
      transaction.database = databaseTransaction;
      return await fn(transaction);
    }.bind(this));
  }

  close() {
    return this.database.close();
  }
}

export default AnySQLStoreLayer;
