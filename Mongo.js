const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectId;

class Mongo {

  constructor() {
    this.ObjectId = ObjectId;
  }

  async connect(config) {
    this.config = config;
    const client = new MongoClient(config.connectionString, config.options);
    await client.connect();
    for (let db of config.dbs) {
      if (!this[db]) {
        this[db] = client.db(db);
      }
    }
  }

  getCollection(dbName, collectionName) {
    dbName = dbName.toString();
    collectionName = collectionName.toString();
    if (!this[dbName]) {
      throw new Error('mongo connect error');
    }
    let collection = this[dbName].collection(collectionName);
    collection.findById = this.findById;
    return collection;
  }

  async findById(_id) {
    try {
      _id = new ObjectId(_id);
    } catch (e) {
      return null;
    }
    return this.findOne({_id});
  }

  async getCollectionNames(dbName) {
    dbName = dbName.toString();
    if (!this[dbName]) {
      throw new Error('mongo connect error');
    }
    let collections = await this[dbName].listCollections().toArray();
    return collections.map(item => item.name);
  }

  async createIndex(dbName, collectionName, indexName, fields, options = {}) {
    let collection = this.getCollection(dbName, collectionName);
    let indexes = await this.getIndexesByCollection(collection);
    if (!indexes.includes(indexName)) {
      await collection.createIndex(fields, options);
    }
  }

  async getIndexesByCollection(collection) {
    try {
      let indexes = await collection.indexes();
      return indexes.map(it => Object.keys(it.key).join());
    } catch (e) {
      return [];
    }
  }

  async getNextCounterByName(name) {
    let result = await this.getCollection(this.config.dbs[0], 'counters').findOneAndUpdate(
      {name: name},
      {$inc: {value: 1}},
      {upsert: true}
    );
    return result.value ? result.value.value : 0;
  }

}

module.exports = new Mongo();
