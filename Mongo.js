const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectId;

class Mongo {

  async connect(config) {
    this.config = config;
    let client = await MongoClient.connect(config.connectionString, config.options);
    for (let db of config.dbs) {
      if (!this[db] || !this[db].serverConfig.isConnected()) {
        this[db] = client.db(db);
      }
    }
  }

  getCollection(dbName, collectionName) {
    dbName = dbName.toString();
    collectionName = collectionName.toString();
    if (!this[dbName] || !this[dbName].serverConfig.isConnected()) {
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
    if (!this[dbName] || !this[dbName].serverConfig.isConnected()) {
      throw new Error('mongo connect error');
    }
    let collections = await this[dbName].listCollections().toArray();
    return collections.map(item => item.name);
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
