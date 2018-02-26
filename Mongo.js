const MongoClient = require('mongodb').MongoClient;

class Mongo {

  async connect(config) {
    this.config = config;
    for (let db of config.dbs) {
      if (!this[db] || !this[db].serverConfig.isConnected()) {
        this[db] = await MongoClient.connect(config.connectionString + db, config.options);
      }
    }
  }

  getCollection(dbName, collectionName) {
    if (!this[dbName] || !this[dbName].serverConfig.isConnected()) {
      throw new Error('mongo connect error');
    }
    return this[dbName].collection(collectionName);
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
