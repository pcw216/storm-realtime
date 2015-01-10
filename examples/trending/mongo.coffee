q = require('q')
MongoClient = require('mongodb').MongoClient

db = null

module.exports = ()->
	if not db?
		db = q.defer()
		db.promise.collection = (collection)->
			return db.promise.then (client)-> return client.collection(collection)

		MongoClient.connect "mongodb://localhost:27017/trending", (err, client)->
			if err? then return db.reject(err)
			db.resolve(client)

	return db.promise
