q = require('q')
MongoClient = require('mongodb').MongoClient

_db = null

module.exports = 
	db: ()->
		if not _db?
			_db = q.defer()
			_db.promise.collection = (collection)->
				return _db.promise.then (client)-> return client.collection(collection)

			MongoClient.connect "mongodb://localhost:27017/trending", (err, client)->
				if err? then return _db.reject(err)
				_db.resolve(client)

		return _db.promise
