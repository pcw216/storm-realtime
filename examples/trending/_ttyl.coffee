SCOPE = /^\/alm-.+\/(.+)$/
storm = require('node-storm')
async = require('async')
q = require('q')
_ = require('lodash')
mongo = require('./mongo')


getCollection = ()->
	index = false
	return mongo().collection('changesByEntity').then (collection)->
		if index then return collection
		deferred = q.defer()
		
 		# 24 hrs expiration
		collection.ensureIndex {timestamp: 1}, {expireAfterSeconds: 86400 }, (err)->
			if err? then return deferred.reject(err)
			deferred.resolve(collection)
		return deferred.promise

module.exports = 
	bolt: ()->
		bolt = storm.oncebolt (data, callback)->
			[message] = data.tuple

			if not message.transaction.user?
				@log('no user, must be a system change')
				return callback()

			getCollection().then((collection)=>
				[message] = data.tuple
				server = message.transaction.bag
				timestamp = message.transaction.timestamp
				
				async.eachLimit(_.keys(message.entities), 1, (uuid, callback)=>
					entity = message.entities[uuid]
					type = entity.scope.match(SCOPE)[1]
					
					record = {
						timestamp: new Date(timestamp)
						server: server
						uuid: uuid
						type: type
						entity: entity
					}
					collection.insert(record, callback)
					@log("recorded change for uuid: #{uuid}")
				, callback)
			).fail callback
		return bolt
