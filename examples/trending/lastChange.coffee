SCOPE = /^\/alm-.+\/(.+)$/
storm = require('node-storm')
redis = require('redis')
async = require('async')
_ = require('lodash')

module.exports = 
	bolt: ()->
		client = null
		bolt = storm.asyncbolt (data, callback)->
			# @log(['lastChangeMessage'])
			client ?= redis.createClient()

			[message] = data.tuple
			server = message.transaction.bag
			timestamp = "#{message.transaction.timestamp}"
			

			async.eachLimit(_.keys(message.entities), 1, (uuid, callback)=>
				# @log([uuid])
				entity = message.entities[uuid]
				type = entity.scope.match(SCOPE)[1]
				client.multi()
					.hget(uuid, 'timestamp')
					.hset(uuid, 'timestamp', timestamp)
					.exec (err, results)=>
						if err? then return callback(err)
						[old, status] = results
						if timestamp isnt old
							@emit [{
								uuid: uuid
								type: type
								current: timestamp
								previous: old
							}]
						callback()
			, callback)

		bolt.declareOutputFields(['timestamp'])
		return bolt
