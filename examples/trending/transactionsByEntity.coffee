storm = require('node-storm')
async = require('async')
_ = require('lodash')

SCOPE = /^\/alm-.+\/(.+)$/

module.exports = 
	bolt: ()->
		bolt = storm.oncebolt (data, callback)->
			[message] = data.tuple

			server = message.transaction.bag
			timestamp = message.transaction.timestamp
			
			async.eachLimit(_.keys(message.entities), 1, (uuid, callback)=>
				entity = message.entities[uuid]
				transaction = _.extend({server, timestamp}, entity)
				transaction.changes = []
				transaction.type = entity.scope.match(SCOPE)[1]

				for uuid, change of entity.changes
					transaction.changes.push _.extend(change, {uuid})

				@emit [{uuid}, transaction]
				callback()
			, callback)

		bolt.declareOutputFields ['key', 'transaction']
		return bolt
