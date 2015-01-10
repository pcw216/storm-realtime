storm = require('node-storm')
async = require('async')
_ = require('lodash')

getCollection = ()->
	index = false
	return mongo().collection('changesByEntity')


module.exports = 
	bolt: ()->
		bolt = storm.oncebolt (data, callback)->
			[message] = data.tuple

			server = message.transaction.bag
			timestamp = message.transaction.timestamp
			
			async.eachLimit(_.keys(message.entities), 1, (uuid, callback)=>
				entity = message.entities[uuid]
				change = _.extend({server, timestamp}, entity)
				@emit [{uuid}, change]
			
			, callback)

		bolt.declareOutputFields ['key', 'change']
		return bolt
