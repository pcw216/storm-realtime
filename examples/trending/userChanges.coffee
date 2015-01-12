storm = require('node-storm')
async = require('async')
_ = require('lodash')

module.exports = 
	bolt: ()->
		bolt = storm.oncebolt (data, callback)->
			[message] = data.tuple

			if not message.transaction.user?
				@log('no user, must be a system change')
				return callback()
			@emit [message]
			callback()
			
		bolt.declareOutputFields ['message']
		return bolt
