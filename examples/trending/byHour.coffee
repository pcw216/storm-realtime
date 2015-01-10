storm = require('node-storm')
async = require('async')
_ = require('lodash')
moment = require('moment')

module.exports = 
	bolt: ()->
		bolt = storm.oncebolt (data, callback)->
			[key, change] = data.tuple
			{timestamp} = change

			hour = moment(timestamp).utc().format('YYYY-MM-DD-HH')

			theWindow = 
				period: hour
				type: 'hour'

			@emit [theWindow, key, change]

		bolt.declareOutputFields ['window', 'key', 'change']
		return bolt
