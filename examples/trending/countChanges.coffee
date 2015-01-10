storm = require('node-storm')
async = require('async')
q = require('q')
_ = require('lodash')
mongo = require('./mongo')


getCollection = ()->
	return mongo().collection('activity')

module.exports = 
	increment: ()->
		bolt = storm.oncebolt (data, callback)->
			[timeWindow, key] = data.tuple

			getCollection().then((collection)=>
				collection.update(
					{'window': timeWindow, 'key': key}
					{$inc: {count: 1}, $upsert: true}
					callback
				)
			).fail callback
		return bolt

	streamAll: ()->
		bolt = storm.oncebolt (data, callback)->
			[timeWindow, version] = data.tuple

			getCollection().then((collection)=>
				stream = collection.find(
					{'window': timeWindow}					
				)

				stream.on 'error', callback
				stream.on 'end', callback
				stream.on 'data', (doc)=>
					@emit [doc['window'], version, doc.count]
			).fail callback
		
		bolt.declareOutputFields ['window', 'version', 'sample']
		return bolt
