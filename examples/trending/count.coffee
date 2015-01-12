storm = require('node-storm')
async = require('async')
q = require('q')
_ = require('lodash')
mongo = require('./mongo')


getCollection = ()->
	return mongo.db().collection('transactions')

module.exports = 
	increment: ()->
		bolt = storm.oncebolt (data, callback)->
			[timeWindow, key, transaction] = data.tuple

			getCollection().then((collection)=>
				collection.update(
					{'window': timeWindow, 'key': key, type: transaction.type}
					{
						$inc: {count: 1}
						$push:
							changes: 
								$each: transaction.changes
					}
					{upsert: true}
					callback
				)
			).fail callback
		return bolt

	# streamAll: ()->
	# 	bolt = storm.oncebolt (data, callback)->
	# 		[timeWindow, version] = data.tuple

	# 		getCollection().then((collection)=>
	# 			stream = collection.find(
	# 				{'window': timeWindow}					
	# 			)

	# 			stream.on 'error', callback
	# 			stream.on 'end', callback
	# 			stream.on 'data', (doc)=>
	# 				@emit [doc['window'], version, doc.count]
	# 		).fail callback
		
	# 	bolt.declareOutputFields ['window', 'version', 'sample']
	# 	return bolt
