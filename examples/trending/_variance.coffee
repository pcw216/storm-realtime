# http://stackoverflow.com/a/5544108
# http://blogs.msdn.com/b/lukeh/archive/2008/10/10/standard-deviation-and-event-based-programming.aspx
storm = require('node-storm')
async = require('async')
q = require('q')
_ = require('lodash')
mongo = require('./mongo')


getCollection = (collectionName='variance')->
	return mongo().collection(collectionName)

module.exports = 
	# recalc variance for a time period
	recalc: ()->
		bolt = storm.oncebolt (data, callback)->
			[timeWindow, key] = data.tuple

			# TODO Update some kind of pointer to the latest version
			# TODO expire old
			# TODO only reset/recalc if one not in progress
			# ??
			getCollection().then((collection)=>
				collection.insert {'window': timeWindow, ddof: 1, n:0, mean:0.0, M2: 0.0}, (err, doc)=>
					if err? then return callback(err)
					@emit [timeWindow, doc._id]
					callback()
			).fail callback
		return bolt

	# Stream in samples and calculate an online/running variance
	updateFromStream: ()->
		bolt = storm.oncebolt (data, callback)->
			[timeWindow, version, sample] = data.tuple
			
			# TODO There's a problem here with ordering. we can really only do one at a time.
			getCollection().then((collection)=>
				
				collection.findOne {_id: version}, (err, doc)=>
					# Special case tells us we're done with the records for set
					if sample is null 
						doc.ready = true
						doc.std = Math.sqrt(doc.variance)
					else
						doc.n += 1
						delta = sample - doc.mean
						doc.mean += delta / doc.n
						doc.M2 += delta * (sample - doc.mean)
						doc.variance = doc.M2 / (doc.n - doc.ddof)
					
					collection.update doc, callback

			).fail callback
		return bolt
