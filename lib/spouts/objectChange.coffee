storm = require('node-storm')
_ = require('lodash')

ALM_BAG = /^alm-(.)+$/
TOPIC = 'bagboy-object-changes-1'

module.exports = 

	spout: (options)->
		spout = storm.spout (sync)->
			
			seqId = 1

			marshmallow = require('../marshmallow')
			marshmallow.connect({connectionString: options.connectionString})
			marshmallow.once 'connected', (err)=>
				if err? then throw err
				marshmallow.subscribe TOPIC
			marshmallow.on 'message', (topic, message, schema)=>
				if topic is TOPIC and ALM_BAG.test(message.transaction.bag)
					@emit [message], {id: seqId++}#"#{message.transaction.id}"}
					sync()
				# if topic is options.topic 
				# @emit [change]
			marshmallow.on 'error', (err)->			
				throw err
		spout.declareOutputFields(['change'])
		return spout
