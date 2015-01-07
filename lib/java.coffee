winston = require('winston')
winston.setLevels(winston.config.syslog.levels)

ALM_BAG = /^alm-(.)+$/

marshmallow = require('./marshmallow')

marshmallow.connect
	connectionString: 'bld-zookeeper-01:2181'
marshmallow.once 'connected', ()->
	marshmallow.subscribe 'bagboy-object-changes-1'

marshmallow.on 'message', (topicName, message, schema)->
	if ALM_BAG.test(message.transaction.bag)
		console.log "#{topicName} message:\n", JSON.stringify(message, null, 4)

process.stdin.resume()
