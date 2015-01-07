winston = require('winston')
winston.setLevels(winston.config.syslog.levels)
winston.debug 'tst debug'
_ = require('lodash')
fs = require('fs')

marshmallow = require('./marshmallow')

#zookeeper = require('node-zookeeper-client')
consumerFactory = require('./kafka/consumerFactory')

{Offset} = require('kafka-node')

console.log 'mc is', marshmallow
mc = marshmallow.createClient('bld-zookeeper-01:2181')
mc.once 'connected', ()->
	id = '83c848d59d6c6d3ce46f6279a1b9a'
	# id = '9ee18feb381c6d33224c0f5b95abdb3'
	mc.getCompressed "/marshmallow/topics/bagboy-object-changes-1", (err, topicData)->
		console.log 'topic', topicData.toString()
		mc.getCompressed "/marshmallow/schemas/bagboy.BagboyChange", (err, schemaMeta)->
			mc.getSchema id, (err, schema)->
				if err? then return winston.error 'error', {err}
				fs.writeFileSync("./schema_#{id}.json", JSON.stringify(schema, null, 2))
mc.connect()



{consumer, client} = consumerFactory({
	connectionString: 'bld-zookeeper-01:2181/kafka8'
	# clientId: 'storm-realtime'
	payloads: [
		{topic: 'bagboy-object-changes-1'}
	]
	zkOptions:
		sessionTimeout: 100
	consumerOptions:
		# groupId: 'storm-realtime-demo'
		autoCommit: false

})

offset = new Offset(client)
consumer.on 'message', (message)->
	# winston.info 'message', message.value.toString()

	fs.writeFileSync('./message.txt', message.value.toString())

	mc.decode message, (err, json)->
		if err? then return winston.error('error decoding message', {err})
		winston.debug 'message', {message: json}

consumer.on 'error', (err)->
	winston.error 'error', {err}
consumer.on 'offsetOutOfRange', (topic)->
	winston.error 'offset is out of rance', {topic}
	offset.fetch [topic], (err, offsets)->

		min = Math.min.apply(null, offsets[topic.topic][topic.partition])
		winston.debug 'min is now', min
		consumer.setOffset(topic.topic, topic.partition, min)

console.log '\n\nscript finished'
# process.stdin.resume()
