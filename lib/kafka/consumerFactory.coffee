{Client, Consumer} = require('kafka-node')

EventEmitter = require('events').EventEmitter
winston = require('winston')
_ = require('lodash')

module.exports = (options)->

	client = new Client(options.connectionString, options.clientId, options.zkOptions)
	consumer = new Consumer(client, options.payloads, options.consumerOptions)

	return {consumer, client}
