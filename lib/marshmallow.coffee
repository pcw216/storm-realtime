path = require('path')
_ = require('lodash')
{EventEmitter} = require('events')
async = require('async')

java = require("java")
uberJar = path.join(__dirname, '../target/storm-realtime-0.0.0-SNAPSHOT-standalone.jar')
java.classpath.push(uberJar)

class Marshmallow extends EventEmitter

	constructor: ()->
		@topics = {}
		@subscribed = {}

	discoverTopic: (topicName, callback)=>
		if !@initialized then return callback('Marshmallow has not been initialized')
		if @topics[topicName] then return callback null, @topics[topicName]

		@repository.discoverTopic topicName, (err, topic)=>
			if err? then return callback(err)
			@topics[topicName] = topic
			callback(err, topic)

	subscribe: (topicName, options, callback)=>
		if not callback? and _.isFunction(options)
			callback = options
			options = null

		callback ?= _.noop
		options ?= {}
		options.consumerGroup ?= 'kafka-consumer-group'
		options.threads ?= 1


		if @subscribed[topicName] then return callback()
		@subscribed[topicName] = true
		@discoverTopic topicName, (err, topic)=>
			if err? then return callback(err)
			topic.subscribe @_createSubscriber(topicName), options.consumerGroup, options.threads, callback

	_createSubscriber: (topicName)=>
		return subscriber = java.newProxy 'marshmallow.Subscriber',
			handle: (msg)=>
				try
					json = JSON.parse(msg.toStringSync())
					schema = JSON.parse(msg.getSchemaSync().toStringSync())
					@emit 'message', topicName, json, schema
				catch err
					@emit 'error', err
					return false
				return true

	connect: (options)=>
		if @initialized then return
		@initialized = true
		
		if options.connectionString
			process.env.ZOOKEEPER_CONNECT = options.connectionString

		async.waterfall [
			(callback)-> java.callStaticMethod "marshmallow.Infrastructure", "init", callback
			(init, callback)-> java.callStaticMethod "marshmallow.Infrastructure", "getZookeeperConnect", callback
			(zk, callback)-> java.callStaticMethod "marshmallow.Repository", "zookeeper", zk, callback
			(repository, callback)-> java.callStaticMethod "marshmallow.Marshmallow", "init", (err)-> callback(err, repository)
		]
		, (err, @repository)=>
			@emit 'connected'


module.exports = new Marshmallow()
