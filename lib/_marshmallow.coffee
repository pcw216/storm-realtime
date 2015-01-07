zookeeper = require('node-zookeeper-client')
{EventEmitter} = require('events')
zlib = require('zlib')
_ = require('lodash')
winston = require('winston')
avronode = require('avronode')

class Marshmallow extends EventEmitter
	
	###
	# Create a new marshmallow client (via zookeeper connection string and options)
	# @see https://github.com/alexguan/node-zookeeper-client#client-createclientconnectionstring-options
	###
	@createClient: (connectionString, options)->
		zk = zookeeper.createClient(connectionString, options)
		return new Marshmallow(zk)

	@METADATA_SCHEMA = {
		type: 'record'
		name: 'Model'
		fields: [
			{
				name: 'metadata'
				type: 
					type: 'record'
					name: 'Metadata' 
					namespace: 'marshmallow'
					fields: [
						{
							name: 'schema_id'
							type: 'string'
						}
					]
			}
		]
	}

	constructor: (@zooKeeper)->

	connect: ()=>
		@zooKeeper.once 'connected', ()=>
			winston.debug 'Marshmallow client connected to ZooKeeper'
			@emit.call(@, 'connected')
		@zooKeeper.connect()

	decode: (message, schema, callback)->
		if not callback? and _.isFunction(schema)
			 callback = schema
			 schema = null
		schema ?= Marshmallow.METADATA_SCHEMA
		
		avro = new avronode.Avro()

		avro.addSchema(JSON.stringify(schema))
		result = avro.decodeDatum(message, schema.name)


		# decoder = new avro.IO.BinaryDecoder(new Buffer(message))
		# reader = new IO.DatumReader(schema)

		# json = reader.read(decoder)

		callback(null, result)

	getCompressed: (zkPath, callback)=>
		@zooKeeper.getData zkPath, (err, data, stat)->
			if err?
				winston.error 'Marshmallow zookeeper getData error', {err}
				return callback(err)
			zlib.gunzip data, (err, unzippedData)->
				if err?
					winston.error 'Marshmallow zookeeper getData error', {err}
					return callback(err)
				try
					uncompressed = unzippedData
				catch err
					winston.error 'Marshmallow zookeeper getData error', {err}
					return callback(err)
				winston.debug('Marshmallow retrieved data', {uncompressed})
				callback(err, uncompressed, stat)

	getSchema: (schemaId, callback)=>
		@getCompressed "/marshmallow/schemas/#{schemaId}", (err, data, stat)->
			if err?
				winston.error 'Marshmallow zookeeper getSchema error', {err}
				return callback(err)
			try
				schema = JSON.parse(data)
			catch err
				winston.error 'Marshmallow zookeeper getSchema error', {err}
				return callback(err)
			winston.debug('Marshmallow retrieved schema', {schema})
			callback(err, schema)

module.exports = Marshmallow
