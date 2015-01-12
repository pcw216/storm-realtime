storm = require('node-storm')
es = require('event-stream')
_ = require('lodash')

transactionsByEntity = require('../../../examples/trending/transactionsByEntity')
sinon = require('sinon')

describe 'examples/trending/transactionsByEntity', (done)->

	beforeEach ()->
		@sandbox = sinon.sandbox.create()
		@sandbox.stub storm, 'oncebolt', (process)->
			return storm.asyncbolt process
	afterEach ()->
		@sandbox.restore()

	it 'should a message for each entity in a transaction', (done)->
		input =
			id: 1
			tuple: [{
				transaction:
					bag: 'bag'
					timestamp: 'timestamp'
				entities: 
					'foo': {}
					'bar': {}			
			}]
		es.readArray [input]
			.pipe transactionsByEntity.bolt()
			.pipe es.writeArray (err, messages)->
				messages = _.filter(messages, {command: 'emit'})
				messages.length.should.eql 2
				[foo, bar] = messages
				foo.tuple.should.eql [{uuid: 'foo'}, {server: 'bag', timestamp: 'timestamp'}]
				bar.tuple.should.eql [{uuid: 'bar'}, {server: 'bag', timestamp: 'timestamp'}]
				done()
	
