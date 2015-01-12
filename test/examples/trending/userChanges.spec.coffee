storm = require('node-storm')
es = require('event-stream')
_ = require('lodash')

userChanges = require('../../../examples/trending/userChanges')
sinon = require('sinon')

describe 'examples/trending/userChanges', (done)->

	beforeEach ()->
		@sandbox = sinon.sandbox.create()
		@sandbox.stub storm, 'oncebolt', (process)->
			return storm.asyncbolt process
	afterEach ()->
		@sandbox.restore()

	it 'should not emit messages for transactions without a user', (done)->
		input =
			id: 1
			tuple: [{
				transaction: {}					
			}]
		es.readArray [input]
			.pipe userChanges.bolt()
			.pipe es.writeArray (err, messages)->
				messages = _.filter(messages, {command: 'emit'})
				messages.should.eql []
				done()
	
	it 'should passthrough and emit messages for transactions with a user', (done)->
		input =
			tuple: [{
				transaction:
					user: 'foobar'
			}]
		es.readArray [input]
			.pipe userChanges.bolt()
			.pipe es.writeArray (err, messages)->
				[output] = _.filter(messages, {command: 'emit'})
				output.tuple.should.eql input.tuple
				done()
