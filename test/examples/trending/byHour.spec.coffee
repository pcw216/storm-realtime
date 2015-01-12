storm = require('node-storm')
es = require('event-stream')
moment = require('moment')
_ = require('lodash')

byHour = require('../../../examples/trending/byHour')
sinon = require('sinon')

describe 'examples/trending/byHour', (done)->

	beforeEach ()->
		@sandbox = sinon.sandbox.create()
		@sandbox.stub storm, 'oncebolt', (process)->
			return storm.asyncbolt process
	afterEach ()->
		@sandbox.restore()

	it 'emits timestamps for messages on the hour of their transaction', (done)->
		input =
			id: 1
			tuple: [
				'key'
				{timestamp: moment('2000-01-01').add('minute', 1).utc().toDate().getTime()}
			]
		es.readArray [input]
			.pipe byHour.bolt()
			.pipe es.writeArray (err, results)->
				[output] = _.filter(results, {command: 'emit'})
				[theWindow, key, change] = output.tuple
				theWindow.should.eql({type: 'hour', period: moment('2000-01-01').utc().format('YYYY-MM-DD-HH')})
				key.should.eql input.tuple[0]
				change.should.eql input.tuple[1]
				done()
