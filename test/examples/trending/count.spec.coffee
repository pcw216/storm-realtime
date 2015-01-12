storm = require('node-storm')
es = require('event-stream')
moment = require('moment')
_ = require('lodash')

countChanges = require('../../../examples/trending/count')
sinon = require('sinon')

describe 'examples/trending/count', (done)->

	beforeEach ()->
		@sandbox = sinon.sandbox.create()
		@sandbox.stub storm, 'oncebolt', (process)->
			return storm.asyncbolt process

		@collection = {
			update: sinon.stub().yields()
		}
		@mongoHelper().resolve {
			collection: ()=>
				return @collection 
		}


	afterEach ()->
		@sandbox.restore()

	describe 'increment', ()->
		it 'should increment activity count for the given window and key', (done)->
			input =
				tuple: [
					'window'
					'key'
				]				
			es.readArray [input]
				.pipe countChanges.increment()
				.pipe es.writeArray (err, results)=>
					@collection.update.called.should.be.true	
					[query, update, options] = @collection.update.lastCall.args
					query.should.eql {window: 'window', key: 'key'}
					update.should.eql { '$inc': { count: 1 }}
					options.should.eql {'upsert': true }
					done()			
				.on 'error', done
