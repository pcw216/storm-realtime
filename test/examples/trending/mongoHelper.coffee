mongo = require('../../../examples/trending/mongo')
sinon = require('sinon')
q = require('q')

_stub = null

beforeEach ()->	
	@mongoHelper = ()->
		db = q.defer()
		db.promise.collection = (collection)->
			return db.promise.then (client)-> 
				return client.collection(collection)
		_stub = sinon.stub(mongo, 'db').returns db.promise
		_stub.deferred = db
		return db

afterEach ()->
	_stub?.restore()
	_stub = null
