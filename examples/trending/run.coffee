q = require('q')
storm = require('node-storm')
objectChange = require('../../lib/spouts/objectChange')
userChanges = require('./userChanges')
changesByEntity = require('./changesByEntity')
byHour = require('./byHour')
countChanges = require('./countChanges')

builder = storm.topologybuilder()
builder.setSpout('objectChange', objectChange.spout({connectionString: 'bld-zookeeper-01:2181'}))
builder.setBolt('userChanges', userChanges.bolt()).shuffleGrouping('objectChange')
builder.setBolt('changesByEntity', changesByEntity.bolt()).shuffleGrouping('userChanges')
builder.setBolt('byHour', byHour.bolt()).shuffleGrouping('changesByEntity')
builder.setBolt('countChanges', countChanges.increment()).shuffleGrouping('byHour')

builder.setBolt('resetVariance', variance.reset()).shuffleGrouping('byHour')
builder.setBolt('streamCounts', countChanges.streamAll()).shuffleGrouping('resetVariance')
builder.setBolt('variance', variance.updateFromStream()).shuffleGrouping('streamCounts')

topology = builder.createTopology()

cluster = storm.localcluster()
options = 
	config: {'topology.debug':  true}

cluster.submit(topology, options)
	.then ()->		
		return q.delay(200000000)
	.finally ()->		
		cluster.shutdown()
	.fail console.error
