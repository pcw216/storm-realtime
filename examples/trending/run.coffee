q = require('q')
storm = require('node-storm')
objectChange = require('../../lib/spouts/objectChange')
lastChange = require('./lastChange')

objectChangeSpout = objectChange.spout({connectionString: 'bld-zookeeper-01:2181'})
lastChangeBolt = lastChange.bolt()

builder = storm.topologybuilder()
builder.setSpout('objectChange', objectChangeSpout)
builder.setBolt('lastChange', lastChangeBolt).shuffleGrouping('objectChange')

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
