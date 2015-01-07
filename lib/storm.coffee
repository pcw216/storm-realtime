winston = require('winston')
winston.setLevels(winston.config.syslog.levels)
q = require('q')
storm = require('node-storm')
objectChange = require('./spouts/objectChange')

objectChangeSpout = objectChange.spout({connectionString: 'bld-zookeeper-01:2181'})
logChange = storm.basicbolt (data)->
	@emit ['foo-log-change']
	# [message] = data.tuple
	# if message?.transaction?.b ag?
	# 	@emit ["foo-#{message.transaction.bag}"]

logChange.declareOutputFields ['bag']

fin = storm.basicbolt ()->
	@emit['fin']
fin.declareOutputFields ['fin']

builder = storm.topologybuilder()
builder.setSpout('objectChange', objectChangeSpout, 1)
builder.setBolt('logChange', logChange, 1).shuffleGrouping('objectChange')
builder.setBolt('fin', fin, 1).shuffleGrouping('logChange')

topology = builder.createTopology()

cluster = storm.localcluster()
options = 
	config: {'topology.debug':  true}

cluster.submit(topology, options)
	.then ()->
		# console.log 'submitted storm'
		return q.delay(20000)
	.finally ()->
		# console.log ''
		cluster.shutdown()
	.fail console.error
