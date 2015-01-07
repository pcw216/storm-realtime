avro = require('node-avro-io')
fs = require('fs')
marshmallow = require('./marshmallow')

schema = JSON.parse(fs.readFileSync('./schema_9ee18feb381c6d33224c0f5b95abdb3.json'))
aschema = new avro.Schema.Schema(schema)

# aschema = marshmallow.METADATA_SCHEMA

data = fs.readFileSync('./message.txt')
block = new avro.DataFile.Block()
block.write(data)

reader = new avro.IO.DatumReader(aschema)
decoder = new avro.IO.BinaryDecoder(block)

result = reader.read(decoder)


console.log '\n\nresult\n\n', result

# avroFile = avro.DataFile.AvroFile()
# reader = avroFile.open('./message.txt', aschema, {flags: 'r'})
# reader.on 'data', (data)->
# 	console.log 'data\n\n', data
