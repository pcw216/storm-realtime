_ = require('lodash')
avronode = require('avronode')
avro = new avronode.Avro()

fs = require('fs')
marshmallow = require('./marshmallow')

schema = JSON.parse(fs.readFileSync('./schema_model.json').toString())
schema = JSON.stringify(schema).trim()
console.log 'schema\n', schema

# aschema = new avro.Schema.Schema(schema)

# aschema = marshmallow.METADATA_SCHEMA

data = fs.readFileSync('./message_trim.txt')
bytes = [];

_.each data, (byte)->
    bytes.push(byte);

avro.addSchema(schema)
console.log 'added schema', schema
result = avro.decodeDatum(data, 'Model')

# bytes = avro.encodeDatum(12345.89, '"double"');
# result = avro.decodeDatum(bytes, '"double"');

console.log '\n\nresult\n\n', result

avro.close();


# avroFile = avro.DataFile.AvroFile()
# reader = avroFile.open('./message.txt', aschema, {flags: 'r'})
# reader.on 'data', (data)->
# 	console.log 'data\n\n', data
