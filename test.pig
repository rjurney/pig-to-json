/* Load Avro jars and define shortcut */
register /me/Software/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/Software/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/Software/pig/contrib/piggybank/java/piggybank.jar
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

register /me/Software/pig-to-json/dist/lib/pig-to-json.jar
register /me/Software/pig-0.10.0/build/ivy/lib/Pig/json-simple-1.1.jar

emails = load '/me/Data/enron.avro' using AvroStorage();
json_test = foreach emails generate message_id, com.hortonworks.pig.udf.ToJson(tos) as from_json;
a = limit json_test 10;
dump a
xb