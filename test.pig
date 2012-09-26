/* Load Avro jars and define shortcut */
register /me/Software/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/Software/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/Software/pig/contrib/piggybank/java/piggybank.jar
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

register /me/Software/pig-to-json/dist/lib/pig-to-json.jar

emails = load '/me/Data/enron.avro' using AvroStorage();
emails = limit emails 10;
json_test = foreach emails generate message_id, com.hortonworks.pig.udf.ToJson(tos) as to_json;
dump json_test
