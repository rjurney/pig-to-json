/* Load Avro jars and define shortcut */
register /me/Software/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/Software/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/Software/pig/contrib/piggybank/java/piggybank.jar
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

register /me/Software/pig-to-json/dist/lib/pig-to-json.jar

-- Available at https://s3.amazonaws.com/rjurney_public_web/hadoop/enron.avro
emails = load '/me/Data/enron.avro' using AvroStorage();
emails = limit emails 10;
json_test = foreach emails generate message_id, com.hortonworks.pig.udf.ToJson(tos) as bag_json;
dump json_test

emails2 = load '/me/Data/enron.avro' using AvroStorage();
emails2 = limit emails2 10;
json_test2 = foreach emails2 generate message_id, com.hortonworks.pig.udf.ToJson(from) as tuple_json;
dump json_test2
