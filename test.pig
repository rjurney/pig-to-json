/* Load Avro jars and define shortcut */
register /me/Software/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/Software/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/Software/pig/contrib/piggybank/java/piggybank.jar
define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

register /me/Software/pig-to-json/dist/lib/pig-to-json.jar

-- Enron emails are available at https://s3.amazonaws.com/rjurney_public_web/hadoop/enron.avro
emails = load '/me/Data/enron.avro' using AvroStorage();
emails = limit emails 10;
json_test = foreach emails generate message_id, com.hortonworks.pig.udf.ToJson(tos) as bag_json;
dump json_test

/* (<589.1075842593084.JavaMail.evans@thyme>,[{"address":"gerald.nemec@enron.com","name":null}])
(<614.1075847580822.JavaMail.evans@thyme>,[{"address":"jfichera@saberpartners.com","name":null},{"address":"barry.goode@gov.ca.gov","name":null},{"address":"hoffman@blackstone.com","name":null},{"address":"joseph.fichera@gov.ca.gov","name":null}])
(<735.1075840186524.JavaMail.evans@thyme>,[{"address":"kam.keiser@enron.com","name":"Kam Keiser"},{"address":"mike.grigsby@enron.com","name":"Mike Grigsby"}])
(<758.1075842602845.JavaMail.evans@thyme>,[{"address":"joan.quick@enron.com","name":null}])
(<765.1075860359973.JavaMail.evans@thyme>,[{"address":"jay_dudley@pgn.com","name":null},{"address":"michele_farrell@pgn.com","name":null}]) */

emails2 = load '/me/Data/enron.avro' using AvroStorage();
emails2 = limit emails2 10;
json_test2 = foreach emails2 generate message_id, com.hortonworks.pig.udf.ToJson(from) as tuple_json;
dump json_test2

/* (<28.1075842613917.JavaMail.evans@thyme>,{"address":"emmye@dracospring.com","name":"\"Emmye\""})
(<85.1075854368299.JavaMail.evans@thyme>,{"address":"darron.giron@enron.com","name":null})
(<167.1075851646300.JavaMail.evans@thyme>,{"address":"jeff.dasovich@enron.com","name":"Jeff Dasovich"})
(<185.1075857304356.JavaMail.evans@thyme>,{"address":"chris.dorland@enron.com","name":"Chris Dorland"})
(<735.1075840186524.JavaMail.evans@thyme>,{"address":"m..love@enron.com","name":"Phillip M. Love"})
(<758.1075842602845.JavaMail.evans@thyme>,{"address":"gerald.nemec@enron.com","name":null})
(<765.1075860359973.JavaMail.evans@thyme>,{"address":"mary.hain@enron.com","name":null}) */

-- This works for arbitrarily complex data structures as well
a = foreach (group emails by from.address) generate group as from_address, COUNT_STAR(emails) as sent_count, FLATTEN(emails.tos) as tos;  
b = group a by from_address;
c = foreach b generate group as from_address, com.hortonworks.pig.udf.ToJson(a) as json_test;
store c into '/tmp/big_test_num';

/* bc@ori.org	[{"tos":[{"address":"klay@enron.com","name":null}],"sent_count":1,"from_address":"bc@ori.org"}]
ben@crs.hn	[{"tos":[{"address":"klay@enron.com","name":null}],"sent_count":1,"from_address":"ben@crs.hn"}]
cba@uh.edu	[{"tos":[{"address":"rod.hayslett@enron.com","name":null}],"sent_count":1,"from_address":"cba@uh.edu"}]
cei@uh.edu	[{"tos":[{"address":"ceiinfo@uh.edu","name":"Shena Cherian"}],"sent_count":1,"from_address":"cei@uh.edu"}]
nc@mmf.com	[{"tos":[{"address":"kenneth.lay@enron.com","name":null}],"sent_count":1,"from_address":"nc@mmf.com"}]
nyb@uni.de	[{"tos":[{"address":"extramoney@mailman.enron.com","name":null}],"sent_count":1,"from_address":"nyb@uni.de"}] */