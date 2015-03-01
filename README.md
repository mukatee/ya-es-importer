Yet Another Elasticsearch Importer
==================================

Imports data from an SQL database into Elasticsearch.

For those like me, who find the ES River/Feeder difficult to use and need something "simpler".
Of course "simple" is a subjective term.

Anyway. To use this, download the [zip file](https://github.com/mukatee/yaes-importer/releases).
Extract it to the directory of your choice. Fill in the configuration in the es_importer.properties file.
Check the [es_importer.properties](https://github.com/mukatee/yaes-importer/blob/master/es_importer.properties) file for instructions.

Start the program with 'java -cp "./yaes-vx.y.z.jar:your-db.driver.jar" net.kanstren.yaes.Main'.

For example:

java -cp "./yaes-v0.1.0.jar:mariadb-java-client-1.1.8.jar" net.kanstren.yaes.Main

License
-------

MIT License


