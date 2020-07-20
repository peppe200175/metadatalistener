Hive Metastore Listener in Java that sends message in ServiceBus Queue everytime a new spark or hive query (prestodb and others tools using the hive metastore) generates a modification to the the metadata of the metastore. 

**Instruction to install**

+ Copy the JAR file to the nodes where  Hive metastore server is running

+ Open Ambari and set the following properties:  
    in hive-env.sh :    
    At the end of the file, add the line and save:
    <code>export HIVE_AUX_JARS_PATH = \[path to JAR] (example : "/hive-metastore-listener-1.0-SNAPSHOT-jar-with-dependencies.jar") </code>
    in hive-site.xml     
    Add the tag before the closing configuration tag <code>\</configuration></code> and save:
    <code>\<property>\<name>hive.metastore.event.listeners\</name>\<value>com.gimartin.hivecatalogsync.MetadataListener\</value>\</property> </code>
    In ambari change all occurrences of  <code>hive.metastore.event.listeners</code> to value : <code>com.gimartin.hivecatalogsync.MetadataListener</code>.
    
+ Restart your Hive Metastore and all Hive services

