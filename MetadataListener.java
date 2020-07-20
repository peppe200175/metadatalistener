package com.gimartin.hivecatalogsync;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.CharEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;

import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterCatalogEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.bitbucket.cowwoc.diffmatchpatch.DiffMatchPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;


public class MetadataListener extends MetaStoreEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataListener.class);
    //   private static final  ObjectMapper objMapper = new ObjectMapper();
    // public static final String storageConnectionString ="DefaultEndpointsProtocol=https;AccountName=prestodbtesthdistorage;AccountKey=todUacHM58anTvhhY7tT4gm3uo1uer/xM8GFvAlx9Sk8lMJJ8KBRRQrsaqruXtQ2PQpECbIfv2PiiXnDTI022g==;EndpointSuffix=core.windows.net";
    //private static MessageFactory messageFactory = MessageFactory.getInstance();
    Gson gson = new GsonBuilder().create();

   // Properties prop = new Properties();
    //private static ResourceBundle rb = ResourceBundle.getBundle("resources/config","");

    //Connection conn=DriverManager.getConnection(reader.getString("db.url"),reader.getString("db.username"),reader.getString("db.password"));


    // HiveConf conf;
    //HiveMetaStoreClient hmc;
    public MetadataListener(Configuration config) throws MetaException {
        super(config);
        logWithHeader("[Thread: " + Thread.currentThread().getName() + "] | [method: " + Thread.currentThread().getStackTrace()[1].getMethodName() + " ] | METADATAHOOK created ");
        //Properties prop = new Properties();
       /* String fileName = "resources/config.propreties";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {

        }
        try {
            prop.load(is);
        } catch (IOException ex) {

        }*/

    }

    @Override

    public void onAlterDatabase(AlterDatabaseEvent event) throws MetaException {
        if (event.getStatus()) {
            try {
                Database db = (event.getOldDatabase());
                DiffMatchPatch dmp = new DiffMatchPatch();
                JsonMessage msg = new JsonMessage();
                msg.action = "ALTER DATABASE " + db.getName();
                msg.sender = hostInfo();
                msg.oldItem = (event.getOldDatabase()).toString();
                msg.newItem = (event.getNewDatabase().toString());
                LinkedList<DiffMatchPatch.Diff> info = dmp.diffMain(msg.oldItem, msg.newItem, false);
                dmp.diffCleanupSemantic(info);
                msg.diffTable = (info.toString());
                try {
                    msg.catalog = event.getNewDatabase().getCatalogName();
                } catch (NoSuchMethodError ex) {
                    msg.catalog = null;
                };
                msg.location=db.getLocationUri();
                logWithHeader(msg.action);
                sendMessage2SB((gson.toJson(msg)));
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
        super.onAlterDatabase(event);
    }

    @Override
    public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {

        super.onConfigChange(tableEvent);
    }

    @Override
    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
        super.onDropPartition(partitionEvent);
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        if (dbEvent.getStatus()) {
            try {
                Database db = (dbEvent.getDatabase());
                JsonMessage msg = new JsonMessage();
                msg.action = "CREATE DATABASE " + db.getLocationUri();
                msg.sender = hostInfo();
                msg.createItem = (db.toString());
                try {
                    msg.catalog = db.getCatalogName();
                } catch (NoSuchMethodError ex) {
                    msg.catalog = null;
                }
                msg.location= db.getLocationUri();
                sendMessage2SB((gson.toJson(msg)));
                logWithHeader(msg.action);
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
        super.onCreateDatabase(dbEvent);
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        if (dbEvent.getStatus()) {
            try {
                Database db = (dbEvent.getDatabase());
                JsonMessage msg = new JsonMessage();
                msg.action = "DROP DATABASE " + db.getName();
                msg.sender = hostInfo();
                try {
                    msg.catalog = db.getCatalogName();
                } catch (NoSuchMethodError ex) {
                    msg.catalog = null;
                }
                msg.location= db.getLocationUri();
                sendMessage2SB((gson.toJson(msg)));
                logWithHeader(msg.action);
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
        super.onDropDatabase(dbEvent);
    }

    @Override
    public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
        super.onLoadPartitionDone(partSetDoneEvent);
    }

    @Override
    public void onCreateFunction(CreateFunctionEvent fnEvent) throws MetaException {
        super.onCreateFunction(fnEvent);
    }

    @Override
    public void onDropFunction(DropFunctionEvent fnEvent) throws MetaException {
        super.onDropFunction(fnEvent);
    }

    @Override
    public void onInsert(InsertEvent insertEvent) throws MetaException {
        super.onInsert(insertEvent);
    }

    @Override
    public void onAddPrimaryKey(AddPrimaryKeyEvent addPrimaryKeyEvent) throws MetaException {
        super.onAddPrimaryKey(addPrimaryKeyEvent);
    }

    @Override
    public void onAddForeignKey(AddForeignKeyEvent addForeignKeyEvent) throws MetaException {
        super.onAddForeignKey(addForeignKeyEvent);
    }

    @Override
    public void onAddUniqueConstraint(AddUniqueConstraintEvent addUniqueConstraintEvent) throws MetaException {
        super.onAddUniqueConstraint(addUniqueConstraintEvent);
    }

    @Override
    public void onAddNotNullConstraint(AddNotNullConstraintEvent addNotNullConstraintEvent) throws MetaException {
        super.onAddNotNullConstraint(addNotNullConstraintEvent);
    }

    @Override
    public void onDropConstraint(DropConstraintEvent dropConstraintEvent) throws MetaException {
        super.onDropConstraint(dropConstraintEvent);
    }


    @Override
    public void onCreateCatalog(CreateCatalogEvent createCatalogEvent) throws MetaException {

        try {
            Catalog cat = (createCatalogEvent.getCatalog());
            JsonMessage msg = new JsonMessage();
            msg.action = "CREATE CATALOG " + cat.getName();
            msg.sender = hostInfo();
            msg.createItem = (cat.toString());
            msg.location = cat.getLocationUri();
            sendMessage2SB((gson.toJson(msg)));
            logWithHeader(msg.action);
        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logWithHeader(errors.toString());
        }


        super.onCreateCatalog(createCatalogEvent);
    }

    @Override
    public void onAlterCatalog(AlterCatalogEvent alterCatalogEvent) throws MetaException {
        if (alterCatalogEvent.getStatus()) {
            try {
                Catalog cat = (alterCatalogEvent.getOldCatalog());
                DiffMatchPatch dmp = new DiffMatchPatch();
                JsonMessage msg = new JsonMessage();
                msg.action = "ALTER CATALOG  " + cat.getName();
                msg.sender = hostInfo();
                msg.oldItem = (alterCatalogEvent.getOldCatalog()).toString();
                msg.newItem = (alterCatalogEvent.getNewCatalog().toString());
                LinkedList<DiffMatchPatch.Diff> info = dmp.diffMain(msg.oldItem, msg.newItem, false);
                dmp.diffCleanupSemantic(info);
                msg.diffTable = (info.toString());
                msg.location = alterCatalogEvent.getNewCatalog().getLocationUri();
                logWithHeader(msg.action);
                sendMessage2SB((gson.toJson(msg)));
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
        super.onAlterCatalog(alterCatalogEvent);
    }

    @Override
    public void onDropCatalog(DropCatalogEvent dropCatalogEvent) throws MetaException {
        try {
            Catalog cat = (dropCatalogEvent.getCatalog());
            JsonMessage msg = new JsonMessage();
            msg.action = "DROP CATALOG " + cat.getName();
            msg.sender = hostInfo();
            msg.info = (cat.toString());
            msg.location = cat.getLocationUri();
            sendMessage2SB((gson.toJson(msg)));
            logWithHeader(msg.action);
        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logWithHeader(errors.toString());
        }
        super.onDropCatalog(dropCatalogEvent);
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
        if (partitionEvent.getStatus()) {
            try {
                Partition part = (partitionEvent.getOldPartition());
                DiffMatchPatch dmp = new DiffMatchPatch();
                JsonMessage msg = new JsonMessage();
                msg.action = "ALTER PARTITION  " + part.getParameters().values().toArray().toString();
                msg.sender = hostInfo();
                msg.oldItem = (partitionEvent.getOldPartition()).toString();
                msg.newItem = (partitionEvent.getNewPartition().toString());
                LinkedList<DiffMatchPatch.Diff> info = dmp.diffMain(msg.oldItem, msg.newItem, false);
                dmp.diffCleanupSemantic(info);
                msg.diffTable = (info.toString());
                msg.location = partitionEvent.getTable().getTableName();
                logWithHeader(msg.action);
                sendMessage2SB((gson.toJson(msg)));
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
        super.onAlterPartition(partitionEvent);
    }

    @Override

    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        super.onDropTable(tableEvent);
        if (tableEvent.getStatus()) {
            try {
                Table tbl = (tableEvent.getTable());
                JsonMessage msg = new JsonMessage();
                msg.action = "DROP TABLE " + tbl.getDbName() + "." + tbl.getTableName();
                msg.sender = hostInfo();
                msg.location = tableEvent.getTable().getSd().getLocation();
                sendMessage2SB((gson.toJson(msg)));
                logWithHeader(msg.action);
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
    }

    @Override

    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
        if (tableEvent.getStatus()) {
            try {
                Table tbl = (tableEvent.getTable());
                JsonMessage msg = new JsonMessage();
                msg.action = "CREATE TABLE " + tbl.getDbName() + "." + tbl.getTableName();
                msg.sender = hostInfo();
                msg.createItem = (tbl.toString());
                msg.location = tableEvent.getTable().getSd().getLocation();
                sendMessage2SB((gson.toJson(msg)));
                logWithHeader(msg.action);
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
        super.onCreateTable(tableEvent);
    }


    @Override
    public void onAlterTable(AlterTableEvent event) throws MetaException {
        if (event.getStatus()) {
            try {
                Table tbl = (event.getOldTable());
                DiffMatchPatch dmp = new DiffMatchPatch();
                JsonMessage msg = new JsonMessage();
                msg.action = "ALTER TABLE " + event.getOldTable().getTableName();
                msg.sender = hostInfo();
                msg.oldItem = (event.getOldTable()).toString();
                msg.newItem = (event.getNewTable().toString());
                LinkedList<DiffMatchPatch.Diff> info = dmp.diffMain(msg.oldItem, msg.newItem, false);
                dmp.diffCleanupSemantic(info);
                msg.diffTable = (info.toString());
                msg.location = event.getNewTable().getSd().getLocation();
                logWithHeader(msg.action);
                sendMessage2SB((gson.toJson(msg)));
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }

        }
        super.onAlterTable(event);
    }

    private void logWithHeader(String obj) {

        LOG.info("[METADATAHOOK][Thread: " + Thread.currentThread().getName() + "] | " + obj);
    }


    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent)
            throws MetaException {
        if (partitionEvent.getStatus()) {
            try {
                Table tbl = (partitionEvent.getTable());
                DiffMatchPatch dmp = new DiffMatchPatch();
                JsonMessage msg = new JsonMessage();
                msg.action = "ADD PARTION " + partitionEvent.getPartitionIterator().toString() + " on TABLE " + partitionEvent.getTable().getTableName();
                msg.sender = hostInfo();
                for (Iterator<Partition> it = partitionEvent.getPartitionIterator(); it.hasNext(); ) {
                    Partition partition = it.next();
                    msg.info = partition.toString();
                    msg.location += partition.getSd().getLocation() + ";";
                }
                logWithHeader(msg.action);
                sendMessage2SB((gson.toJson(msg)));
            } catch (Exception ex) {
                StringWriter errors = new StringWriter();
                ex.printStackTrace(new PrintWriter(errors));
                logWithHeader(errors.toString());
            }
        }
        super.onAddPartition(partitionEvent);
    }

    private void sendMessage2SB(String inputmessage) throws UnsupportedEncodingException {

        String connectionString = System.getenv("SB_CONNECTIONSTRING");
        ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName("queue1")
                .buildClient();


        byte[] data = inputmessage.getBytes(CharEncoding.UTF_8);


        //final String messageId = Integer.toString(i);
        //ServiceBusMessage message = null;

        ServiceBusMessage message = new ServiceBusMessage(data);

        message.setContentType("application/text");
        message.setLabel("queue1");
        message.setMessageId(String.valueOf(System.currentTimeMillis()));
        message.setTimeToLive(Duration.ofMinutes(30));
        logWithHeader("Message sending: Id = " + message.getMessageId());

        senderClient.send(message);
        logWithHeader("\tMessage acknowledged: Id = " + message.getMessageId());

        senderClient.close();


    }


    private String hostInfo() {
        InetAddress ip;
        String hostname;
        try {
            ip = InetAddress.getLocalHost();
            hostname = ip.getHostName();
            logWithHeader("Your current IP address : " + ip);
            logWithHeader(" Your current Hostname : " + hostname);
            return hostname + "(" + ip + ") ";

        } catch (UnknownHostException e) {

            e.printStackTrace();
            return "";
        }

    }

    private static class JsonMessage {
        public String getInfo() {
            return info;
        }

        public void setInfo(String info) {
            this.info = info;
        }

        public String info;
        public String sender;
        public String action;
        public String newItem;
        public String oldItem;
        public String createItem;
        public String diffTable;
        public String timeStamp;
        public String location;
        public String catalog;

        public String getCatalog() {
            return catalog;
        }

        public void setCatalog(String catalog) {
            this.catalog = catalog;
        }


        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            location = location;
        }


        public String getTimeStamp() {
            return timeStamp;
        }

        public String getNewItem() {
            return newItem;
        }

        public void setNewItem(String newItem) {
            this.newItem = newItem;
        }

        public String getOldItem() {
            return oldItem;
        }

        public void setOldItem(String oldItem) {
            this.oldItem = oldItem;
        }

        public String getCreateItem() {
            return createItem;
        }

        public void setCreateItem(String createItem) {
            this.createItem = createItem;
        }

        public String getDiffTable() {
            return diffTable;
        }

        public void setDiffTable(String diffTable) {
            this.diffTable = diffTable;
        }


        public JsonMessage() {
            timeStamp = String.valueOf(System.currentTimeMillis());
        }

        public String getSender() {
            return sender;
        }

        public void setSender(String sender) {
            sender = sender;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        @Override
        public String toString() {
            return "JsonMessage{" +
                    "info='" + info + '\'' +
                    ", sender='" + sender + '\'' +
                    ", action='" + action + '\'' +
                    ", newTable='" + newItem + '\'' +
                    ", oldTable='" + oldItem + '\'' +
                    ", createTable='" + createItem + '\'' +
                    ", diffTable='" + diffTable + '\'' +
                    ", timeStamp='" + timeStamp + '\'' +
                    '}';
        }

    }


}
