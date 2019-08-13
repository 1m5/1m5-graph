package io.onemfive.neo4j;

import io.onemfive.core.infovault.DAO;
import io.onemfive.core.infovault.InfoVaultDB;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class Neo4jDB implements InfoVaultDB {

    private static final Logger LOG = Logger.getLogger(Neo4jDB.class.getName());

    private boolean initialized = false;
    private Properties properties;
    private GraphDatabaseService graphDb;
    private String location;

    public Neo4jDB() {
        super();
    }

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public void execute(DAO dao) throws Exception {
        if(dao instanceof Neo4jDAO) {
            ((Neo4jDAO)dao).setNeo4j(this);
            dao.execute();
        } else
            throw new Exception("DAO not instance of Neo4jDAO");
    }

    @Override
    public void save(String label, String key, byte[] content, boolean autoCreate) throws FileNotFoundException {
        try (Transaction tx = graphDb.beginTx()) {
            Node n = graphDb.findNode(Label.label(label),"name",key);
            if(n == null) {
                if(autoCreate) {
                    n = graphDb.createNode(Label.label(File.class.getName()));
                    n.setProperty("name", key);
                } else
                    throw new FileNotFoundException("Key not found and autoCreate=false");
            }
            n.setProperty("content",content);
            tx.success();
        }
    }

    @Override
    public byte[] load(String label, String key) throws FileNotFoundException {
        byte[] content;
        try (Transaction tx = graphDb.beginTx()) {
            Node n = graphDb.findNode(Label.label(label),"name",key);
            if(n == null)
                throw new FileNotFoundException("Key "+key+" not found.");
            Object obj = n.getProperty("content");
            if(obj == null)
                throw new FileNotFoundException("Property 'content' not found for key="+key);
            content = (byte[])obj;
            tx.success();
        }
        return content;
    }

    @Override
    public List<byte[]> loadAll(String label) {
        List<byte[]> byteList = new ArrayList<>();
        ResourceIterator<Node> nodes = graphDb.findNodes(Label.label(label));
        Node n;
        Object obj;
        while(nodes.hasNext()) {
            n = nodes.next();
            obj = n.getProperty("content");
            byteList.add((byte[])obj);
        }
        return byteList;
    }

    public boolean init(Properties properties) {
        if(location==null) {
            LOG.warning("Neo4J DB location required. Please provide.");
            return false;
        }
        if(!initialized) {
            this.properties = properties;
            File dbDir = new File(location);
            if(!dbDir.exists() && !dbDir.mkdir()) {
                LOG.warning("Unable to create graph db directory at: "+location);
                return false;
            }

//            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir);
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbDir)
                    .setConfig(GraphDatabaseSettings.allow_upgrade,"true")
                    .newGraphDatabase();

            Runtime.getRuntime().addShutdownHook(new Thread()
            {
                @Override
                public void run()
                {
                    LOG.info("Stopping...");
                    graphDb.shutdown();
                    LOG.info("Stopped.");
                }
            } );

            return true;
        }
        return true;
    }

    public boolean teardown() {
        LOG.info("Tearing down...");
        graphDb.shutdown();
        LOG.info("Torn down.");
        return true;
    }
}
