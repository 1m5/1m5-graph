package io.onemfive.neo4j;

import io.onemfive.core.infovault.DAO;
import io.onemfive.core.infovault.InfoVaultDB;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.util.Properties;
import java.util.logging.Logger;

public class Neo4jDB implements InfoVaultDB {

    private static final Logger LOG = Logger.getLogger(Neo4jDB.class.getName());

    private boolean initialized = false;
    private Properties properties;
    private GraphDatabaseService graphDb;

    protected Neo4jDB() {
        super();
    }

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public void execute(DAO dao) throws Exception {
        if(dao instanceof Neo4jDAO) {
            ((Neo4jDAO)dao).setNeo4j(this);
        }
        dao.execute();
    }

    public boolean init(Properties properties) {
        if(!initialized) {
            this.properties = properties;
            String dbLocation = properties.getProperty("1m5.neo4j.db.location");
            if(dbLocation == null) {
                LOG.warning("Unable to find property 1m5.neo4j.db.location. Please provide.");
                return false;
            }
            File dbDir = new File(dbLocation);
            if(!dbDir.exists() && !dbDir.mkdir()) {
                LOG.warning("Unable to create graph db directory at: "+dbLocation);
                return false;
            }

            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir);

            Runtime.getRuntime().addShutdownHook(new Thread()
            {
                @Override
                public void run()
                {
                    LOG.info("Stopping...");
                    graphDb.shutdown();
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
