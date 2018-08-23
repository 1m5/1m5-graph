package io.onemfive.neo4j;

import io.onemfive.core.sensors.SensorManager;
import io.onemfive.data.Peer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SensorManagerNeo4j implements SensorManager {

    private static final String DBLOCATION = "/smn";

    private Neo4jDB db;

    @Override
    public void updatePeer(Peer peer) {
        try (Transaction tx = db.getGraphDb().beginTx()) {
            Node n = db.getGraphDb().findNode(Label.label(Peer.class.getName()),"address",peer.getAddress());
            if(n != null) {
                GraphUtil.updateProperties(n, peer.toMap());
                peer.fromMap(n.getAllProperties());
            } else {
                n = db.getGraphDb().createNode(Label.label(Peer.class.getName()));
                GraphUtil.updateProperties(n, peer.toMap());
            }
        }
    }

    @Override
    public Map<String,Peer> getAllPeers() {
        Map<String,Peer> peers = new HashMap<>();
        try (Transaction tx = db.getGraphDb().beginTx();
             ResourceIterator<Node> i = db.getGraphDb().findNodes(Label.label(Peer.class.getName()))) {
            Peer p;
            Node n;
            while(i.hasNext()) {
                n = i.next();
                p = new Peer();
                p.fromMap(n.getAllProperties());
                if(p.getAddress()!=null)
                    peers.put(p.getAddress(),p);
            }
            tx.success();
        }
        return peers;
    }

    @Override
    public boolean init(Properties properties) {
        db = new Neo4jDB();
        String baseDir = properties.getProperty("1m5.dir.base");
        properties.setProperty("1m5.neo4j.db.location",baseDir+DBLOCATION);
        db.init(properties);
        return true;
    }

    @Override
    public boolean shutdown() {
        return false;
    }
}
