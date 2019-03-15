package io.onemfive.neo4j;

import io.onemfive.data.NetworkPeer;
import org.neo4j.graphdb.PropertyContainer;

import java.util.Map;
import java.util.Set;

public class GraphUtil {

    public static void updateProperties(PropertyContainer container, Map<String,Object> attributes) {
        Set<String> keys = attributes.keySet();
        for(String key : keys) {
            container.setProperty(key,attributes.get(key));
        }
    }

    public static NetworkPeer mapToPeer(Map<String,Object> m) {
        NetworkPeer p = new NetworkPeer();
        p.fromMap(m);
        return p;
    }

    public static Map<String,Object> peerToMap(NetworkPeer p) {
        return p.toMap();
    }

}
