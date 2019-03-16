package io.onemfive.neo4j;

import org.neo4j.graphdb.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GraphUtil {

    public static void updateProperties(PropertyContainer c, Map<String,Object> a) {
        Set<String> keys = a.keySet();
        for(String key : keys) {
            c.setProperty(key,a.get(key));
        }
    }

    public static Map<String,Object> getAttributes(PropertyContainer c) {
        Map<String,Object> a = new HashMap<>();
        for (String key : c.getPropertyKeys()) {
            a.put(key, c.getProperty(key));
        }
        return a;
    }

}
