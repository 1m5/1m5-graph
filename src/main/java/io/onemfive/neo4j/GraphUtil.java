package io.onemfive.neo4j;

import org.neo4j.graphdb.*;

import java.util.Map;
import java.util.Set;

public class GraphUtil {

    public static void updateProperties(PropertyContainer container, Map<String,Object> attributes) {
        Set<String> keys = attributes.keySet();
        for(String key : keys) {
            container.setProperty(key,attributes.get(key));
        }
    }

}
