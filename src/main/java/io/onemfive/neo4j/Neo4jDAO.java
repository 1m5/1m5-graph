package io.onemfive.neo4j;

import io.onemfive.core.infovault.DAO;

public interface Neo4jDAO extends DAO {
    void setNeo4j(Neo4jDB db);
}
