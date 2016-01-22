package com.bigdata.rdf.sparql.ast;

/**
 * Type safe enumeration reporting the high level type of the query. This
 * provides access to information which may otherwise be difficult to determine
 * by inspecting the generated AST or query plan.
 */
public enum QueryType {

    ASK, DESCRIBE, CONSTRUCT, SELECT;

    private QueryType() {
        
    }

}
