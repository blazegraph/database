package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * AST node base class.
 */
public abstract class QueryNodeBase extends ASTBase implements
        IQueryNode {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public QueryNodeBase() {

        super(BOp.NOARGS, null/* anns */);

    }

    public QueryNodeBase(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    public QueryNodeBase(QueryNodeBase op) {

        super(op);
        
    }

    @Override
    public final String toString() {

        return toString(0/* indent */);
        
    }

}
