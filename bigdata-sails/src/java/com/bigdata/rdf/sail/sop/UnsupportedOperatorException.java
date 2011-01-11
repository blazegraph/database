package com.bigdata.rdf.sail.sop;

import org.openrdf.query.algebra.QueryModelNode;

public class UnsupportedOperatorException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1493443291958364334L;
	
    private QueryModelNode operator;

    public UnsupportedOperatorException(final QueryModelNode operator) {
        this.operator = operator;
    }

    public QueryModelNode getOperator() {
        return operator;
    }
    
}

