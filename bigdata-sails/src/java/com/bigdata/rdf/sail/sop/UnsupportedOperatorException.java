package com.bigdata.rdf.sail.sop;

import org.openrdf.query.algebra.QueryModelNode;

/**
 * An exception thrown when an operator can not be translated into native
 * bigdata evaluation. This is used to detect such problems and then optionally
 * delegate the operator to openrdf.
 * 
 * @author mrpersonick
 */
public class UnsupportedOperatorException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1493443291958364334L;
	
    final private QueryModelNode operator;

	/**
	 * Wrap with another instance of this class.
	 * 
	 * @param cause
	 */
    public UnsupportedOperatorException(final UnsupportedOperatorException cause) {
        super(cause);
        this.operator = cause.operator;
    }

    public UnsupportedOperatorException(final QueryModelNode operator) {
    	super(""+operator);
        this.operator = operator;
    }

    public QueryModelNode getOperator() {
        return operator;
    }
    
}

