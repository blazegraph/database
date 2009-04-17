package com.bigdata.rdf.rio;

/**
 * An instance of this exception is thrown when the same blank node appears
 * in the context position of two or more statements having a distinct
 * subject predicate, and object. This is an error because it implies that
 * two statements with different bindings are the same statement.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class UnificationException extends RuntimeException {
    
    /**
     * 
     */
    private static final long serialVersionUID = 6430403508687043789L;

    public UnificationException(String msg) {
        
        super(msg);
        
    }
    
}