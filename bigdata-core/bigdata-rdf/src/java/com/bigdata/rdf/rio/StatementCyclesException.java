package com.bigdata.rdf.rio;

/**
 * An instance of this exception is thrown if cycles are detected amoung
 * statements. A cycle can exist only when statement identifiers are enabled
 * and a statement is made either directly about itself or indirectly via
 * one or more statements about itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementCyclesException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 3506732137721004208L;
    
    public StatementCyclesException(String msg) {
        
        super(msg);
        
    }
    
}