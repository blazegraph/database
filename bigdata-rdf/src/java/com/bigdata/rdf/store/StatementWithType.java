package com.bigdata.rdf.store;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;

import com.bigdata.rdf.model.StatementEnum;

/**
 * Also reports whether the statement is explicit, inferred or an axiom.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StatementWithType extends StatementImpl {

    /**
     * 
     */
    private static final long serialVersionUID = 6739949195958368365L;

    private StatementEnum type;
    
    /**
     * @param subject
     * @param predicate
     * @param object
     */
    public StatementWithType(Resource subject, URI predicate, Value object,StatementEnum type) {
        
        super(subject, predicate, object);
        
        this.type = type;
        
    }

    /**
     * Whether the statement is explicit, inferred or an axiom.
     */
    public StatementEnum getStatementType() {
        
        return type;
        
    }
    
}