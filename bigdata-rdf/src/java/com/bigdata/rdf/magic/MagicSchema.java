package com.bigdata.rdf.magic;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.RelationSchema;

/**
 * Extensions for additional state maintained by the
 * {@link AbstractTripleStore} in the global row store.
 * 
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class MagicSchema extends RelationSchema {

    private static final String ns = MagicSchema.class.getPackage().getName()+".";
    
    /**
     * A serialized collection of symbol names, which correspond to the 
     * relations in the magic store.
     */
    public static final String SYMBOLS = ns + "symbols";
    
    /**
     * The arity of the relation.
     */
    public static final String ARITY = ns + "arity";

    /**
     * The key orders for the relation.
     */
    public static final String KEY_ORDERS = ns + "keyOrders";

    /**
     * De-serialization ctor.
     */
    public MagicSchema() {
        
    }
    
}
