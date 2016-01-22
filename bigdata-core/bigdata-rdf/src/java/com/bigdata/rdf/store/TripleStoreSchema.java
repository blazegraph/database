package com.bigdata.rdf.store;

import com.bigdata.rdf.axioms.Axioms;
import com.bigdata.rdf.vocab.RDFSVocabulary;
import com.bigdata.relation.RelationSchema;

/**
 * Extensions for additional state maintained by the
 * {@link AbstractTripleStore} in the global row store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TripleStoreSchema extends RelationSchema {

    /**
     * 
     */
    private static final long serialVersionUID = -4981670950510775408L;
    
    private static final String ns = TripleStoreSchema.class.getPackage().getName()+".";
    
    /**
     * The serialized {@link Axioms} as configured for the database.
     */
    public static final String AXIOMS = ns + "axioms";

    /**
     * The serialized {@link RDFSVocabulary} as configured for the database.
     */
    public static final String VOCABULARY = ns + "vocabulary";

    /**
     * De-serialization ctor.
     */
    public TripleStoreSchema() {
        
    }
    
}
