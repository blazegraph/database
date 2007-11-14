package com.bigdata.rdf.inf;

import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.SPO;

/**
 * Filter matches <code>(x rdf:type rdfs:Resource).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RdfTypeRdfsResourceFilter implements ISPOFilter {

    private final long rdfType;
    private final long rdfsResource;
    
    /**
     * 
     * @param vocab
     */
    public RdfTypeRdfsResourceFilter(RDFSHelper vocab) {
        
        this.rdfType = vocab.rdfType.id;
        
        this.rdfsResource = vocab.rdfsResource.id;
        
    }

    public boolean isMatch(SPO spo) {

        if (spo.p == rdfType && spo.o == rdfsResource) {
            
            // reject (?x, rdf:type, rdfs:Resource )
            
            return true;
            
        }
        
        // Accept everything else.
        
        return false;
        
    }
    
}