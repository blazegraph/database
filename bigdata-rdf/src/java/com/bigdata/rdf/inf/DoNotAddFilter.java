package com.bigdata.rdf.inf;

import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPOFilter;
import com.bigdata.rdf.spo.SPO;

/**
 * Filter keeps matched triple patterns generated OUT of the database.
 * <p>
 * Note: {@link StatementEnum#Explicit} triples are always rejected by this
 * filter so that explicitly asserted triples will always be stored in the
 * database.
 * <p>
 * Note: {@link StatementEnum#Axiom}s are always rejected by this filter so
 * that they will be stored in the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DoNotAddFilter implements ISPOFilter {

    private final InferenceEngine inf;
    
    public DoNotAddFilter(InferenceEngine inf) {
        
        this.inf = inf;
        
    }

    public boolean isMatch(SPO spo) {

        if((spo.s & 0x01L) == 1L) {
            
            /*
             * Note: Explicitly toss out entailments that would place a
             * literal into the subject position. These statements can enter
             * the database via rdfs3 and rdfs4b.
             */

            return true;
            
        }
        
        if (spo.type == StatementEnum.Explicit
                || spo.type == StatementEnum.Axiom) {
            
            // Accept all explicit triples or axioms.
            
            return false;
            
        }

        if (!inf.forwardChainRdfTypeRdfsResource && spo.p == inf.rdfType.id
                && spo.o == inf.rdfsResource.id) {
            
            // reject (?x, rdf:type, rdfs:Resource ) 
            
            return true;
            
        }
        
        // Accept everything else.
        
        return false;
        
    }
    
}