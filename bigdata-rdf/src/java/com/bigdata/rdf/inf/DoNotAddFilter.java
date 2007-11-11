/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
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

    private final long rdfType;
    private final long rdfsResource;
    private final boolean forwardChainRdfTypeRdfsResource;
    
    /**
     * 
     * @param vocab
     * @param forwardChainRdfTypeRdfsResource
     */
    public DoNotAddFilter(RDFSHelper vocab, boolean forwardChainRdfTypeRdfsResource) {
        
        this.rdfType = vocab.rdfType.id;
        
        this.rdfsResource = vocab.rdfsResource.id;
        
        this.forwardChainRdfTypeRdfsResource = forwardChainRdfTypeRdfsResource;
        
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

        if (!forwardChainRdfTypeRdfsResource && spo.p == rdfType
                && spo.o == rdfsResource) {
            
            // reject (?x, rdf:type, rdfs:Resource ) 
            
            return true;
            
        }
        
        // Accept everything else.
        
        return false;
        
    }
    
}