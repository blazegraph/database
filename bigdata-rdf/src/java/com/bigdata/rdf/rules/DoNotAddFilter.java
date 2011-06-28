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
package com.bigdata.rdf.rules;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.axioms.Axioms;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.eval.ISolution;

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
public class DoNotAddFilter<E extends ISPO> extends SPOFilter<E> {

//    protected static final Logger log = Logger.getLogger(DoNotAddFilter.class);
//    
//    protected static final boolean INFO = log.isInfoEnabled();
//    
//    protected static final boolean DEBUG = log.isDebugEnabled();
    
    /**
     * 
     */
    private static final long serialVersionUID = -7833182476134679170L;
    
    private final Axioms axioms;
    
    private final IV rdfType;
    private final IV rdfsResource;
    private final boolean forwardChainRdfTypeRdfsResource;
    
    /**
     * 
     * @param vocab
     *            The {@link Vocabulary}
     * @param axioms
     *            The {@link Axioms}.
     * @param forwardChainRdfTypeRdfsResource
     *            <code>true</code> if we generate the entailments for (x
     *            rdf:type rdfs:Resource) when the closure of the database is
     *            updated.
     */
    public DoNotAddFilter(final Vocabulary vocab, final Axioms axioms,
            boolean forwardChainRdfTypeRdfsResource) {

        if (vocab == null)
            throw new IllegalArgumentException();

        if (axioms == null)
            throw new IllegalArgumentException();
        
        this.axioms = axioms;

        this.forwardChainRdfTypeRdfsResource = forwardChainRdfTypeRdfsResource;

        if (!forwardChainRdfTypeRdfsResource && axioms.isRdfSchema()) {

            /*
             * If we are not forward chaining the type resource entailments then
             * we want to keep those statements out of the database since they
             * are materialized by the backward chainer. For that purpose we
             * need to save off the term identifier for rdf:type and
             * rdfs:Resource.
             */
            
            this.rdfType = vocab.get(RDF.TYPE);

            this.rdfsResource = vocab.get(RDFS.RESOURCE);

        } else {

            /*
             * These will not be used unless we are forward chaining type
             * resource entailments and they will not be defined unless the rdfs
             * axioms were specified (really, unless the RDFSVocabulary was
             * defined).
             */
         
            this.rdfType = this.rdfsResource = null;

        }

    }
    
    public boolean isValid(Object o) {

        if (!canAccept(o)) {
            
            return true;
            
        }
        
        return accept((ISPO) o);
        
    }

    private boolean accept(final ISPO o) {
        
        final ISPO spo = (ISPO) o;
        
        if(spo.s().isLiteral()) {
            
            /*
             * Note: Explicitly toss out entailments that would place a
             * literal into the subject position. These statements can enter
             * the database via rdfs3 and rdfs4b.
             */

            return false;
            
        }
        
        if (spo.getStatementType() == StatementEnum.Explicit ) {
            
            // Accept all explicit statements.
            
            return true;
            
        }

//        if (spo.isOverride()) {
//            
//            // Accept all statements with the override flag set.
//            
//            return true;
//            
//        }

        if( axioms.isAxiom(spo.s(), spo.p(), spo.o())) {
            
            /*
             * Reject all statements which correspond to axioms.
             * 
             * Note: This will let in explicit statements that correspond to
             * axioms since we let in all explicit statements above. The main
             * thing that this does is keep axioms generated by the rules from
             * showing up in the database, where they convert statements from
             * Axiom to Inferred.
             */
            
            return false;
            
        }

        if (!forwardChainRdfTypeRdfsResource && IVUtility.equals(spo.p(), rdfType)
                && IVUtility.equals(spo.o(), rdfsResource)) {
            
            // reject (?x, rdf:type, rdfs:Resource ) 
            
            return false;
            
        }
        
        // Accept everything else.
        
        return true;
        
    }
    
}
