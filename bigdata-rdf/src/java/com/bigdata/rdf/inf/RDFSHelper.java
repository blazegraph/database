/**

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
/*
 * Created on Oct 28, 2007
 */

package com.bigdata.rdf.inf;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AccessPathFusedView;
import com.bigdata.rdf.store.IAccessPath;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Resolves or defines well-known RDF values against an {@link AbstractTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFSHelper {

    /**
     * Value used for a "NULL" term identifier.
     */
    public final long NULL = IRawTripleStore.NULL;
    
    final static public Logger log = Logger.getLogger(RDFSHelper.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final static public boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final static public boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * The database that is the authority for the defined terms and term
     * identifiers.
     */
    final public AbstractTripleStore database;
    
    /*
     * Identifiers for well-known RDF values. 
     */
    public final Id rdfType;
    public final Id rdfProperty;
    public final Id rdfsSubClassOf;
    public final Id rdfsSubPropertyOf;
    public final Id rdfsDomain;
    public final Id rdfsRange;
    public final Id rdfsClass;
    public final Id rdfsResource;
    public final Id rdfsCMP;
    public final Id rdfsDatatype;
    public final Id rdfsMember;
    public final Id rdfsLiteral;
    
    public final Id owlSameAs;
    public final Id owlEquivalentClass;
    public final Id owlEquivalentProperty;

    /**
     * Resolves or defines well-known RDF values.
     * 
     * @see #rdfType and friends which are initialized by this method.
     */
    public RDFSHelper(AbstractTripleStore store) {

        if (store == null)
            throw new IllegalArgumentException();

        this.database = store;
        
        _Value rdfType = new _URI(RDF.TYPE);
        _Value rdfProperty = new _URI(RDF.PROPERTY);
        _Value rdfsSubClassOf = new _URI(RDFS.SUBCLASSOF);
        _Value rdfsSubPropertyOf = new _URI(RDFS.SUBPROPERTYOF);
        _Value rdfsDomain = new _URI(RDFS.DOMAIN);
        _Value rdfsRange = new _URI(RDFS.RANGE);
        _Value rdfsClass = new _URI(RDFS.CLASS);
        _Value rdfsResource = new _URI(RDFS.RESOURCE);
        _Value rdfsCMP = new _URI(RDFS.CONTAINERMEMBERSHIPPROPERTY);
        _Value rdfsDatatype = new _URI(RDFS.DATATYPE);
        _Value rdfsMember = new _URI(RDFS.MEMBER);
        _Value rdfsLiteral = new _URI(RDFS.LITERAL);
        
        _Value owlSameAs = new _URI(OWL.SAMEAS);
        _Value owlEquivlentClass = new _URI(OWL.EQUIVALENTCLASS);
        _Value owlEquivlentProperty = new _URI(OWL.EQUIVALENTPROPERTY);
        
        _Value[] terms = new _Value[]{
        
                rdfType,
                rdfProperty,
                rdfsSubClassOf,
                rdfsSubPropertyOf,
                rdfsDomain,
                rdfsRange,
                rdfsClass,
                rdfsResource,
                rdfsCMP,
                rdfsDatatype,
                rdfsMember,
                rdfsLiteral,
                
                owlSameAs,
                owlEquivlentClass,
                owlEquivlentProperty
                
        };
        
        store.addTerms(terms, terms.length);

        this.rdfType = new Id(rdfType.termId);
        this.rdfProperty = new Id(rdfProperty.termId);
        this.rdfsSubClassOf = new Id(rdfsSubClassOf.termId);
        this.rdfsSubPropertyOf= new Id(rdfsSubPropertyOf.termId);
        this.rdfsDomain = new Id(rdfsDomain.termId);
        this.rdfsRange = new Id(rdfsRange.termId);
        this.rdfsClass = new Id(rdfsClass.termId);
        this.rdfsResource = new Id(rdfsResource.termId);
        this.rdfsCMP = new Id(rdfsCMP.termId);
        this.rdfsDatatype = new Id(rdfsDatatype.termId);
        this.rdfsMember = new Id(rdfsMember.termId);
        this.rdfsLiteral = new Id(rdfsLiteral.termId);

        this.owlSameAs = new Id(owlSameAs.termId);
        this.owlEquivalentClass = new Id(owlEquivlentClass.termId);
        this.owlEquivalentProperty = new Id(owlEquivlentProperty.termId);
        
    }


    /**
     * Computes the set of possible sub properties of rdfs:subPropertyOf (<code>P</code>).
     * This is used by steps 2-4 in {@link #fastForwardClosure()}.
     * 
     * @param focusStore
     * @param database
     * 
     * @return A set containing the term identifiers for the members of P.
     */
    public Set<Long> getSubProperties(AbstractTripleStore focusStore, AbstractTripleStore database) {

        final Set<Long> P = new HashSet<Long>();
        
        P.add(rdfsSubPropertyOf.id);
        
        /*
         * query := (?x, P, P), adding new members to P until P reaches fix
         * point.
         */
        {

            int nbefore;
            int nafter = 0;
            int nrounds = 0;

            Set<Long> tmp = new HashSet<Long>();

            do {

                nbefore = P.size();

                tmp.clear();

                /*
                 * query := (?x, p, ?y ) for each p in P, filter ?y element of
                 * P.
                 */

                for (Long p : P) {

                    final IAccessPath accessPath = (focusStore == null //
                            ? database.getAccessPath(NULL, p, NULL)//
                            : new AccessPathFusedView(focusStore.getAccessPath(
                                    NULL, p, NULL), //
                                    database.getAccessPath(NULL, p, NULL)//
                            ));

                    ISPOIterator itr = accessPath.iterator();

                    while(itr.hasNext()) {
                        
                        SPO[] stmts = itr.nextChunk();
                            
                        for(SPO stmt : stmts) {

                            if (P.contains(stmt.o)) {

                                tmp.add(stmt.s);

                            }

                        }

                    }
                    
                }

                P.addAll(tmp);

                nafter = P.size();

                nrounds++;

            } while (nafter > nbefore);

        }
        
        if(DEBUG){
            
            Set<String> terms = new HashSet<String>();
            
            for( Long id : P ) {
                
                terms.add(database.toString(id));
                
            }
            
            log.debug("P: "+terms);
        
        }
        
        return P;

    }
    
    /**
     * Query the <i>database</i> for the sub properties of a given property.
     * <p>
     * Pre-condition: The closure of <code>rdfs:subPropertyOf</code> has been
     * asserted on the database.
     * 
     * @param focusStore
     * @param database
     * @param p
     *            The term identifier for the property whose sub-properties will
     *            be obtain.
     * 
     * @return A set containing the term identifiers for the sub properties of
     *         <i>p</i>.
     */
    public Set<Long> getSubPropertiesOf(AbstractTripleStore focusStore,
            AbstractTripleStore database, final long p) {

        final IAccessPath accessPath = //
        (focusStore == null //
        ? database.getAccessPath(NULL/* x */, rdfsSubPropertyOf.id, p)//
                : new AccessPathFusedView(//
                        focusStore.getAccessPath(NULL/* x */,
                                rdfsSubPropertyOf.id, p), //
                        database.getAccessPath(NULL/* x */,
                                rdfsSubPropertyOf.id, p)//
                ));

        if(DEBUG) {
            
            log.debug("p="+database.toString(p));
            
        }
        
        final Set<Long> tmp = new HashSet<Long>();

        /*
         * query := (?x, rdfs:subPropertyOf, p).
         * 
         * Distinct ?x are gathered in [tmp].
         * 
         * Note: This query is two-bound on the POS index.
         */

        ISPOIterator itr = accessPath.iterator();

        while(itr.hasNext()) {
            
            SPO[] stmts = itr.nextChunk();
            
            for( SPO spo : stmts ) {
                
                boolean added = tmp.add(spo.s);
                
                if(DEBUG) {
                    
                    log.debug(spo.toString(database) + ", added subject="+added);
                    
                }
                
            }

        }
        
        if(DEBUG){
        
            Set<String> terms = new HashSet<String>();
            
            for( Long id : tmp ) {
                
                terms.add(database.toString(id));
                
            }
            
            log.debug("sub properties: "+terms);
        
        }
        
        return tmp;

    }

}
