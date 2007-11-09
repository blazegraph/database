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

import org.openrdf.vocabulary.OWL;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Resolves or defines well-known RDF values against an {@link AbstractTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFSHelper {

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
        
        store.insertTerms(terms, terms.length, false/*haveKeys*/, false/*sorted*/);

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

}
