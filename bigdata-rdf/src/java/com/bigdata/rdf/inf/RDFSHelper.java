/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Oct 28, 2007
 */

package com.bigdata.rdf.inf;

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
                rdfsLiteral
                
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

    }

}
