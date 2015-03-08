/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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

import java.util.Collection;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.axioms.OwlAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;

public class SampleAxioms extends OwlAxioms {
    
    /**
     * De-serialization ctor.
     */
    public SampleAxioms() {

        super();
        
    }
    
    public SampleAxioms(final String namespace) {
        
        super(namespace);
        
    }

	/**
	 * Per RDF Model Theory, new properties defined in the vocabulary will
	 * always be of rdf:type rdf:Property and rdfs:Resource, and they will
	 * always be subproperties of themselves. By defining these as axioms, we
	 * save the inference engine some work by not having to keep track of
	 * proving this information over and over again.
	 */
    protected void addAxioms(Collection<BigdataStatement> axioms) {

        super.addAxioms(axioms);

        final BigdataValueFactory valueFactory = getValueFactory();
        
        // axioms for #similarTo
        axioms.add( valueFactory.createStatement( SAMPLE.SIMILAR_TO, RDF.TYPE, RDF.PROPERTY, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( SAMPLE.SIMILAR_TO, RDF.TYPE, RDFS.RESOURCE, null, StatementEnum.Axiom));
        axioms.add( valueFactory.createStatement( SAMPLE.SIMILAR_TO, RDFS.SUBPROPERTYOF, SAMPLE.SIMILAR_TO, null, StatementEnum.Axiom));
 
    }
    
}
