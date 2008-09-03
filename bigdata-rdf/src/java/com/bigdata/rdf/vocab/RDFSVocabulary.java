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

package com.bigdata.rdf.vocab;

import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A {@link Vocabulary} including well-known {@link Value}s for RDF, RDF
 * Schema, and owl:sameAs and friends.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RDFSVocabulary extends BaseVocabulary {

    private static final long serialVersionUID = -6646094201371729113L;

    /**
     * De-serialization ctor.
     */
    public RDFSVocabulary() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param db
     *            The database.
     */
    public RDFSVocabulary(AbstractTripleStore db) {

        super( db );
        
    }

    @Override
    protected void addValues() {

        add(RDF.TYPE);
        add(RDF.PROPERTY);
        add(RDFS.SUBCLASSOF);
        add(RDFS.SUBPROPERTYOF);
        add(RDFS.DOMAIN);
        add(RDFS.RANGE);
        add(RDFS.CLASS);
        add(RDFS.RESOURCE);
        add(RDFS.CONTAINERMEMBERSHIPPROPERTY);
        add(RDFS.DATATYPE);
        add(RDFS.MEMBER);
        add(RDFS.LITERAL);

        add(OWL.SAMEAS);
        add(OWL.EQUIVALENTCLASS);
        add(OWL.EQUIVALENTPROPERTY);
        add(OWL.INVERSEOF);
        add(OWL.CLASS);
        add(OWL.OBJECTPROPERTY);
        add(OWL.TRANSITIVEPROPERTY);
        add(OWL.DATATYPEPROPERTY);

    }

}
