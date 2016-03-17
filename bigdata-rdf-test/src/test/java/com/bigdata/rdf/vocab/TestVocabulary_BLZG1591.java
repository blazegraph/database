/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.decls.BSBMVocabularyDecl;
import com.bigdata.rdf.vocab.decls.DCAllVocabularyDecl;
import com.bigdata.rdf.vocab.decls.RDFSVocabularyDecl;
import com.bigdata.rdf.vocab.decls.RDFVocabularyDecl;
import com.bigdata.rdf.vocab.decls.XMLSchemaVocabularyDecl;

/**
 * A {@link Vocabulary} covering the ontologies used by LUBM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestVocabulary_BLZG1591 extends BaseVocabulary {

    /**
     * De-serialization ctor.
     */
    public TestVocabulary_BLZG1591() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public TestVocabulary_BLZG1591(final String namespace) {

        super( namespace );
        
    }

    @Override
    protected void addValues() {

        addDecl(new TestVocabularyDecl_BLZG1591());
        addDecl(new RDFVocabularyDecl());
        addDecl(new RDFSVocabularyDecl());
        addDecl(new DCAllVocabularyDecl());
        addDecl(new XMLSchemaVocabularyDecl());
        addDecl(new BSBMVocabularyDecl());

    }

}
