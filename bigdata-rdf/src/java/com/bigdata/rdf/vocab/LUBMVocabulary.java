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

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.decls.LUBMVocabularyDecl;
import com.bigdata.rdf.vocab.decls.OWLVocabularyDecl;
import com.bigdata.rdf.vocab.decls.RDFSVocabularyDecl;
import com.bigdata.rdf.vocab.decls.RDFVocabularyDecl;

/**
 * A {@link Vocabulary} covering the ontologies used by LUBM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LUBMVocabulary extends BaseVocabulary {

    /**
     * De-serialization ctor.
     */
    public LUBMVocabulary() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public LUBMVocabulary(final String namespace) {

        super( namespace );
        
    }

    @Override
    protected void addValues() {

        addDecl(new RDFVocabularyDecl());
        addDecl(new RDFSVocabularyDecl());
        addDecl(new OWLVocabularyDecl());
        addDecl(new LUBMVocabularyDecl());

    }

}
