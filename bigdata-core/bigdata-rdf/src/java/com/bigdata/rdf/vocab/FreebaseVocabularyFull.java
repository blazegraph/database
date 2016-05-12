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
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20160317;

/**
 * Vocabulary class for Freebase (full vocabulary).
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class FreebaseVocabularyFull extends BigdataCoreVocabulary_v20160317 {

    /**
     * De-serialization ctor.
     */
    public FreebaseVocabularyFull() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public FreebaseVocabularyFull(final String namespace) {

        super( namespace );
        
    }

    @Override
    protected void addValues() {

        super.addValues();
        
        addDecl(new FreebaseTypesVocabularyDecl());

        addDecl(new FreebasePredicateVocabularyDecl1());
        addDecl(new FreebasePredicateVocabularyDecl2());
        addDecl(new FreebasePredicateVocabularyDecl3());
        addDecl(new FreebasePredicateVocabularyDecl4());
        
        addDecl(new FreebaseInlinedUriVocabularyDecl());
   
    }

}