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
/*
 * Created on September 24, 2015
 */
package com.blazegraph.vocab.pubchem;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20151210;

/**
 * 
 * A {@link Vocabulary} covering the PubChem data from {@link https://pubchem.ncbi.nlm.nih.gov/rdf/}.
 * Use the vocabulary by adding a property to your journal file per below.
 * 
 * <code>
 * com.bigdata.rdf.store.AbstractTripleStore.vocabularyClass=com.blazegraph.vocab.pubchem.PubChemVocabulary
 * </code>
 * 
 * @author beebs
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 * 
 */
public class PubChemVocabulary extends BigdataCoreVocabulary_v20151210 {

    /**
     * De-serialization ctor.
     */
    public PubChemVocabulary() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public PubChemVocabulary(final String namespace) {

        super( namespace );
        
    }

    @Override
    protected void addValues() {

        addDecl(new PubChemVocabularyDecl());
        
        super.addValues();

    }

}
