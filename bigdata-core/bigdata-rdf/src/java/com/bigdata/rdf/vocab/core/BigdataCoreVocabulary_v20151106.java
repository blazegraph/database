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

package com.bigdata.rdf.vocab.core;

import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.BaseVocabularyDecl;
import com.bigdata.rdf.vocab.DefaultBigdataVocabulary;

/**
 * Core Bigdata vocabulary.
 *  
 * Note: Do not modify this class.  Create an entirely new vocabulary that
 * extends this one and edit
 * {@link AbstractTripleStore.Options#DEFAULT_VOCABULARY_CLASS}.
 */
public class BigdataCoreVocabulary_v20151106 extends DefaultBigdataVocabulary {

    /**
     * De-serialization ctor.
     */
    public BigdataCoreVocabulary_v20151106() {
        
        super();
        
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public BigdataCoreVocabulary_v20151106(final String namespace) {

        super(namespace);
        
    }

    @Override
    protected void addValues() {

        super.addValues();
        
        /*
         * Some new URIs for inline URI handling.
         */
        addDecl(new BaseVocabularyDecl(
                PackedLongIV.PACKED_LONG, // supported range: [0;72057594037927935L].
                CompressedTimestampExtension.COMPRESSED_TIMESTAMP                
                ));

    }

}
