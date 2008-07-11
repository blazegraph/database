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
 * Created on May 21, 2007
 */

package com.bigdata.rdf.store;

import java.util.Properties;

import com.bigdata.journal.IIndexManager;

/**
 * Abstract base class for both transient and persistent {@link ITripleStore}
 * implementations using local storage.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractLocalTripleStore extends AbstractTripleStore {

    /**
     * @param indexManager
     * @param namespace
     * @param timestamp
     * @param properties
     */
    protected AbstractLocalTripleStore(IIndexManager indexManager,
            String namespace, Long timestamp, Properties properties) {

        super(indexManager, namespace, timestamp, properties);

    }
    
//    /**
//     * Shared {@link IndexMetadata} configuration.
//     * 
//     * @param name
//     *            The index name.
//     * 
//     * @return A new {@link IndexMetadata} object for that index.
//     */
//    protected IndexMetadata getIndexMetadata(String name) {
//
//        final IndexMetadata metadata = new IndexMetadata(name, UUID.randomUUID());
//
//        metadata.setBranchingFactor(branchingFactor);
//
//        /*
//         * Note: Mainly used for torture testing.
//         */
//        if(false){
//            
//            // An override that makes a split very likely.
//            final ISplitHandler splitHandler = new DefaultSplitHandler(
//                    10 * Bytes.kilobyte32, // minimumEntryCount
//                    50 * Bytes.kilobyte32, // entryCountPerSplit
//                    1.5, // overCapacityMultiplier
//                    .75, // underCapacityMultiplier
//                    20 // sampleRate
//            );
//            
//            metadata.setSplitHandler(splitHandler);
//            
//        }
//                
//        return metadata;
//
//    }
//
//    /**
//     * Overrides for the {@link IRawTripleStore#getTerm2IdIndex()}.
//     */
//    protected IndexMetadata getTerm2IdIndexMetadata(String name) {
//
//        final IndexMetadata metadata = getIndexMetadata(name);
//
//        return metadata;
//
//    }
//
//    /**
//     * Overrides for the {@link IRawTripleStore#getId2TermIndex()}.
//     */
//    protected IndexMetadata getId2TermIndexMetadata(String name) {
//
//        final IndexMetadata metadata = getIndexMetadata(name);
//
//        return metadata;
//
//    }
//
//    /**
//     * Overrides for the statement indices.
//     */
//    protected IndexMetadata getStatementIndexMetadata(String name) {
//
//        final IndexMetadata metadata = getIndexMetadata(name);
//
//        metadata.setLeafKeySerializer(FastRDFKeyCompression.N3);
//
//        if (!statementIdentifiers) {
//
//            /*
//             * FIXME this value serializer does not know about statement
//             * identifiers. Therefore it is turned off if statement identifiers
//             * are enabled. Examine some options for value compression for the
//             * statement indices when statement identifiers are enabled.
//             */
//
//            metadata.setLeafValueSerializer(new FastRDFValueCompression());
//
//        }
//        
//        return metadata;
//
//    }
//
//    /**
//     * Overrides for the {@link IRawTripleStore#getJustificationIndex()}.
//     */
//    protected IndexMetadata getJustIndexMetadata(String name) {
//
//        final IndexMetadata metadata = getIndexMetadata(name);
//
//        metadata.setLeafValueSerializer(NoDataSerializer.INSTANCE);
//
//        return metadata;
//
//    }

}
