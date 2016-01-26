/*

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
 * Created on Jul 3, 2008
 */

package com.bigdata.sparse;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;
import com.bigdata.journal.TimestampUtility;
import com.bigdata.relation.AbstractRelation;

/**
 * Helper class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 *
 */
public class GlobalRowStoreHelper {

    static final public transient String GLOBAL_ROW_STORE_INDEX = "__globalRowStore";

   /**
    * Note: It is important that this reference is not exposed since that can
    * break isolation when using group commit by allowing the GRS index to be 
    * registered from within a task .
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/1132"> GlobalRowStoreHelper
    *      can hold hard reference to GSR index (GROUP COMMIT) </a>
    */
    private final IIndexManager indexManager;
    
    protected static final transient Logger log = Logger.getLogger(GlobalRowStoreHelper.class);
    
//    protected static final boolean INFO = log.isInfoEnabled();
    
    public GlobalRowStoreHelper(final IIndexManager indexManager) {
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;
        
    }
    
    /**
    * @return The unisolated view of the GRS index
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/867"> NSS concurrency problem
    *      with list namespaces and create namespace </a>
    * @see <a href="http://trac.blazegraph.com/ticket/1132"> GlobalRowStoreHelper
    *      can hold hard reference to GSR index (GROUP COMMIT) </a>
    */
    synchronized public SparseRowStore getGlobalRowStore() {

        if (log.isInfoEnabled())
            log.info("");

        final SparseRowStore globalRowStore;
//        if (globalRowStore == null) 
        {

            /**
             * The GRS view needs to be protected by an
             * UnisolatedReadWriteIndex.
             * 
             * @see <a href="http://trac.blazegraph.com/ticket/867"> NSS
             *      concurrency problem with list namespaces and create
             *      namespace </a>
             */
            IIndex ndx = AbstractRelation.getIndex(indexManager,
                    GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);
            
            if (ndx == null) {

                if (log.isInfoEnabled())
                    log.info("Global row store does not exist - will try to register now");
                
                try {

                    /*
                     * Note: This specifies an split handler that keeps the
                     * logical row together. This is a hard requirement. The
                     * atomic read/update guarantee depends on this.
                     * 
                     * @todo The global row store does not get properties so
                     * only system defaults are used when it is registered.
                     */
                    
                    final IndexMetadata indexMetadata = new IndexMetadata(
                            GLOBAL_ROW_STORE_INDEX, UUID.randomUUID());

                    // Ensure that splits do not break logical rows.
                    indexMetadata
                            .setSplitHandler(LogicalRowSplitHandler.INSTANCE);
/*
 * This is now handled by using the UTF8 encoding of the primary key regardless
 * of the collator mode chosen (this fixes the problem with embedded nuls).
 */
//                    if (CollatorEnum.JDK.toString().equals(
//                            System.getProperty(KeyBuilder.Options.COLLATOR))) {
//                        /*
//                         * The JDK RulesBasedCollator embeds nul bytes in the
//                         * Unicode sort keys. This makes them unsuitable for the
//                         * SparseRowStore, which can not locate the start of the
//                         * column name if there are embedded nuls in a Unicode
//                         * primary key. As a work around, this forces an ASCII
//                         * collation sequence if the JDK collator is the
//                         * default. This is not ideal since non-ascii
//                         * distinctions will be lost, but it is better than
//                         * being unable to decode the column names.
//                         */
//                        log.warn("Forcing ASCII collator.");
//                        indexMetadata
//                                .setTupleSerializer(new DefaultTupleSerializer(
//                                        new ASCIIKeyBuilderFactory()));
//                    }
                    
                    // Register the index.
                    indexManager.registerIndex(indexMetadata);

                } catch (Exception ex) {

                    throw new RuntimeException(ex);

                }

                /**
                 * The live view of the global row store must be wrapped by an
                 * UnisolatedReadWriteIndex on a Journal.
                 * 
                 * @see http://sourceforge.net/apps/trac/bigdata/ticket/616 (Row
                 *      store read/update not isolated on Journal)
                 */
                ndx = AbstractRelation.getIndex(indexManager,
                        GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);
//                ndx = indexManager.getIndex(GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);

                if (ndx == null) {

                    throw new RuntimeException("Could not find index?");

                }

            }

            // row store is flyweight wrapper around index.
            globalRowStore = new SparseRowStore(ndx);

        }
        
        return globalRowStore;

    }

   /**
    * Note: The hard reference to the unisolated view of the GSR is no longer
    * cached.
    * 
    * @see <a href="http://trac.blazegraph.com/ticket/1132"> GlobalRowStoreHelper
    *      can hold hard reference to GSR index (GROUP COMMIT) </a>
    */
//    private transient SparseRowStore globalRowStore;

    /**
     * Return a view of the global row store as of the specified timestamp IFF
     * the backing index exists as of that timestamp.
     */
    public SparseRowStore get(final long timestamp) {

        if (log.isInfoEnabled())
            log.info(TimestampUtility.toString(timestamp));

        if (timestamp == ITx.UNISOLATED) {

            /* This version does an implicit create if the GRS does not exist. */
            return getGlobalRowStore();

        }
        
        final IIndex ndx;
        
        /**
         * The live view of the global row store must be wrapped by an
         * UnisolatedReadWriteIndex on a Journal.
         * 
         * @see http://sourceforge.net/apps/trac/bigdata/ticket/616 (Row store
         *      read/update not isolated on Journal)
         */
        ndx = AbstractRelation.getIndex(indexManager, GLOBAL_ROW_STORE_INDEX,
                TimestampUtility.asHistoricalRead(timestamp));
        
//        ndx = indexManager.getIndex(GLOBAL_ROW_STORE_INDEX,
//                TimestampUtility.asHistoricalRead(timestamp));

        if (ndx == null) {

            return null;

        }

        return new SparseRowStore(ndx);

    }
    
}
