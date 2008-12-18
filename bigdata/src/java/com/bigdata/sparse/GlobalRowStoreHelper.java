/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jul 3, 2008
 */

package com.bigdata.sparse;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.ITx;

/**
 * Helper class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class GlobalRowStoreHelper {

    static final public transient String GLOBAL_ROW_STORE_INDEX = "__globalRowStore";

    private final IIndexManager indexManager;
    
    protected static final transient Logger log = Logger.getLogger(GlobalRowStoreHelper.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    public GlobalRowStoreHelper(IIndexManager indexManager) {
        
        if (indexManager == null)
            throw new IllegalArgumentException();

        this.indexManager = indexManager;
        
    }
    
    /**
     * @return
     */
    synchronized public SparseRowStore getGlobalRowStore() {

        if (INFO)
            log.info("");

        if (globalRowStore == null) {

            IIndex ndx = indexManager.getIndex(GLOBAL_ROW_STORE_INDEX,
                    ITx.UNISOLATED);

            if (ndx == null) {

                if (INFO)
                    log.info("Global row store does not exist - will try to register now");
                
                try {

                    /*
                     * @todo The global row store does not get properties so
                     * only system defaults are used when it is registered.
                     */
                    
                    indexManager.registerIndex(new IndexMetadata(GLOBAL_ROW_STORE_INDEX,
                            UUID.randomUUID()));

                } catch (Exception ex) {

                    throw new RuntimeException(ex);

                }

                ndx = indexManager.getIndex(GLOBAL_ROW_STORE_INDEX, ITx.UNISOLATED);

                if (ndx == null) {

                    throw new RuntimeException("Could not find index?");

                }

            }

            globalRowStore = new SparseRowStore(ndx);

        }
        
        return globalRowStore;

    }

    private transient SparseRowStore globalRowStore;

    /**
     * Return an {@link ITx#READ_COMMITTED} view IFF the backing index exists.
     */
    synchronized public SparseRowStore getReadCommitted() {

        if (INFO)
            log.info("");

        final IIndex ndx = indexManager.getIndex(GLOBAL_ROW_STORE_INDEX,
                ITx.READ_COMMITTED);

        if (ndx == null) {

            return null;

        }

        return new SparseRowStore(ndx);

    }
    
}
