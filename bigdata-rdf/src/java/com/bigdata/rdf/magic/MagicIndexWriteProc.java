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
 * Created on Jan 25, 2008
 */
package com.bigdata.rdf.magic;

import java.io.Externalizable;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

public class MagicIndexWriteProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure, IMutableRelationIndexWriteProcedure, 
        Externalizable {

    private static final long serialVersionUID = -6213370004972073550L;

    protected transient static final Logger log = Logger
            .getLogger(MagicIndexWriteProc.class);

    final transient protected boolean INFO = log.isInfoEnabled();
    final transient protected boolean DEBUG = log.isDebugEnabled();

    public final boolean isReadOnly() {

        return false;

    }

    /**
     * De-serialization constructor.
     */
    public MagicIndexWriteProc() {

    }

    /**
     * 
     * @param fromIndex
     * @param toIndex
     * @param keys
     * @param vals
     */
    protected MagicIndexWriteProc(IRabaCoder keySer, IRabaCoder valSer,
            int fromIndex, int toIndex, byte[][] keys, byte[][] vals) {

        super(keySer, valSer, fromIndex, toIndex, keys, vals);

        assert vals != null;

    }

    public static class IndexWriteProcConstructor extends
            AbstractKeyArrayIndexProcedureConstructor<MagicIndexWriteProc> {

        public static IndexWriteProcConstructor INSTANCE = new IndexWriteProcConstructor();

        private IndexWriteProcConstructor() {
        }

        /**
         * Values are required.
         */
        public final boolean sendValues() {
        
            return true;
            
        }
        
        public MagicIndexWriteProc newInstance(IRabaCoder keySer,
                IRabaCoder valSer, int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            return new MagicIndexWriteProc(keySer,valSer,fromIndex, toIndex, keys, vals);

        }
        
    }
    
    /**
     * 
     * @return The #of statements actually written on the index as an
     *         {@link Long}.
     */
    public Object apply(final IIndex ndx) {

        // #of statements actually written on the index partition.
        long writeCount = 0;

        final int n = getKeyCount();

        for (int i = 0; i < n; i++) {

            // the key encodes the {s:p:o} of the statement.
            final byte[] key = getKey(i);
            assert key != null;

            if (ndx.contains(key) == false) {

                /*
                 * Magic tuple is NOT pre-existing.
                 */

                ndx.insert(key, null);

                writeCount++;

            }

        }

        return Long.valueOf(writeCount);

    }

}
