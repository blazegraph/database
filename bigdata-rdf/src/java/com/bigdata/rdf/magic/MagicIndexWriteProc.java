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

import java.io.IOException;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

/**
 * Procedure for batch index on a single statement index (or index partition).
 * <p>
 * The key for each statement encodes the {s:p:o} of the statement in the order
 * that is appropriate for the index (SPO, POS, OSP, etc). The key is written
 * unchanged on the index.
 * <p>
 * The value for each statement is a byte that encodes the {@link StatementEnum}
 * and also encodes whether or not the "override" flag is set using - see
 * {@link StatementEnum#MASK_OVERRIDE} - followed by 8 bytes representing the
 * statement identifier IFF statement identifiers are enabled AND the
 * {@link StatementEnum} is {@link StatementEnum#Explicit}. The value requires
 * interpretation to determine the byte[] that will be written as the value on
 * the index - see the code for more details.
 * <p>
 * Note: This needs to be a custom batch operation using a conditional insert so
 * that we do not write on the index when the data would not be changed and to
 * handle the overflow flag and the optional statement identifier correctly.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MagicIndexWriteProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure, IMutableRelationIndexWriteProcedure {

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
    protected MagicIndexWriteProc(IDataSerializer keySer, IDataSerializer valSer,
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
        
        public MagicIndexWriteProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer, int fromIndex, int toIndex,
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

        // used to generate the values that we write on the index.
        final ByteArrayBuffer tmp = new ByteArrayBuffer(1 + 8/* max size */);

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
