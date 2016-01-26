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
 * Created on Jan 7, 2008
 */

package com.bigdata.rdf.spo;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.rdf.inf.Justification;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

/**
 * Procedure for writing {@link Justification}s on an index or index
 * partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class JustIndexWriteProc
        extends AbstractKeyArrayIndexProcedure<Long>
        implements IParallelizableIndexProcedure<Long>, IMutableRelationIndexWriteProcedure<Long> {

    /**
     * 
     */
    private static final long serialVersionUID = -7469842097766417950L;

    @Override
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * De-serialization constructor.
     *
     */
    public JustIndexWriteProc() {
        
        super();

    }

    public JustIndexWriteProc(IRabaCoder keySer, int fromIndex,
            int toIndex, byte[][] keys) {

        super(keySer, null, fromIndex, toIndex, keys, null/* vals */);

    }

    public static class WriteJustificationsProcConstructor extends
            AbstractKeyArrayIndexProcedureConstructor<JustIndexWriteProc> {

        public static WriteJustificationsProcConstructor INSTANCE = new WriteJustificationsProcConstructor();

        /**
         * Values ARE NOT used.
         */
        @Override
        public final boolean sendValues() {
        
            return false;
            
        }
        
        private WriteJustificationsProcConstructor() {
        }

        @Override
        public JustIndexWriteProc newInstance(IRabaCoder keySer,
                IRabaCoder valSer, int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            assert vals == null;

            return new JustIndexWriteProc(keySer, fromIndex, toIndex, keys);

        }

    }

    /**
     * @return The #of justifications actually written on the index as a
     *         {@link Long}.
     */
    @Override
    public Long applyOnce(final IIndex ndx, final IRaba keys, final IRaba vals) {

        long nwritten = 0;
        
        final int n = keys.size();
        
        for (int i = 0; i < n; i++) {

            final byte[] key = keys.get( i );
            
			/*
			 * Note: We can not decide nwritten using putIfAbsent() since the
			 * index is storing nulls.
			 * 
			 * See BLZG-1539.
			 */
            if (!ndx.contains(key)) {

                ndx.insert(key, null/* no value */);

                nwritten++;

            }

        }
        
        return Long.valueOf(nwritten);
        
    }

    /**
     * Uses {@link LongAggregator} to combine the mutation counts.
     */
	@Override
	protected IResultHandler<Long, Long> newAggregator() {

		return new LongAggregator();

	}
    
}
