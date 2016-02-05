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
package com.bigdata.search;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.btree.proc.LongAggregator;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

/**
 * Writes on the text index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TextIndexWriteProc extends AbstractKeyArrayIndexProcedure<Long>
        implements IParallelizableIndexProcedure<Long>,
        IMutableRelationIndexWriteProcedure<Long> {

    /**
     * 
     */
    private static final long serialVersionUID = 9013449121306914750L;

    private static transient final Logger log = Logger.getLogger(TextIndexWriteProc.class);
    
    public static class IndexWriteProcConstructor extends
            AbstractKeyArrayIndexProcedureConstructor<TextIndexWriteProc> {

        /**
         * Variant which always overwrites any existing entry. Note that you
         * must still delete all entries for a document before re-indexing that
         * document.
         */
        public static TextIndexWriteProc.IndexWriteProcConstructor OVERWRITE = new IndexWriteProcConstructor(
                true);

        /**
         * Variant that will not overwrite an existing entry for a
         * {term,doc,field}. This is useful when you have a corpus which (a)
         * only grows in size; and (b) the content of each document is
         * unchanging.
         */
        public static TextIndexWriteProc.IndexWriteProcConstructor NO_OVERWRITE = new IndexWriteProcConstructor(
                false);
        
        private final boolean overwrite;

        /**
         * Values are required.
         */
        @Override
        public final boolean sendValues() {
        
            return true;
            
        }
        
        /**
         * 
         * @param overwrite
         */
        private IndexWriteProcConstructor(final boolean overwrite) {
            
            this.overwrite = overwrite;
            
        }
        
        @Override
        public TextIndexWriteProc newInstance(final IRabaCoder keySer,
                final IRabaCoder valSer, final int fromIndex,
                final int toIndex, final byte[][] keys, final byte[][] vals) {

            return new TextIndexWriteProc(keySer, valSer, fromIndex, toIndex,
                    keys, vals, overwrite);

        }

    }
    
    /**
     * De-serialization constructor.
     */
    public TextIndexWriteProc() {
        
    }
    
    private boolean overwrite;
    
    protected TextIndexWriteProc(final IRabaCoder keySer,
            final IRabaCoder valSer, final int fromIndex, final int toIndex,
            final byte[][] keys, final byte[][] vals, final boolean overwrite) {

        super(keySer, valSer, fromIndex, toIndex, keys, vals);
        
        assert vals != null;
        
    }

    @Override
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * @return The #of pre-existing tuples that were updated as an
     *         {@link Integer}.
     */
    @Override
    public Long applyOnce(final IIndex ndx, final IRaba keys, final IRaba vals) {

        long updateCount = 0;

        final int n = keys.size();

        for (int i = 0; i < n; i++) {

            final byte[] key = keys.get(i);
            assert key != null;
            assert key.length > 0;

            /*
             * Note: The value MAY be used to encoded information. While it is
             * no longer used to encode information in the com.bigdata.search
             * package, the RDF specific full text indices still use the value.
             * Therefore it now MAY be null and these asserts have been removed.
             */
            final byte[] val = vals.get(i);
//            assert val != null;
//            assert val.length > 0;

            /*
             * Write on the index if (a) overwrite was specified; or (b) the
             * index does not contain an entry for the key.
             * 
             * Note: This is an optimization which avoids mutation of the btree
             * when there would be no change in the data.
             */
            if(overwrite) {

            	// overwrite.
            	if (ndx.insert(key, val) != null) {

					updateCount++;

				}
            	
            } else {
            	
            	// conditional mutation.
				if (ndx.putIfAbsent(key, val) != null) {

                    updateCount++;

            	}
            	
            }

//            final boolean write = overwrite || !ndx.contains(key);
//            
//            if (write && ndx.insert(key, val) != null) {
//                
//                updateCount++;
//                
//            }

        }

        if (log.isInfoEnabled())
            log.info("wrote " + n + " tuples of which " + updateCount
                    + " were updated rows");
        
        return updateCount;
        
    }
    
    @Override
    protected void readMetadata(final ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readMetadata(in);
        
        overwrite = in.readBoolean();
        
    }

    /**
     * Extended to write the {@link #overwrite} flag.
     */
    @Override
    protected void writeMetadata(final ObjectOutput out) throws IOException {

        super.writeMetadata(out);
        
        out.writeBoolean(overwrite);
        
    }

	/**
	 * Uses {@link LongAggregator} to combine the mutation counts.
	 */
	@Override
	protected IResultHandler<Long, Long> newAggregator() {

		return new LongAggregator();
		
	}
    
}
