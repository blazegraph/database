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
package com.bigdata.rdf.lexicon;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.CognitiveWeb.extser.LongPacker;
import org.CognitiveWeb.extser.ShortPacker;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractIndexProcedureConstructor;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IParallelizableIndexProcedure;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IResultHandler;
import com.bigdata.btree.ResultSet;
import com.bigdata.btree.AbstractTupleFilterator.CompletionScan;
import com.bigdata.rdf.spo.DistinctTermScanner;

/**
 * This unisolated operation matches terms in the <em>term:id</em> index
 * having any of the given strings as a prefix. This may be used to auto-suggest
 * terms in the KB based on some user input. Optionally, when the given string
 * is longer than the term, the string MUST extend the term on a "word boundary" -
 * this constraint may be used to identify only exact matches and phrase
 * completions.
 * 
 * @deprecated This is not finished yet. See {@link CompletionScan} and
 *             {@link DistinctTermScanner}. These things need support for
 *             {@link IRangeQuery#CURSOR}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PrefixScanProc extends AbstractKeyArrayIndexProcedure implements
        IParallelizableIndexProcedure {

    private static final long serialVersionUID = 3024326045069513835L;

    protected static final Logger log = Logger.getLogger(PrefixScanProc.class);
    
    /**
     * De-serialization constructor.
     */
    public PrefixScanProc() {
        
    }
    
    protected PrefixScanProc(IDataSerializer keySer, int fromIndex, int toIndex,
            byte[][] keys) {

        super(keySer, null, fromIndex, toIndex, keys, null /* vals */);
        
    }

    public static class PrefixScanProcConstructor extends
            AbstractIndexProcedureConstructor<PrefixScanProc> {

        public static PrefixScanProcConstructor INSTANCE = new PrefixScanProcConstructor();

        private PrefixScanProcConstructor() {
        
        }
        
        public PrefixScanProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer,int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            assert vals == null;
            
            return new PrefixScanProc(keySer, fromIndex, toIndex, keys);

        }

    }

    /**
     * For each key prefix mapped to the current index partition, use a
     * key-range scan to lookup all tuples in the index having a given key
     * prefix. The given key prefixes must be in sorted order. Each prefix is
     * treated in turn, and the set of tuples having that prefix are buffered by
     * the {@link ResultSet}. The scan then restarts with the next given key
     * prefix.
     * <p>
     * If the request capacity is exceeded then the need for a continuation
     * query is noted and the key at which that continuation query would begin
     * is included in the {@link ResultSet}.
     * 
     * @param ndx
     *            The index.
     * 
     * @return The {@link ResultSet}.
     * 
     * FIXME handle continuation queries.
     * 
     * FIXME write a pull style iterator that implements {@link IResultHandler}
     * and reads from the {@link ResultSet}s as they arrive, causing
     * continuation queries to be issued as necessary. This must include the
     * logic for handling stale locators and would ideally be used by the
     * standard iterator as well.
     */
    public ResultSet apply(IIndex ndx) {
        
        final int n = getKeyCount();
        
        for (int i = 0; i < n; i++) {

            final byte[] key = getKey(i);

            // Lookup in the forward index.
            final byte[] tmp = ndx.lookup(key);

            if (tmp == null) continue;

        }

        // FIXME This impl is not finished.
        throw new UnsupportedOperationException();

    }

//    protected void readMetadata(ObjectInput in) throws IOException, ClassNotFoundException {
//        
//        super.readMetadata(in);
//        
//        phraseMatch = in.readBoolean();
//        
//    }
//
//    protected void writeMetadata(ObjectOutput out) throws IOException {
//        
//        super.writeMetadata(out);
//        
//        out.writeBoolean(phraseMatch);
//        
//    }
    
    /**
     * Object encapsulates the discovered term identifiers and provides
     * efficient serialization for communication of those data to the client.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Result implements Externalizable {

        private static final long serialVersionUID = 2682323152107786634L;
        
        public long[] ids;

        /**
         * De-serialization constructor.
         */
        public Result() {
            
        }
        
        public Result(long[] ids) {

            assert ids != null;
            
            assert ids.length > 0;
            
            this.ids = ids;
            
        }

        private final static transient short VERSION0 = 0x0;
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            final short version = ShortPacker.unpackShort(in);
            
            if(version!=VERSION0) {
                
                throw new IOException("Unknown version: "+version);
                
            }
            
            final int n = (int) LongPacker.unpackLong(in);
            
            ids = new long[n];
            
            for(int i=0; i<n; i++) {
                
                ids[i] = LongPacker.unpackLong(in);
                
            }
            
        }

        public void writeExternal(ObjectOutput out) throws IOException {

            final int n = ids.length;
            
            ShortPacker.packShort(out, VERSION0);
            
            LongPacker.packLong(out,n);

            for(int i=0; i<n; i++) {
                
                LongPacker.packLong(out, ids[i]);
                
            }
            
        }
        
    }
    
}
