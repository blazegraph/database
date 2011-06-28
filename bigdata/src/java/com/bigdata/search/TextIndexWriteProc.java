package com.bigdata.search;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.proc.AbstractKeyArrayIndexProcedureConstructor;
import com.bigdata.btree.proc.IParallelizableIndexProcedure;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.relation.IMutableRelationIndexWriteProcedure;

/**
 * Writes on the text index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TextIndexWriteProc extends AbstractKeyArrayIndexProcedure
        implements IParallelizableIndexProcedure,
        IMutableRelationIndexWriteProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = 9013449121306914750L;

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
        
        public TextIndexWriteProc newInstance(IRabaCoder keySer,
                IRabaCoder valSer,int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            return new TextIndexWriteProc(keySer, valSer, fromIndex, toIndex, keys,
                    vals, overwrite);

        }

    }
    
    /**
     * De-serialization constructor.
     */
    public TextIndexWriteProc() {
        
    }
    
    private boolean overwrite;
    
    protected TextIndexWriteProc(IRabaCoder keySer, IRabaCoder valSer,
            int fromIndex, int toIndex, byte[][] keys, byte[][] vals,
            boolean overwrite) {

        super(keySer, valSer, fromIndex, toIndex, keys, vals);
        
        assert vals != null;
        
    }

    public final boolean isReadOnly() {
        
        return false;
        
    }
    
    /**
     * @return The #of pre-existing tuples that were updated as an
     *         {@link Integer}.
     */
    public Object apply(final IIndex ndx) {

        int updateCount = 0;

        final int n = getKeyCount();

        for (int i = 0; i < n; i++) {

            final byte[] key = getKey(i);
            assert key != null;
            assert key.length > 0;

            // the value encodes the term-frequency and optional position metadata.
            final byte[] val = getValue(i);
            assert val != null;
            assert val.length > 0;

            /*
             * Write on the index if (a) overwrite was specified; or (b) the
             * index does not contain an entry for the key.
             * 
             * Note: This is an optimization which avoids mutation of the btree
             * when there would be no change in the data.
             */
            final boolean write = overwrite || !ndx.contains(key);
            
            if (write && ndx.insert(key, val) != null) {
                
                updateCount++;
                
            }

        }

        if (log.isInfoEnabled())
            log.info("wrote " + n + " tuples of which " + updateCount
                    + " were updated rows");
        
        return updateCount;
        
    }
    
    protected void readMetadata(ObjectInput in) throws IOException, ClassNotFoundException {
        
        super.readMetadata(in);
        
        overwrite = in.readBoolean();
        
    }

    /**
     * Extended to write the {@link #overwrite} flag.
     */
    protected void writeMetadata(ObjectOutput out) throws IOException {

        super.writeMetadata(out);
        
        out.writeBoolean(overwrite);
        
    }
    
}
