package com.bigdata.text;

import com.bigdata.btree.AbstractIndexProcedureConstructor;
import com.bigdata.btree.AbstractKeyArrayIndexProcedure;
import com.bigdata.btree.IDataSerializer;
import com.bigdata.btree.IIndex;

/**
 * Writes on the text index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class IndexWriteProc extends AbstractKeyArrayIndexProcedure {

    /**
     * 
     */
    private static final long serialVersionUID = 9013449121306914750L;

    public static class IndexWriteProcConstructor extends
            AbstractIndexProcedureConstructor<IndexWriteProc> {

        public static IndexWriteProc.IndexWriteProcConstructor INSTANCE = new IndexWriteProcConstructor();

        private IndexWriteProcConstructor() {}
        
        public IndexWriteProc newInstance(IDataSerializer keySer,
                IDataSerializer valSer,int fromIndex, int toIndex,
                byte[][] keys, byte[][] vals) {

            return new IndexWriteProc(keySer,valSer,fromIndex, toIndex, keys, vals);

        }

    }
    
    /**
     * De-serialization constructor.
     */
    public IndexWriteProc() {
        
    }
    
    protected IndexWriteProc(IDataSerializer keySer, IDataSerializer valSer,
            int fromIndex, int toIndex, byte[][] keys, byte[][] vals) {

        super(keySer, valSer, fromIndex, toIndex, keys, vals);
        
        assert vals != null;
        
    }

    /**
     * @return The #of pre-existing tuples that were updated as an
     *         {@link Integer}.
     */
    public Object apply(IIndex ndx) {

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

            if(ndx.insert(key, val) != null) {
                
                updateCount++;
                
            }

        }

        log.info("wrote "+n+" tuples of which "+updateCount+" were updated rows");
        
        return updateCount;
        
    }
    
}