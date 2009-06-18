package com.bigdata.service.ndx.pipeline;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.KVO;

/**
 * Implementation which retains one instance of each tuple having the same
 * unsigned byte[] key and the same byte[] value. For efficiency, you may
 * specify that the presence of the same non-<code>null</code> object reference
 * may be used to detect duplicates without requiring the comparison of the
 * byte[] values.
 * <p>
 * When duplicates are eliminated, {@link KVOC}s identified as duplicates are
 * arranged into a linked list.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <O>
 *            The generic type of the object associated with the key-value pair.
 */
public class DefaultDuplicateRemover<O> implements IDuplicateRemover<O> {

//    static protected transient final Logger log = Logger
//            .getLogger(DefaultDuplicateRemover.class);

    final private boolean testRefs;

    /**
     * Instance verifies the same unsigned byte[] key and the same byte[]
     * value.,
     */
    public transient static final IDuplicateRemover KEY_VAL = new DefaultDuplicateRemover(
            false/* testRefs */);

    /**
     * Instance verifies the same unsigned byte[] key and will accept the same
     * non-<code>null</code> object reference as indicating the same value. If
     * the object reference is <code>null</code> then it will compare the byte[]
     * values.
     */
    public transient static final IDuplicateRemover KEY_REF_VAL = new DefaultDuplicateRemover(
            false/* testRefs */);
    
    /**
     * @param testRefs
     *            When <code>true</code>, {@link KVO}s having the same key
     *            and the same non-<code>null</code> object reference will be
     *            filtered without testing the byte[] values for equality.
     */
    public DefaultDuplicateRemover(final boolean testRefs) {
        
        this.testRefs = testRefs;
        
    }
    
    public KVO<O>[] filter(final KVO<O>[] src) {

        final KVO<O>[] tmp = new KVO[src.length];

        int ndistinct = 0;

        KVO<O> prior = null;

        for (KVO<O> other : src) {

            if (prior != null) {

                if (filterDuplicate(prior, other)) {
                    
                    continue;
                    
                }

            }

            tmp[ndistinct++] = prior = other;

        }

        // Make the array dense.
        return KVO.dense(tmp, ndistinct);

    }

    /**
     * Return <code>true</code> if the <i>other</i> instance is a duplicate and
     * may be dropped. (This implementation recognizes {@link KVOList} and
     * handles it appropriately.)
     * 
     * @param prior
     *            The previous {@link KVO} instance.
     * @param other
     *            Another {@link KVO} instance.
     *            
     * @return <code>true</code> if the <i>other</i> is a duplicate.
     */
    protected boolean filterDuplicate(final KVO<O> prior, final KVO<O> other) {

        // same key?
        if (BytesUtil.bytesEqual(prior.key, other.key)) {

            // same reference (if ref testing) or same value?
            if ((testRefs && prior.obj != null && prior.obj == other.obj)
                    || BytesUtil.bytesEqual(prior.val, other.val)) {

                if (prior instanceof KVOList) {

                    // link the duplicates together.
                    ((KVOList) prior).add(other);

                }

                return true;

            }

        }

        return false;

    }

}
