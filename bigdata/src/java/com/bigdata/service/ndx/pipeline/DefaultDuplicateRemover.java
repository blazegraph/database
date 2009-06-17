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

    static protected transient final Logger log = Logger
            .getLogger(DefaultDuplicateRemover.class);

    static protected transient final boolean DEBUG = log.isDebugEnabled();

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

        int ndistinct = 0;

        final int nsrc = src.length;
        
        final KVO<O>[] tmp = new KVO[nsrc];
        
        {

            KVO<O> lastDistinct = null;

            for (int i = 0; i < nsrc; i++) {

                final KVO<O> thisKVO = src[i];
                
                if (lastDistinct != null) {

                    if (BytesUtil.bytesEqual(thisKVO.key, lastDistinct.key)) {

                        // same key
                        if (log.isTraceEnabled())
                            log.trace("duplicate key: "
                                    + BytesUtil.toString(thisKVO.key));

                        if (testRefs && thisKVO.obj != null
                                && thisKVO.obj == lastDistinct.obj) {

                            // duplicate reference.

                            if (DEBUG)
                                log.debug("duplicate reference: " + thisKVO.obj);

                            if(lastDistinct instanceof KVOList) {
                                
                                // link the duplicates together.
                                ((KVOList)lastDistinct).add(thisKVO);
                                
                            }
                            
                            continue;

                        }

                        if (BytesUtil.bytesEqual(lastDistinct.val, thisKVO.val)) {

                            // same value.

                            if (DEBUG)
                                log.debug("duplicate value: " + thisKVO.val);

                            if(lastDistinct instanceof KVOList) {
                                
                                // link the duplicates together.
                                ((KVOList)lastDistinct).add(thisKVO);
                                
                            }

                            continue;

                        }

                    }

                }

                // add to the temporary array.
                tmp[ndistinct++] = lastDistinct = thisKVO;

            }

        }

        if (ndistinct == nsrc) {

            // Nothing was filtered so the array is dense and we are done.
            return tmp;

        }

        /*
         * Make the array dense (this also covers the case where the array is
         * empty).
         */
        final KVO<O>[] out = new KVO[ndistinct];

        // copy the data
        System.arraycopy(tmp/* src */, 0/* srcpos */, out/* dst */,
                0/* dstpos */, ndistinct/* len */);

        // Done.
        return out;

    }

}
