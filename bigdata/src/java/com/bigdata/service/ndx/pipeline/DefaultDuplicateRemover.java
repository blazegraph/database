package com.bigdata.service.ndx.pipeline;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.keys.KVO;

/**
 * Implementation which retains one instance of each tuple having the same
 * unsigned byte[] key and the same byte[] value. For efficiency, you may
 * specify that the presence of the same non-<code>null</code> object
 * reference may be used to detect duplicates without requiring the comparison
 * of the byte[] values.
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
     * Instance verifies the same unsigned byte[] key and the same byte[] value.,
     */
    public transient static final IDuplicateRemover KEY_VAL = new DefaultDuplicateRemover(false);
    
    /**
     * Instance verifies the same unsigned byte[] key and will accept the same
     * non-<code>null</code> object reference as indicating the same value.
     * If the object reference is <code>null</code> then it will compare the
     * byte[] values.
     */
    public transient static final IDuplicateRemover KEY_REF_VAL = new DefaultDuplicateRemover(false);
    
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

            for (int i = 0; i < nsrc; i++) {

                if (i > 0 && BytesUtil.bytesEqual(src[i].key, src[i - 1].key)) {

                    // same key
                    if (log.isTraceEnabled())
                        log.trace("duplicate key: "
                                + BytesUtil.toString(src[i].key));
                    
                    if (testRefs && src[i].obj != null
                            && src[i].obj == src[i - 1].obj) {

                        if (DEBUG)
                            log.debug("duplicate reference: " + src[i].obj);

                        // duplicate reference.
                        continue;

                    }

                    if (BytesUtil.bytesEqual(src[i - 1].val, src[i].val)) {

                        if (DEBUG)
                            log.debug("duplicate value: " + src[i].val);

                        // same value.
                        continue;

                    }

                }

                // add to the temporary array.
                tmp[ndistinct++] = src[i];

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
