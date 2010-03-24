package com.bigdata.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.apache.log4j.Logger;

import com.bigdata.rawstore.Bytes;

/**
 * Imposes constraint that the key before the separatorKey must differ in
 * the first N bytes from the key after the separator key.
 */
public class FixedLengthPrefixSplits implements
        ISimpleSplitHandler, Serializable, Externalizable {
    
    protected static transient final Logger log = Logger
            .getLogger(FixedLengthPrefixSplits.class);

    /**
     * 
     */
    private static final long serialVersionUID = -4873807205429701805L;
    
    private int N;

    public FixedLengthPrefixSplits(final int nbytes) {

        if (nbytes <= 0)
            throw new IllegalArgumentException();

        this.N = nbytes;
        
    }

    /**
     * Linear search for the first successor of the keyAt(splitAt) which
     * differs in the first N bytes.
     * 
     * @todo This will be faster using an {@link ITupleCursor} if we have to
     *       leaf the current leaf.
     * 
     * @todo We could also search backward or use a forward/backward search,
     *       or a progressive search, but there should seldom be any reason
     *       to do so.
     * 
     * @todo This could also be done without materializing the keys we are
     *       testing if it was written at a much lower level.
     */
    public byte[] getSeparatorKey(final IndexSegment seg,
            final int fromIndex, final int toIndex, final int splitAt) {

        final int N = this.N;

        final byte[] a = seg.keyAt(splitAt);

        for (int i = splitAt + 1; i < toIndex; i++) {

            final byte[] b = seg.keyAt(i);

            /*
             * Compare the first N bytes of those keys (unsigned byte[]
             * comparison).
             */
            final int cmp = BytesUtil.compareBytesWithLenAndOffset(//
                    0/* aoff */, Bytes.SIZEOF_LONG/* alen */, a,//
                    0/* boff */, Bytes.SIZEOF_LONG/* blen */, b//
                    );

            // the keys must be correctly ordered.
            assert cmp <= 0;

            if (cmp < 0) {

                /*
                 * The N byte prefix has changed. Clone the first N bytes of the
                 * current key and return them to the caller. This is the
                 * minimum length first successor of the recommended key which
                 * can serve as a separator key for an N byte prefix constraint.
                 */

                final byte[] prefix = new byte[N];

                System.arraycopy(b/* src */, 0/* srcPos */,
                        prefix/* dest */, 0/* destPos */, N/* length */);

                if (log.isInfoEnabled())
                    log.info("Found: prefix=" + BytesUtil.toString(prefix)
                            + ", splitAt=" + splitAt + ", i=" + i);
                
                return prefix;

            }
            
        }

        log.warn("No successor: nbytes=" + N + ", splitAt=" + splitAt);

        // No such successor!
        return null;
        
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        N = in.readInt();
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        
        out.writeInt(N);
        
    }

}
