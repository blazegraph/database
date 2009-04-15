package com.bigdata.service.ndx.pipeline;

import com.bigdata.btree.keys.KVO;

/**
 * NOP implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <O>
 */
public class NOPDuplicateRemover<O> implements IDuplicateRemover<O> {

    /**
     * Returns its argument.
     * 
     * @return Its argument.
     */
    public KVO<O>[] filter(final KVO<O>[] a) {

        return a;

    }

}
