package com.bigdata.isolation;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;

/**
 * Does not resolve any conflicts.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
final public class NoConflictResolver implements IConflictResolver {

    /**
     * 
     */
    private static final long serialVersionUID = 4873027180161852127L;

    public boolean resolveConflict(IIndex writeSet, ITuple txTuple,
            ITuple currentTuple) throws Exception {

        return false;

    }

}