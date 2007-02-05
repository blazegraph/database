package com.bigdata.journal;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;


/**
 * An interface implemented by a persistence capable data structure such as
 * a btree so that it can participate in the commit protocol for the store.
 * <p>
 * This interface is invoked by {@link #commit()} for each registered
 * committer. The {@link Addr} returned by {@link #commit()} will be saved
 * on the root block in the slot identified to the committer when it
 * registered itself.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IRawStore#registerCommitter(int,
 *      com.bigdata.objndx.IRawStore.ICommitter)
 */
public interface ICommitter {

    /**
     * Flush all dirty records to disk in preparation for an atomic commit.
     * 
     * @return The {@link Addr address} of the record from which the
     *         persistence capable data structure can load itself. If no
     *         changes have been made then the previous address should be
     *         returned as it is still valid.
     */
    public long handleCommit();

}
