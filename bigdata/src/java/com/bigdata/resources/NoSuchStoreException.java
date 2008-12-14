package com.bigdata.resources;

import java.util.UUID;

import com.bigdata.journal.ITx;

/**
 * An instance of this class is thrown when a {@link UUID} does not identify any
 * store known to the {@link StoreManager}. This can arise if the store file is
 * deleted by an operator, but the most common explanation is that resource was
 * considered "release free" and was deleted by the {@link StoreManager}.
 * <p>
 * Full transactions both hold read locks which prevent a store from being
 * released while the transaction is active. Those read locks are managed by the
 * transaction manager, which advances the release time based on the earliest
 * required by any active transaction.
 * <p>
 * An {@link ITx#UNISOLATED} operation uses the live view of an index, and the
 * resources for the live view may not be released. Likewise, read-committed
 * operations update a local release time which provides a read-lock solely for
 * the duration of the read-committed operation.
 * <p>
 * Non-transactional read-historical operations DO NOT hold read locks and MAY
 * encounter this exception. You may simply retry the read with a more recent
 * timestamp. However, DO NOT attempt to manage the release time directly.
 * Instead, you may use a read-only transaction which will manage the read locks
 * automatically on your behalf.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NoSuchStoreException extends RuntimeException {
    
    /**
     * 
     */
    private static final long serialVersionUID = -4200720776469568430L;

    public NoSuchStoreException(String msg) {
        
        super(msg);
        
    }
 
    public NoSuchStoreException(UUID uuid) {
        
        this(uuid.toString());
        
    }
    
}