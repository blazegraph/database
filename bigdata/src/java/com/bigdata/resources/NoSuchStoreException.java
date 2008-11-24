package com.bigdata.resources;

import java.util.UUID;

/**
 * An instance of this class is thrown when a {@link UUID} does not identify
 * any store known to the {@link StoreManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NoSuchStoreException extends RuntimeException {
    
    /**
     * 
     */
    private static final long serialVersionUID = -4200720776469568430L;

    public NoSuchStoreException(UUID uuid) {
        
        super(uuid.toString());
        
    }
    
}