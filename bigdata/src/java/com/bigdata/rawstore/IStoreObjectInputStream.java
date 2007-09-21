package com.bigdata.rawstore;

import java.io.DataInput;
import java.io.ObjectInput;

/**
 * Interface exposes the {@link IRawStore} during de-serialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStoreObjectInputStream extends DataInput, ObjectInput {
    
    public IRawStore getStore();
    
}