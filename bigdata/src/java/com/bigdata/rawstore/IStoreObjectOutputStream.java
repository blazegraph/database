package com.bigdata.rawstore;

import java.io.DataOutput;
import java.io.ObjectOutput;

/**
 * Interface exposes the {@link IRawStore} during serialization.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IStoreObjectOutputStream extends DataOutput, ObjectOutput {

    public IRawStore getStore();

}