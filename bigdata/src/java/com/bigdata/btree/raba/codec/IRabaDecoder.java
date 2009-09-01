package com.bigdata.btree.raba.codec;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.io.AbstractFixedByteArrayBuffer;

/**
 * Interface for an coded logical byte[][]. Implementations of this interface
 * MUST be thread-safe since the B+Tree guarantees the ability to perform
 * concurrent read operations and instances of this interface are used to code
 * the keys and values of B+Tree nodes and leaves.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IRabaCoder
 */
public interface IRabaDecoder extends IRaba {

    /**
     * The coded (aka compressed) data.
     */
    AbstractFixedByteArrayBuffer data();

}
