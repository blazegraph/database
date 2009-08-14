package com.bigdata.btree.raba.codec;

import java.nio.ByteBuffer;

import com.bigdata.btree.raba.IRaba;

/**
 * Interface for an coded logical byte[][].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IRabaCoder
 */
public interface IRabaDecoder extends IRaba {

    /**
     * The encoded data.
     */
    ByteBuffer data();

}
