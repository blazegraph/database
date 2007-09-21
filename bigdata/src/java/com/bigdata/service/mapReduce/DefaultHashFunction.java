package com.bigdata.service.mapReduce;

import java.util.Arrays;

/**
 * This is default implementation of {@link IHashFunction} - it is based on
 * {@link Arrays#hashCode(byte[])}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultHashFunction implements IHashFunction {

    public static transient final IHashFunction INSTANCE = new DefaultHashFunction();

    private DefaultHashFunction() {

    }

    public int hashCode(byte[] key) {

        return Arrays.hashCode(key);

    }

}