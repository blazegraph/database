package com.bigdata.btree;


/**
 * A read-only view of an {@link ICounter}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadOnlyCounter implements ICounter {

    private ICounter src;

    public ReadOnlyCounter(ICounter src) {

        assert src != null;

        this.src = src;

    }

    public long get() {

        return src.get();

    }

    public long inc() {

        throw new UnsupportedOperationException();

    }

}