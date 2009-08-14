package com.bigdata.btree.data;

import java.io.OutputStream;

import com.bigdata.btree.IAbstractNodeData;
import com.bigdata.btree.raba.IRaba;

/**
 * Abstract base class for mock node and leaf data implementations for unit
 * tests.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract class AbstractMockNodeData implements IAbstractNodeData {

    // mutable.
    final private IRaba keys;

    final public int getKeyCount() {

        return keys.size();

    }

    final public IRaba getKeys() {

        return keys;

    }

    public final byte[] getKey(final int index) {

        return keys.get(index);

    }

    final public void copyKey(final int index, final OutputStream os) {

        keys.copy(index, os);

    }

    protected AbstractMockNodeData(final IRaba keys) {

        if (keys == null)
            throw new IllegalArgumentException();

        if (!keys.isKeys())
            throw new IllegalArgumentException();

        this.keys = keys;

    }

}
