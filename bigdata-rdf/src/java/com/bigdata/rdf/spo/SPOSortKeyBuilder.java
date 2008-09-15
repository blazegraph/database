package com.bigdata.rdf.spo;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.ISortKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Class produces unsigned byte[] sort keys for {@link ISPO}s. This
 * implementation is NOT thread-safe.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOSortKeyBuilder implements ISortKeyBuilder<ISPO> {

    final IKeyBuilder keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG
            * IRawTripleStore.N);

    public SPOSortKeyBuilder() {

    }

    /**
     * Distinct iff the {s:p:o} are distinct.
     */
    public byte[] getSortKey(ISPO spo) {

        return keyBuilder.reset().append(spo.s()).append(spo.p()).append(
                spo.o()).getKey();

    }

}
