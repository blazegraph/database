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

    final private int arity;
    final private IKeyBuilder keyBuilder;

    public SPOSortKeyBuilder(final int arity) {
        assert arity == 3 || arity == 4;
        this.arity = arity;
        this.keyBuilder = new KeyBuilder(Bytes.SIZEOF_LONG * arity);
    }

    /**
     * Distinct iff the {s:p:o} are distinct.
     */
    public byte[] getSortKey(final ISPO spo) {

        keyBuilder.reset();
        
        spo.s().encode(keyBuilder);
        spo.p().encode(keyBuilder);
        spo.o().encode(keyBuilder);

        if (arity == 4) {

            spo.c().encode(keyBuilder);
            
        }

        return keyBuilder.getKey();

    }

}
