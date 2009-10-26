package com.bigdata.rdf.lexicon;

import java.util.concurrent.Callable;

import org.openrdf.model.BNode;
import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KVO;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.Id2TermWriteProc.Id2TermWriteProcConstructor;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.model.BigdataValueSerializer;
import com.bigdata.rdf.spo.ISPO;

/**
 * Add terms to the reverse index, which is the index that we use to lookup the
 * RDF value by its term identifier so that we can externalize {@link ISPO}s as
 * RDF/XML or the like.
 * <p>
 * Note: Every term asserted against the forward mapping [terms] MUST be
 * asserted against the reverse mapping [ids] EVERY time. This is required in
 * order to guarantee that the reverse index remains complete and consistent.
 * Otherwise a client that writes on the terms index and fails before writing on
 * the ids index would cause those terms to remain undefined in the reverse
 * index.
 */
public class ReverseIndexWriterTask implements Callable<Long> {

    private final IIndex idTermIndex;

    private final BigdataValueSerializer<BigdataValue> ser;

    private final KVO<BigdataValue>[] a;

    private final int ndistinct;

    /**
     * 
     * @param idTermIndex
     *            The index on which to write the data.
     * @param valueFactory
     *            This determines how the {@link Value} objects are serialized
     *            on the index.
     * @param a
     *            The terms (in sorted order by their term identifiers).
     * @param ndistinct
     *            The #of elements in <i>a</i>.
     */
    public ReverseIndexWriterTask(final IIndex idTermIndex,
            final BigdataValueFactoryImpl valueFactory,
            final KVO<BigdataValue>[] a, final int ndistinct) {

        if (idTermIndex == null)
            throw new IllegalArgumentException();

        if (valueFactory == null)
            throw new IllegalArgumentException();

        if (a == null)
            throw new IllegalArgumentException();

        if (ndistinct < 0 || ndistinct > a.length)
            throw new IllegalArgumentException();

        this.idTermIndex = idTermIndex;

        this.ser = valueFactory.getValueSerializer();

        this.a = a;

        this.ndistinct = ndistinct;

    }

    /**
     * @return the elapsed time for this task.
     */
    public Long call() throws Exception {

        final long _begin = System.currentTimeMillis();

        /*
         * Create a key buffer to hold the keys generated from the term
         * identifiers and then generate those keys.
         * 
         * Note: We DO NOT write BNodes on the reverse index.
         */
        final byte[][] keys = new byte[ndistinct][];
        final byte[][] vals = new byte[ndistinct][];
        int nonBNodeCount = 0; // #of non-bnodes.
        {

            // thread-local key builder removes single-threaded constraint.
            final IKeyBuilder tmp = KeyBuilder.newInstance(Bytes.SIZEOF_LONG);

            // buffer is reused for each serialized term.
            final DataOutputBuffer out = new DataOutputBuffer();

            for (int i = 0; i < ndistinct; i++) {

                final BigdataValue x = a[i].obj;

                if (x instanceof BNode) {

                    // Blank nodes are not entered into the reverse index.
                    continue;

                }

                keys[nonBNodeCount] = tmp.reset().append(x.getTermId())
                        .getKey();

                // Serialize the term.
                vals[nonBNodeCount] = ser.serialize(x, out.reset());

                nonBNodeCount++;

            }

        }

        // run the procedure on the index.
        if (nonBNodeCount > 0) {

            idTermIndex.submit(0/* fromIndex */, nonBNodeCount/* toIndex */,
                    keys, vals, Id2TermWriteProcConstructor.INSTANCE, null/* resultHandler */
            );

        }

        return System.currentTimeMillis() - _begin;

    }

}
