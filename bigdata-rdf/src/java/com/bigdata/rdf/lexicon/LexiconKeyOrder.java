package com.bigdata.rdf.lexicon;

import java.io.ObjectStreamException;
import java.util.Comparator;

import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.striterator.AbstractKeyOrder;

/**
 * Natural index orders for the {@link LexiconRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LexiconKeyOrder extends AbstractKeyOrder<BigdataValue> {

    /*
     * Note: these constants make it possible to use switch(index())
     * constructs.
     */
    private static final transient int _TERM2ID = 0;
    private static final transient int _ID2TERM = 1;
    private static final transient int _BLOBS = 2; // was TERMS

    /**
     * Keys are RDF {@link Value}s.  Values are {@link TermId}s.  Note that the
     * keys of this index CAN NOT be decoded (they are unicode sort keys).
     */
    public static final transient LexiconKeyOrder TERM2ID = new LexiconKeyOrder(
            _TERM2ID);
    
    /**
     * Keys are {@link TermId}s.  Values are RDF {@link Value}s (which MAY be
     * stored on raw records, which is transparent to the user if it occurs).
     */
    public static final transient LexiconKeyOrder ID2TERM = new LexiconKeyOrder(
            _ID2TERM);

    /**
     * Keys are {@link BlobIV}s. Values are RDF {@link Value}s which are
     * typically stored on raw records.
     * <p>
     * The index whose keys are formed from the hash code of the RDF
     * {@link Value} plus a counter (to break ties on the hash code). The
     * {@link IV} for an entry in the TERMS index is formed by wrapping this
     * key. The values are the RDF {@link Value}s and are often represented by
     * raw records on the backing {@link Journal}. This index is only used for
     * "large" {@link Value}s. Most RDF {@link Value}s wind up inlined into the
     * statement indices.
     */
    public static final transient LexiconKeyOrder BLOBS = new LexiconKeyOrder(
            _BLOBS);

    /**
     * The positional index corresponding to the {@link BigdataValue} in a
     * {@link LexPredicate}.
     */
    public final static transient int SLOT_TERM = 0;

    /**
     * The positional index corresponding to the {@link IV} in a
     * {@link LexPredicate}.
     */
    public final static transient int SLOT_IV = 1;
    
    /**
     * This is the only piece of serializable state for this class.
     */
    private final int index;

    private LexiconKeyOrder(final int index) {

        this.index = index;

    }

    /**
     * Returns the singleton corresponding to the <i>index</i>.
     * 
     * @param index
     *            The index.
     * 
     * @return The singleton {@link LexiconKeyOrder} having that <i>index</i>.
     * 
     * @throws IllegalArgumentException
     *             if the <i>index</i> is not valid.
     */
    static public LexiconKeyOrder valueOf(final int index) {

        switch (index) {
        case _TERM2ID:
            return TERM2ID;
        case _ID2TERM:
            return ID2TERM;
        case _BLOBS:
            return BLOBS;
        default:
            throw new AssertionError();
        }

    }

    /**
     * The base name for the index.
     */
    public String getIndexName() {

        switch (index) {
        case _TERM2ID:
            return "TERM2ID";
        case _ID2TERM:
            return "ID2TERM";
        case _BLOBS:
            return "BLOBS";
        default:
            throw new AssertionError();
        }

    }

    /**
     * Return {@link #getIndexName()}'s value.
     */
    public String toString() {

        return getIndexName();

    }

    /**
     * The integer used to represent the {@link LexiconKeyOrder}.
     */
    public int index() {

        return index;

    }

    final public int getKeyArity() {
     
        return 1;
        
    }

    final public int getKeyOrder(final int keyPos) {

        if (keyPos != 0) {
            /*
             * Note: All lexicon indices have a single key component (either the
             * RDF {@link Value} or the {@link IV}). Therefore keyPos MUST be
             * ZERO (0).
             */
            throw new IllegalArgumentException();
        }

        switch (index) {
        case _TERM2ID:
            // Key is the RDF Value (can not be decoded).
            return SLOT_TERM;
        case _ID2TERM:
            // Key is the IV.
            return SLOT_IV;
        case _BLOBS:
            // Key is the IV.
            return SLOT_IV;
        default:
            throw new AssertionError();
        }
        
    }

    /**
     * Operation is not supported.
     * <p>
     * Note: The TERMS index key order is defined by the {@link BlobIV} keys.
     * They are formed from the {@link VTE}, the hashCode (of the
     * {@link BigdataValue}), and a collision counter. The collision counter is
     * not known unless you actually scan the collision bucket in the TERMS
     * index. So there is no way to provide a comparator for the TERMS index
     * unless all of the {@link BigdataValue}s have been resolved to their
     * {@link BlobIV}s.
     * 
     * @throws UnsupportedOperationException
     */
    final public Comparator<BigdataValue> getComparator() {

        throw new UnsupportedOperationException();

    }

    /**
     * Imposes the canonicalizing mapping during object de-serialization.
     */
    private Object readResolve() throws ObjectStreamException {

        return LexiconKeyOrder.valueOf(index);

    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to handle the encoding of the {@link IV} for the index.
     */
    @Override
    protected void appendKeyComponent(final IKeyBuilder keyBuilder,
            final int i, final Object keyComponent) {

        ((IV<?,?>) keyComponent).encode(keyBuilder);

    }

}
