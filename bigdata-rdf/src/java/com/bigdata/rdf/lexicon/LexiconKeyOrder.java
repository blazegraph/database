package com.bigdata.rdf.lexicon;

import java.io.ObjectStreamException;
import java.util.Comparator;

import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;
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
    private static final transient int _TERMS = 0;

	/**
	 * The index whose keys are formed from the hash code of the RDF
	 * {@link Value} plus a counter (to break ties on the hash code). The
	 * {@link IV} for an entry in the TERMS index is formed by wrapping this
	 * key. The values are the RDF {@link Value}s and are often represented by
	 * raw records on the backing {@link Journal}. This index is only used for
	 * "large" {@link Value}s. Most RDF {@link Value}s wind up inlined into the
	 * statement indices.
	 */
    public static final transient LexiconKeyOrder TERMS = new LexiconKeyOrder(
            _TERMS);

    /**
     * The positional index corresponding to the {@link BigdataValue} in a
     * {@link LexPredicate}.
     */
    public final static transient int SLOT_TERM = 0;

    /**
     * The positional index corresponding to the {@link TermId} in a
     * {@link LexPredicate}.
     */
    public final static transient int SLOT_ID = 1;
    
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

        return TERMS;

    }

    /**
     * The base name for the index.
     */
    public String getIndexName() {

        return "TERMS";

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

    /**
     * {@inheritDoc}
     * <p>
     * Note: The TERMS index has a single key component. Therefore keyPos MUST
     * be ZERO (0).
     */
    final public int getKeyOrder(final int keyPos) {

        if (keyPos != 0)
            throw new IllegalArgumentException();
        
        return SLOT_ID;
        
    }

    /**
     * Operation is not supported.
     * <p>
     * Note: The TERMS index key order is defined by the {@link TermId} keys.
     * They are formed from the {@link VTE}, the hashCode (of the
     * {@link BigdataValue}), and a collision counter. The collision counter is
     * not known unless you actually scan the collision bucket in the TERMS
     * index. So there is no way to provide a comparator for the TERMS index
     * unless all of the {@link BigdataValue}s have been resolved to their
     * {@link TermId}s.
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
     * Overridden to handle the encoding of the {@link TermId} for the index.
     */
    @Override
    protected void appendKeyComponent(final IKeyBuilder keyBuilder,
            final int i, final Object keyComponent) {

        ((TermId<?>) keyComponent).encode(keyBuilder);

    }

}
