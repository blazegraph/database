package com.bigdata.rdf.lexicon;

import java.io.ObjectStreamException;
import java.util.Comparator;

import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueIdComparator;
import com.bigdata.striterator.AbstractKeyOrder;
import com.bigdata.striterator.IKeyOrder;

/**
 * Natural index orders for the {@link LexiconRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo define a BigdataValuePredicate that interoperates with this class to
 *       support joins against the lexicon.
 */
public class LexiconKeyOrder extends AbstractKeyOrder<BigdataValue> {

    /*
     * Note: these constants make it possible to use switch(index())
     * constructs.
     */
    public static final transient int _TERM2ID = 0;

    public static final transient int _ID2TERM = 1;

    /**
     * The index whose keys are formed from the terms (RDF {@link Value}s).
     */
    public static final transient LexiconKeyOrder TERM2ID = new LexiconKeyOrder(
            _TERM2ID);

    /**
     * The index whose keys are formed from the term identifiers.
     */
    public static final transient LexiconKeyOrder ID2TERM = new LexiconKeyOrder(
            _ID2TERM);

    /**
     * The positional index corresponding to the RDF Value in a
     * "lexicon predicate".
     */
    private final static transient int SLOT_TERM = 0;

    /**
     * The positional index corresponding to the term identifier in a
     * "lexicon predicate".
     */
    private final static transient int SLOT_ID = 1;
    
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
        default:
            throw new IllegalArgumentException("Unknown: index" + index);
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
     * The integer used to represent the {@link LexiconKeyOrder} which will
     * be one of the following symbolic constants: {@link #_TERM2ID} or
     * {@link #ID2TERM}.
     */
    public int index() {

        return index;

    }

    final public int getKeyArity() {
        switch(index) {
        case _TERM2ID: return 1;
        case _ID2TERM: return 1;
        default: throw new AssertionError();
        }
    }

    /*
     * Note: The TERM2ID and ID2TERM indices each have a single component to the
     * key. Therefore keyPos MUST be ZERO (0).
     */
    final public int getKeyOrder(final int keyPos) {

        if (keyPos != 0)
            throw new IllegalArgumentException();
        
        switch(index) {
        case _TERM2ID: return SLOT_TERM;
        case _ID2TERM: return SLOT_ID;
        default: throw new AssertionError();
        }
        
    }
    
    /**
     * Return the comparator that places {@link BigdataValue}s into the
     * natural order for the associated index.
     */
    final public Comparator<BigdataValue> getComparator() {

        switch (index) {
        case _TERM2ID:
            /*
             * FIXME Must impose unsigned byte[] comparison. That is
             * expensive if we have to dynamically serialize the keys for
             * each compare. The _Value object caches those serializations
             * for efficiency and the BigdataValue object probably needs to
             * do the same.
             */
            throw new UnsupportedOperationException();
        case _ID2TERM:
            return BigdataValueIdComparator.INSTANCE;
        default:
            throw new IllegalArgumentException("Unknown: " + this);
        }

    }

    /**
     * Imposes the canonicalizing mapping during object de-serialization.
     */
    private Object readResolve() throws ObjectStreamException {

        return LexiconKeyOrder.valueOf(index);

    }

}
