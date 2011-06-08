package com.bigdata.rdf.lexicon;

import java.io.ObjectStreamException;
import java.util.Comparator;

import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueIdComparator;
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
	/** @deprecated by {@link #_TERMS}. */
    public static final transient int _TERM2ID = 0;

	/** @deprecated by {@link #_TERMS}. */
    public static final transient int _ID2TERM = 1;

    public static final transient int _TERMS = 2;

    /**
     * The index whose keys are formed from the terms (RDF {@link Value}s).
     * 
     * @deprecated by {@link #TERMS}.
     */
    public static final transient LexiconKeyOrder TERM2ID = new LexiconKeyOrder(
            _TERM2ID);

    /**
     * The index whose keys are formed from the term identifiers.
     * 
     * @deprecated by {@link #TERMS}.
     */
    public static final transient LexiconKeyOrder ID2TERM = new LexiconKeyOrder(
            _ID2TERM);

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
        case _TERMS:
            return TERMS;
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
        case _TERMS:
            return "TERMS";
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
        case _TERMS: return 1;
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
        case _TERMS: return SLOT_ID;
        default: throw new AssertionError();
        }
        
    }
    
    /**
     * Return the comparator that places {@link BigdataValue}s into the
     * natural order for the associated index.
     */
    final public Comparator<BigdataValue> getComparator() {

        switch (index) {
        case _TERMS:
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
    
    protected void appendKeyComponent(final IKeyBuilder keyBuilder,
            final int i, final Object keyComponent) {

        if (index == _TERM2ID) {
        	
        	final BigdataValue term = (BigdataValue) keyComponent;
        	final LexiconKeyBuilder lexKeyBuilder = 
        		new LexiconKeyBuilder(keyBuilder);
        	lexKeyBuilder.value2Key(term);
        	
        } else if (index == _ID2TERM) {
        	
        	final TermId id = (TermId) keyComponent;
        	id.encode(keyBuilder);
        	
        } else {
        	throw new AssertionError();
        }

    }

}
