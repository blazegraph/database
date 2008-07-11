package com.bigdata.rdf.lexicon;

import java.io.ObjectStreamException;
import java.util.Comparator;

import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueIdComparator;
import com.bigdata.relation.accesspath.IKeyOrder;

/**
 * Natural index orders for the {@link LexiconRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LexiconKeyOrder implements IKeyOrder<BigdataValue> {

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
            _TERM2ID, "TERM2ID");

    /**
     * The index whose keys are formed from the term identifiers.
     */
    public static final transient LexiconKeyOrder ID2TERM = new LexiconKeyOrder(
            _ID2TERM, "ID2TERM");

    private final int index;

    private final String name;

    private LexiconKeyOrder(final int index, final String name) {

        this.index = index;

        this.name = name;

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
    static public LexiconKeyOrder valueOf(int index) {

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

        return name;

    }

    /**
     * Return {@link #getIndexName()}'s value.
     */
    public String toString() {

        return name;

    }

    /**
     * The integer used to represent the {@link LexiconKeyOrder} which will
     * be one of the following symbolic constants: {@link #_TERM2ID} or
     * {@link #ID2TERM}.
     */
    public int index() {

        return index;

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
