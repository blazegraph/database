package com.bigdata.rdf.internal;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Implementation for any kind of RDF Value when the values is not being
 * inlined. Instances of this class can represent URIs, Blank Nodes (if they are
 * not being inlined), Literals (including datatype literals if they are not
 * being inlined) or SIDs (statement identifiers).
 */
public class TermId<V extends BigdataValue/* URI,BNode,Literal,SID */>
        extends AbstractInternalValue<V, Void> {

    /**
     * 
     */
    private static final long serialVersionUID = 4309045651680610931L;
    
    /** The term identifier. */
    private final long termId;

//    /** The datatype term identifier. */
//    private final long dataTypeId;

    /**
     * Constructor for a term identifier when you are decoding and already have
     * the flags.
     * 
     * @param flags
     *            The flags
     * @param termId
     */
    public TermId(final byte flags, final long termId) {

        super(flags);

        this.termId = termId;
        
//        this.dataTypeId = 0L;
        
    }

    /**
     * Constructor for a term identifier.
     * @param vte
     * @param termId
     */
    public TermId(final VTE vte, final long termId) {

        /*
         * Note: XSDBoolean happens to be assigned the code value of 0, which is
         * the value we we want when the data type enumeration will be ignored.
         */
        super(vte, false/* inline */, false/* extension */, DTE.XSDBoolean);

        this.termId = termId;
        
//        this.dataTypeId = 0L;
        
    }

//    /**
//     * Constructor for a term identifier for a datatype literal. Do NOT use this
//     * constructor when the lexicon is configured such that the datatype literal
//     * should be inlined.
//     * 
//     * @param vte
//     * @param dte
//     * @param termId
//     * @param dataTypeId
//     */
//    public TermId(final VTE vte, final DTE dte, final long termId,
//            final long dataTypeId) {
//
//        super(vte, false/* inline */, true/* extension */, dte);
//
//        if (dataTypeId == IRawTripleStore.NULL)
//            throw new IllegalArgumentException();
//        
//        this.termId = termId;
//        
//        this.dataTypeId = dataTypeId;
//
//    }

    /**
     * Human readable representation includes the term identifier, whether
     * this is a URI, Literal, Blank node, or Statement identifier and the
     * datatype URI if one is assigned. This representation is based solely
     * on the flag bits and the term identifier.
     */
    public String toString() {

        final String datatype = getInternalDataTypeEnum().getDatatype();

        return "TermId(" + termId
                + getInternalValueTypeEnum().getCharCode() + ")"
                + (datatype == null ? "" : datatype);

    }

    final public V asValue(BigdataValueFactory f)
            throws UnsupportedOperationException {
        // TODO asValue()
        throw new UnsupportedOperationException();
    }

    final public Void getInlineValue() {
        throw new UnsupportedOperationException();
    }

    final public long getTermId() {
        return termId;
    }
    
//    /**
//     * Return the term identifier for the datatype associated with the term.
//     */
//    final public long getDataTypeID() {
//        return dataTypeId;
//    }

    /**
     * Always returns <code>false</code> since the RDF value is not inline.
     */
    @Override
    final public boolean isInline() {
        return false;
    }

    /**
     * Always returns <code>true</code> since this is a term identifier.
     */
    @Override
    final public boolean isTermId() {
        return true;
    }

    /**
     * Return <code>true</code> iff the term identifier is
     * {@link IRawTripleStore#NULL}.
     */
    final public boolean isNull() {
        return termId == IRawTripleStore.NULL;
    }

    /**
     * Note: only the termId matters for equality (unless we also have a
     * transient reference to the value factory which stands in as a proxy for
     * the KB instance).
     */
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof TermId<?>) {
            return termId == ((TermId<?>) o).termId;
        }
        return false;
    }

    /**
     * Return the hash code of the long term identifier.
     * 
     * @see Long#hashCode()
     */
    public int hashCode() {
        return (int) (termId ^ (termId >>> 32));
    }
    
}
