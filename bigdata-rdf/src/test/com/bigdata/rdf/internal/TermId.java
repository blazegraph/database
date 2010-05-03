package com.bigdata.rdf.internal;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueImpl;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Implementation for any kind of RDF Value when the values is not being
 * inlined. Instances of this class can represent URIs, Blank Nodes (if they are
 * not being inlined), Literals (including datatype literals if they are not
 * being inlined) or SIDs (statement identifiers).
 * <p>
 * Note: The <em>always</em> captures the datatype information in the flag bits.
 * This is a change from the historical representation of a term identifier
 * which only captured the value type information in the flag bits.
 * 
 * @todo Reconcile the lack of the value with the type information available
 *       about that value. What does this imply about
 *       {@link #asValue(BigdataValueFactory)} for {@link TermId}? Do we need a
 *       method to combine the {@link TermId} object with its value (from the
 *       ID2TERM index), obtaining a {@link BigdataValueImpl}?
 *       <p>
 *       {@link #getInlineValue()} declares a <code>Void</code> return type (and
 *       will always throw an {@link UnsupportedOperationException}) since the
 *       actual value information is not represented by this class (it could be
 *       if we change how we manage the {@link InternalDataTypeEnum}, breaking
 *       it into an (inline|termId) bit and 5 bits for the actual data type
 *       enum).
 *       <p>
 *       When an RDF Value is materialized from its term identifier, the result
 *       will be a {@link BigdataValue} which DOES NOT extend
 *       {@link AbstractInternalValue} since it will include information which
 *       is not captured inlined.
 */
public class TermId<V extends BigdataValue/* URI,BNode,Literal,SID */>
        extends AbstractInternalValue<V, Void> {

    /**
     * 
     */
    private static final long serialVersionUID = 4309045651680610931L;
    
    /** The term identifier. */
    private final long termId;

    public TermId(final InternalValueTypeEnum vte,
            final InternalDataTypeEnum dte, final long termId) {

        super(vte, dte);
        
        this.termId = termId;
        
    }

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

    final public boolean isInline() {
        return false;
    }

    final public boolean isNull() {
        return termId == IRawTripleStore.NULL;
    }

    final public boolean isTermId() {
        return true;
    }

    /**
     * Note: only the termId matters for equality (unless we also have a
     * transient reference to the value factory which stands in as a proxy for
     * the KB instance).
     */
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o instanceof TermId) {
            return termId == ((TermId) o).termId;
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
