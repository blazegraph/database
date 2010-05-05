package com.bigdata.rdf.internal;

import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataValue;

/**
 * Abstract base class for inline RDF values (literals, blank nodes, and
 * statement identifiers can be inlined).
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 */
abstract public class AbstractInlineInternalValue<V extends BigdataValue, T>
        extends AbstractInternalValue<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = -2847844163772097836L;

    protected AbstractInlineInternalValue(final InternalValueTypeEnum vte,
            final InternalDataTypeEnum dte) {

        super(vte, dte);

    }
    
    /**
     * Returns the String-value of a Value object. This returns either a
     * Literal's label, a URI's URI or a BNode's ID.
     * 
     * @see Value#stringValue()
     */
    abstract public String stringValue();
    
    public String toString() {
        
        return super.toString() + "[" + stringValue() + "]";
        
    }
    
}