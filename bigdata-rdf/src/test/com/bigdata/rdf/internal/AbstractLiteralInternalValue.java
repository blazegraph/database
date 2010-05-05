package com.bigdata.rdf.internal;

import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Abstract base class for inline RDF literals.
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 */
abstract public class AbstractLiteralInternalValue<V extends BigdataLiteral, T>
        extends AbstractInlineInternalValue<V, T> {

    /**
     * 
     */
    private static final long serialVersionUID = -2684528247542410336L;

    protected AbstractLiteralInternalValue(final InternalDataTypeEnum dte) {

        super(InternalValueTypeEnum.LITERAL, dte);

    }

}