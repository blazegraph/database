package com.bigdata.rdf.internal;

import java.util.UUID;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Class for inline RDF blank nodes. Blank nodes MUST be based on UUIDs in
 * order to be lined.
 * <p>
 * {@inheritDoc}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id: TestEncodeDecodeKeys.java 2753 2010-05-01 16:36:59Z
 *          thompsonbry $
 * 
 * @see AbstractTripleStore.Options
 */
public class AbstractBNodeInternalValue<V extends BigdataBNode> extends
        AbstractInlineInternalValue<V, UUID> {

    /**
     * 
     */
    private static final long serialVersionUID = -4560216387427028030L;
    
    private final UUID id;
    
    public AbstractBNodeInternalValue(final UUID id) {

        super(VTE.BNODE, DTE.UUID);

        if (id == null)
            throw new IllegalArgumentException();

        this.id = id;

    }

    @Override
    public String stringValue() {
        return id.toString();
    }

    final public UUID getInlineValue() {
        return id;
    }

    final public long getTermId() {
        throw new UnsupportedOperationException();
    }

    public V asValue(BigdataValueFactory f)
            throws UnsupportedOperationException {
        // TODO asValue()
        throw new UnsupportedOperationException();
    }

    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof AbstractBNodeInternalValue<?>) {
            return this.id.equals(((AbstractBNodeInternalValue<?>) o).id);
        }
        return false;
    }

    public int hashCode() {
        return id.hashCode();
    }
 
    public int byteLength() {
        return 1 + Bytes.SIZEOF_UUID;
    }
    
}