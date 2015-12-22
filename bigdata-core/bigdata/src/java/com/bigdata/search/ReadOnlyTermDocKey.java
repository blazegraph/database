package com.bigdata.search;

/**
 * Read-only {@link ITermDocKey}.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ReadOnlyTermDocKey<V extends Comparable<V>>
        implements ITermDocKey<V> {

    private final V docId;
    
    private final Integer fieldId;

    private final double termWeight;

    public ReadOnlyTermDocKey(final V docId, final int fieldId, final double termWeight) {

        if (docId == null)
            throw new IllegalArgumentException();

        this.docId = docId;
        
        this.fieldId = fieldId;

        this.termWeight = termWeight;

    }

    public String getToken() {
        throw new UnsupportedOperationException();
    }

    public V getDocId() {
        return docId;
    }
    
    public int getFieldId() throws UnsupportedOperationException {

        if (fieldId == Integer.MIN_VALUE)
            throw new UnsupportedOperationException();

        return fieldId;

    }
    
    public double getLocalTermWeight() {
    	return termWeight;
    }

}