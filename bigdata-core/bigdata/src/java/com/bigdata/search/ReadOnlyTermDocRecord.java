package com.bigdata.search;

/**
 * Read-only {@link ITermDocRecord}.
 *  
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ReadOnlyTermDocRecord<V extends Comparable<V>>
        implements ITermDocRecord<V> {

    private final String text;
    
    private final V docId;

    private final Integer fieldId;

//    private final int termFreq;

    private final double termWeight;

    public ReadOnlyTermDocRecord(final String text, final V docId,
            final int fieldId, 
//            final int termFreq, 
            final double termWeight) {

        if (docId == null)
            throw new IllegalArgumentException();

        this.text = text; // MAY be null.
        this.docId = docId;
        this.fieldId = fieldId;
//        this.termFreq = termFreq;
        this.termWeight = termWeight;

    }

    public String toString(){

        return getClass().getName() + "{text=" + text + ", docId=" + docId
                + ", fieldId=" + fieldId /* + ", termFreq=" + termFreq */
                + ", termWeight=" + termWeight + "}";
        
    }
    
    public String getToken() {

        if (text == null)
            throw new UnsupportedOperationException();
        
        return text;
        
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

//    public int termFreq() {
//        return termFreq;
//    }
    
}