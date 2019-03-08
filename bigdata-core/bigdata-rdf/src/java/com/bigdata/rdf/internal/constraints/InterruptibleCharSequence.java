package com.bigdata.rdf.internal.constraints;
/**
 * CharSequence that noticed thread interrupts -- as might be necessary
 * to recover from a loose regex on unexpected challenging input.
 * From: https://stackoverflow.com/a/910798/214196
 * @author gojomo
 */
public class InterruptibleCharSequence implements CharSequence {
    /**
     * Parent sequence.
     */
    private final CharSequence inner;

    public InterruptibleCharSequence(CharSequence inner) {
        this.inner = inner;
    }

    @Override
    public char charAt(int index) {
        if (Thread.interrupted()) { // clears flag if set
            throw new RuntimeException(new InterruptedException());
        }
        return inner.charAt(index);
    }

    @Override
    public int length() {
        return inner.length();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return new InterruptibleCharSequence(inner.subSequence(start, end));
    }

    @Override
    public String toString() {
        return inner.toString();
    }
}
