package com.bigdata.bop;

/**
 * Exception thrown when a {@link BOp.Annotations#BOP_ID} is not an
 * {@link Integer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BadBOpIdTypeException extends RuntimeException {

    /**
     * @param msg
     */
    public BadBOpIdTypeException(String msg) {
        super(msg);
    }

    private static final long serialVersionUID = 1L;

}