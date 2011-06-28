package com.bigdata.bop;

/**
 * Exception thrown when a {@link BOp.Annotations#BOP_ID} appears more than
 * once in an operator tree with the same value (the bop identifiers must be
 * distinct).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public class DuplicateBOpIdException extends RuntimeException {

    /**
     * @param msg
     */
    public DuplicateBOpIdException(String msg) {
        super(msg);
    }

    private static final long serialVersionUID = 1L;

}