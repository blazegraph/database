package com.bigdata.bop;

/**
 * Exception thrown when a {@link BOp} appears more than once in an operator
 * tree (operator trees must not contain loops and operator instances may not
 * appear more than once unless they are an {@link IConstant} or an
 * {@link IVariable}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DuplicateBOpIdException.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class DuplicateBOpException extends RuntimeException {

    /**
     * @param msg
     */
    public DuplicateBOpException(String msg) {
        super(msg);
    }

    private static final long serialVersionUID = 1L;

}