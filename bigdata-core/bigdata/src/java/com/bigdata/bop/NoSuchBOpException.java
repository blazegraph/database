package com.bigdata.bop;

/**
 * Exception thrown when there is no {@link BOp} in an operator tree having the
 * desired {@link BOp.Annotations#BOP_ID}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NoSuchBOpException extends RuntimeException {

    /**
     * @param msg
     */
    public NoSuchBOpException(Integer id) {
        super("id=" + id);
    }

    private static final long serialVersionUID = 1L;

}