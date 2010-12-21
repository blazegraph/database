package com.bigdata.bop;

/**
 * Exception thrown when a {@link PipelineOp.Annotations#CONDITIONAL_GROUP} is
 * not an {@link Integer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BadBOpIdTypeException.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 */
public class BadConditionalGroupIdTypeException extends RuntimeException {

    /**
     * @param msg
     */
    public BadConditionalGroupIdTypeException(String msg) {
        super(msg);
    }

    private static final long serialVersionUID = 1L;

}
