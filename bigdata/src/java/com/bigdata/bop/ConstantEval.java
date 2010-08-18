package com.bigdata.bop;

/**
 * Evaluate a constant
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @param <E>
 */
public interface ConstantEval<E> extends BOp {

    /**
     * Evaluate a constant.
     * 
     * @return The value.
     */
    E eval();

}