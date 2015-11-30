package com.bigdata.service.ndx;

import com.bigdata.btree.proc.IResultHandler;
import com.bigdata.service.Split;

/**
 * Hands back the object visited for a single index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class IdentityHandler<T> implements IResultHandler<T, T> {

    private int nvisited = 0;
    private T ret;
    
    @Override
    public void aggregate(final T result, final Split split) {

		synchronized (this) {
			
			if (nvisited != 0) {

				/*
				 * You can not use this handler if the procedure is mapped over
				 * more than one split.
				 */

				throw new UnsupportedOperationException();

			}

			this.ret = result;

			nvisited++;

		}
        
    }

    @Override
    public T getResult() {

		synchronized (this) {
			
			return ret;
			
		}
        
    }
    
}