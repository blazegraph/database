package com.bigdata.service;

import com.bigdata.btree.IResultHandler;

/**
 * Hands back the object visited for a single index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
@SuppressWarnings("unused") class IdentityHandler implements IResultHandler<Object, Object> {

    int nvisited = 0;
    private Object ret;
    
    public void aggregate(Object result, Split split) {

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

    public Object getResult() {

        return ret;
        
    }
    
}