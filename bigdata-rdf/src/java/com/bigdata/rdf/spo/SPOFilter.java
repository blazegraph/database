package com.bigdata.rdf.spo;

import com.bigdata.rdf.store.IAccessPath;

import cutthecrap.utils.striterators.Filter;

/**
 * An adaptor between the CTC {@link Filter} and an {@link ISPOFilter}.
 * 
 * @see IAccessPath#iterator(ISPOFilter)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOFilter extends Filter {
    
    private static final long serialVersionUID = -7225954116582658262L;
    
    final private ISPOFilter filter;
    
    SPOFilter(ISPOFilter filter) {
        
        this.filter = filter;
        
    }

    protected boolean isValid(Object arg0) {

        if(filter!=null) {
            
            SPO spo = (SPO)arg0;
            
            if(filter.isMatch(spo)) {
                
                return true;
                
            }
            
        }
        
        return false;
        
    }
    
}