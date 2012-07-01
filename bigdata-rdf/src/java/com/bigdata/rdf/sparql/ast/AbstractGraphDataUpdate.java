/**

Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Mar 10, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.spo.ISPO;

/**
 * Abstract base class for the <code>INSERT DATA</code> and
 * <code>DELETE DATA</code> operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractGraphDataUpdate extends GraphUpdate {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends GraphUpdate.Annotations {

        /**
         * The {@link BigdataStatement}[] data.
         */
        String DATA = "data";

    }

    public AbstractGraphDataUpdate(final UpdateType updateType) {
        
        super(updateType);
        
    }

    /**
     * @param op
     */
    public AbstractGraphDataUpdate(final AbstractGraphDataUpdate op) {

        super(op);
        
    }

    /**
     * @param args
     * @param anns
     */
    public AbstractGraphDataUpdate(final BOp[] args,
            final Map<String, Object> anns) {

        super(args, anns);
        
    }

    public BigdataStatement[] getData() {

        return (BigdataStatement[]) getProperty(Annotations.DATA);

    }

    public void setData(final BigdataStatement[] data) {

        setProperty(Annotations.DATA, data);

    }

    final public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        final BigdataStatement[] data = getData();

        if (data != null) {
            
            final String s = indent(indent + 1);
            
            sb.append("\n");
            
            sb.append(s);
            
            for(ISPO spo : data) {
            
                sb.append(spo.toString());
                
            }
            
        }
        
        sb.append("\n");
        
        return sb.toString();

    }
    
}
