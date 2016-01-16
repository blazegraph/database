/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;

/**
 * Abstract base class for the <code>INSERT DATA</code> and
 * <code>DELETE DATA</code> operations.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class AbstractGraphDataUpdate extends GraphUpdate {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    interface Annotations extends GraphUpdate.Annotations {

        /**
         * The {@link BigdataStatement}[] data.
         * 
         * TODO The strong typing as a {@link BigdataStatement}[] was introduced
         * to support of the ticket below (573). This forces us to work with
         * materialized RDF Values. That makes it more difficult to optimize
         * SPARQL UPDATE by not materializing {@link IV}s as
         * {@link BigdataValue}s (ticket 522).
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/573">
         *      NullPointerException when attempting to INSERT DATA containing a
         *      blank node </a>
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/522">
         *      SPARQL UPDATE should not materialize RDF Values</a>
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

    @Override
    final public String toString(final int indent) {

        final StringBuilder sb = new StringBuilder();

        sb.append(indent(indent));
        
        sb.append(getUpdateType());

        final BigdataStatement[] data = getData();

        if (data != null) {
            
            final String s = indent(indent + 1);
            
            sb.append("\n");
            
            sb.append(s);

            int i = 0;
            
            for(ISPO spo : data) {
            
                if (i >= 10) {
                    
                    /**
                     * Truncate the description.
                     * 
                     * Note: People sometimes push a LOT of data through with a
                     * DeleteData or InsertData operation. This truncates the
                     * description to avoid problems with log files or the echo
                     * of the update operations in a request by the NSS.
                     * 
                     * @see https://sourceforge.net/apps/trac/bigdata/ticket/613
                     *      (SPARQL UPDATE response inlines large DELETE or
                     *      INSERT triple graphs)
                     * 
                     * @see https
                     *      ://sourceforge.net/projects/bigdata/forums/forum
                     *      /676946/topic/6092294/index/page/1
                     */
                    
                    sb.append("... out of " + data.length + " statements\n");

                    // Break out of the loop.
                    break;
                    
                }
                
                sb.append(spo.toString());
                
                i++;
                
            }
            
        }
        
        sb.append("\n");
        
        return sb.toString();

    }
    
}
