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
import com.bigdata.rdf.model.BigdataStatement;

/**
 * The DELETE DATA operation removes some triples, given inline in the request,
 * if the respective graphs in the Graph Store contain those:
 * 
 * <pre>
 * DELETE DATA  QuadData
 * </pre>
 * 
 * @see http://www.w3.org/TR/sparql11-update/#deleteData
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DeleteData extends AbstractGraphDataUpdate {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public DeleteData() {
        super(UpdateType.DeleteData);
    }

    /**
     * @param op
     */
    public DeleteData(DeleteData op) {
        super(op);
    }

    /**
     * @param args
     * @param anns
     */
    public DeleteData(BOp[] args, Map<String, Object> anns) {
        super(args, anns);
    }

    @Override
    public void setData(final BigdataStatement[] data) {

      setProperty(Annotations.DATA, data);

   }
    
}
