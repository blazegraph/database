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

/**
 * Graph management operation inserts all data from one graph into another. Data
 * from the source graph is not effected. Data already present in the target
 * graph is not effected.
 * 
 * <pre>
 * ADD ( SILENT )? ( ( GRAPH )? IRIref_from | DEFAULT) TO ( ( GRAPH )? IRIref_to | DEFAULT)
 * </pre>
 * 
 * @see http://www.w3.org/TR/sparql11-update/#add
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AddGraph extends AbstractFromToGraphManagement {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public AddGraph() {

        super(UpdateType.Add);
        
    }

    /**
     * @param op
     */
    public AddGraph(final AddGraph op) {
        super(op);
    }

    /**
     * @param args
     * @param anns
     */
    public AddGraph(final BOp[] args, final Map<String, Object> anns) {
        super(args, anns);
    }

}
