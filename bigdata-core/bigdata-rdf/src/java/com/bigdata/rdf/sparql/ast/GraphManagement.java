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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IVariable;

/**
 * A Graph Management operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class GraphManagement extends Update {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param updateType
     */
    public GraphManagement(final UpdateType updateType) {
        
        super(updateType);
        
    }

    /**
     * @param op
     */
    public GraphManagement(final GraphManagement op) {
        
        super(op);
        
    }

    /**
     * @param args
     * @param anns
     */
    public GraphManagement(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);
        
    }

    @Override
    final public boolean isSilent() {

        return getProperty(Annotations.SILENT, Annotations.DEFAULT_SILENT);

    }

    @Override
    final public void setSilent(final boolean silent) {

        setProperty(Annotations.SILENT, silent);

    }

    @Override
    public Set<IVariable<?>> getRequiredBound(StaticAnalysis sa) {
       return new HashSet<IVariable<?>>();
    }

    @Override
    public Set<IVariable<?>> getDesiredBound(StaticAnalysis sa) {
       return new HashSet<IVariable<?>>();
    }

}
