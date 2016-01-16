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
 * Created on Mar 9, 2012
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;

/**
 * An object which mocks the statistics for a single empty solution set. This is
 * used to make some of the unit tests happy which were written before the
 * {@link ISolutionSetStats} interface was introduced.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptySolutionSetStats implements ISolutionSetStats {

    public static final ISolutionSetStats INSTANCE = new EmptySolutionSetStats();
    
    private EmptySolutionSetStats() {

    }
    
    @Override
    public long getSolutionSetSize() {
        return 1;
    }

    @Override
    public Set<IVariable<?>> getUsedVars() {
        return Collections.emptySet();
    }

    @Override
    public Set<IVariable<?>> getAlwaysBound() {
        return Collections.emptySet();
    }

    @Override
    public Set<IVariable<?>> getNotAlwaysBound() {
        return Collections.emptySet();
    }

    @Override
    public Set<IVariable<?>> getMaterialized() {
        return Collections.emptySet();
    }

    @Override
    public Map<IVariable<?>, IConstant<?>> getConstants() {
        return Collections.emptyMap();
    }

}
