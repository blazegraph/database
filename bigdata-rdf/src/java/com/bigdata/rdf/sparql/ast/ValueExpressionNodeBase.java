/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 17, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Iterator;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class ValueExpressionNodeBase extends GroupMemberNodeBase
        implements IValueExpressionMetadata {

    /**
     * Static helper used to determine materialization requirements.
     */
    static INeedsMaterialization.Requirement gatherVarsToMaterialize(
            final IValueExpression<?> c, final Set<IVariable<IV>> terms) {
     
        boolean materialize = false;
        boolean always = false;
        
        final Iterator<BOp> it = BOpUtility.preOrderIterator(c);
        
        while (it.hasNext()) {
            
            final BOp bop = it.next();
            
            if (bop instanceof INeedsMaterialization) {
                
                final INeedsMaterialization bop2 = (INeedsMaterialization) bop;
                
                final Set<IVariable<IV>> t = bop2.getTermsToMaterialize();
                
                if (t.size() > 0) {
                    
                    terms.addAll(t);
                    
                    materialize = true;
                    
                    // if any bops have terms that always needs materialization
                    // then mark the whole constraint as such
                    if (bop2.getRequirement() == Requirement.ALWAYS) {
                        
                        always = true;
                        
                    }
                    
                }
                
            }
            
        }

        return materialize ? (always ? Requirement.ALWAYS
                : Requirement.SOMETIMES) : Requirement.NEVER;

    }
    
}
