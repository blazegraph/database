/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.iris;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.ISolutionExpander;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;

/**
 * A predicate that is a triple with one or more variables. While the general
 * case allows a predicate to have an arbitrary name, for RDFS reasoning we are
 * only concerned with predicates of the form <code>triple(s,p,o)</code>.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MagicPredicate implements IPredicate<IMagicTuple> {

    /**
     * Serialization version. 
     */
    private static final long serialVersionUID = -8818264629625242326L;

    
    public MagicPredicate(String symbol, IVariableOrConstant<Long>[] terms) {
        
    }
    
    public int arity() {
        // TODO Auto-generated method stub
        return 0;
    }

    public IPredicate<IMagicTuple> asBound(IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        return null;
    }

    public IVariableOrConstant get(int index) {
        // TODO Auto-generated method stub
        return null;
    }

    public IElementFilter<IMagicTuple> getConstraint() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getOnlyRelationName() {
        // TODO Auto-generated method stub
        return null;
    }

    public int getPartitionId() {
        // TODO Auto-generated method stub
        return 0;
    }

    public int getRelationCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getRelationName(int index) {
        // TODO Auto-generated method stub
        return null;
    }

    public ISolutionExpander<IMagicTuple> getSolutionExpander() {
        // TODO Auto-generated method stub
        return null;
    }

    public int getVariableCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    public boolean isFullyBound() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isOptional() {
        // TODO Auto-generated method stub
        return false;
    }

    public IPredicate<IMagicTuple> setPartitionId(int partitionId) {
        // TODO Auto-generated method stub
        return null;
    }

    public IPredicate<IMagicTuple> setRelationName(String[] relationName) {
        // TODO Auto-generated method stub
        return null;
    }

    public String toString(IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        return null;
    }

}
