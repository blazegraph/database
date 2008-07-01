/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 25, 2008
 */

package com.bigdata.relation.rule;

import java.util.concurrent.ExecutorService;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.IRelationLocator;
import com.bigdata.relation.IRelationName;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MockJoinNexus implements IJoinNexus {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    public MockJoinNexus() {
        // TODO Auto-generated constructor stub
    }

    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        
    }

    public IBindingSet newBindingSet(IRule rule) {
        // TODO Auto-generated method stub
        return null;
    }

    public Object newElement(IPredicate predicate, IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        return null;
    }

    public ISolution newSolution(IRule rule, IBindingSet bindingSet) {
        // TODO Auto-generated method stub
        return null;
    }

    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {
        // TODO Auto-generated method stub
        return null;
    }

    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {
        // TODO Auto-generated method stub
        return null;
    }

    public IBlockingBuffer<ISolution> newQueryBuffer() {
        // TODO Auto-generated method stub
        return null;
    }

    public long runMutation(ActionEnum action, IProgram program) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public IChunkedOrderedIterator<ISolution> runQuery(IProgram program) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    public IRelation getRelation(IRelationName relationName) {
        // TODO Auto-generated method stub
        return null;
    }

    public ExecutorService getExecutorService() {
        // TODO Auto-generated method stub
        return null;
    }

    public IRelationLocator getRelationLocator() {
        // TODO Auto-generated method stub
        return null;
    }

    public long getTimestamp() {
        // TODO Auto-generated method stub
        return 0;
    }

}
