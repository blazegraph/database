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
 * Created on Jun 29, 2008
 */

package com.bigdata.relation.rule.eval;

import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.IStep;

/**
 * Delgation pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DelegateJoinNexus implements IJoinNexus {

    private IJoinNexus delegate;

    /**
     * 
     */
    public DelegateJoinNexus(IJoinNexus delegate) {

        if (delegate == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
    }

    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet) {
        delegate.copyValues(e, predicate, bindingSet);
    }

    public IBindingSet newBindingSet(IRule rule) {
        return delegate.newBindingSet(rule);
    }

    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {
        return delegate.newDeleteBuffer(relation);
    }

    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {
        return delegate.newInsertBuffer(relation);
    }

    public IBlockingBuffer<ISolution> newQueryBuffer() {
        return delegate.newQueryBuffer();
    }

    public ISolution newSolution(IRule rule, IBindingSet bindingSet) {
        return delegate.newSolution(rule, bindingSet);
    }

    public long runMutation(ActionEnum action, IStep step) throws Exception {
        return delegate.runMutation(action, step);
    }

    public IChunkedOrderedIterator<ISolution> runQuery(IStep step) throws Exception {
        return delegate.runQuery(step);
    }

    public IIndexManager getIndexManager() {
        return delegate.getIndexManager();
    }

    public long getWriteTimestamp() {
        return delegate.getWriteTimestamp();
    }

    public long getReadTimestamp(String relationName) {
        return delegate.getReadTimestamp(relationName);
    }

    public int solutionFlags() {
        return delegate.solutionFlags();
    }

    public IRuleTaskFactory getRuleTaskFactory(boolean parallel, IRule rule) {
        return delegate.getRuleTaskFactory(parallel, rule);
    }

    public IJoinNexusFactory getJoinNexusFactory() {
        return delegate.getJoinNexusFactory();
    }

    public IRelation getReadRelationView(IPredicate pred) {
        return delegate.getReadRelationView(pred);
    }

}
