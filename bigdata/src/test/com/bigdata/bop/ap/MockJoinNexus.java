/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 19, 2010
 */

package com.bigdata.bop.ap;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.Var;
import com.bigdata.btree.keys.ISortKeyBuilder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.eval.AbstractJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Mock object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class MockJoinNexus extends AbstractJoinNexus implements IJoinNexus {

    protected MockJoinNexus(IJoinNexusFactory joinNexusFactory,
            IIndexManager indexManager) {
        super(joinNexusFactory, indexManager);
    }

    public IConstant fakeBinding(IPredicate predicate, Var var) {
        // TODO Auto-generated method stub
        return null;
    }

    public IAccessPath getTailAccessPath(IRelation relation, IPredicate pred) {
        // TODO Auto-generated method stub
        return null;
    }

    public IRelation getTailRelationView(IPredicate pred) {
        // TODO Auto-generated method stub
        return null;
    }

    public ISortKeyBuilder<IBindingSet> newBindingSetSortKeyBuilder(IRule rule) {
        // TODO Auto-generated method stub
        return null;
    }

    public IBuffer<ISolution[]> newInsertBuffer(IMutableRelation relation) {
        // TODO Auto-generated method stub
        return null;
    }

    public IChunkedOrderedIterator<ISolution> runQuery(IStep step)
            throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
