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
/*
 * Created on March 11, 2008
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;
import com.bigdata.rdf.store.ProxyTestCase;

/**
 * @author <a href="mailto:mpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestAST extends ProxyTestCase {

	private static final Logger log = Logger.getLogger(TestAST.class);
	
    /**
     * 
     */
    public TestAST() {
        super();
    }

    /**
     * @param name
     */
    public TestAST(String name) {
        super(name);
    }
    
    @Override
    public Properties getProperties() {
    	final Properties props = super.getProperties();
//    	props.setProperty(BigdataSail.Options.INLINE_DATE_TIMES, "true");
    	return props;
    }

    /**
     * select *
     * where {
     *   predicate1 .
     *   predicate2 .
     *   filter3 .
     * }
     */
    public void testSimpleJoin() {
    	
    	final IGroupNode root = new JoinGroupNode();
    	
    	root.addChild(sp(1));
    	root.addChild(sp(2));
    	root.addChild(filter(3));
    	
    	final QueryRoot query = new QueryRoot(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   predicate1 .
     *   optional { predicate2 . }
     * }
     */
    public void testSimpleOptional() {
    	
    	final IGroupNode root = new JoinGroupNode();
    	root.addChild(sp(1));
    	
    	final IGroupNode optional = new JoinGroupNode(true);
    	optional.addChild(sp(2));
    	
    	root.addChild(optional);
    	
    	final QueryRoot query = new QueryRoot(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   predicate1 .
     *   optional { 
     *     predicate2 .
     *     predicate3 . 
     *   }
     * }
     */
    public void testOptionalJoinGroup() {
    	
    	final IGroupNode root = new JoinGroupNode();
    	root.addChild(sp(1));
    	
    	final IGroupNode optional = new JoinGroupNode(true);
    	optional.addChild(sp(2));
    	optional.addChild(sp(3));
    	
    	root.addChild(optional);
    	
    	final QueryRoot query = new QueryRoot(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   { 
     *     predicate1 .
     *     predicate2 .
     *   } union {
     *     predicate3 .
     *     predicate4 . 
     *   }
     * }
     */
    public void testUnion() {
    	
    	final IGroupNode g1 = new JoinGroupNode();
    	g1.addChild(sp(1));
    	g1.addChild(sp(2));
    	
    	final IGroupNode g2 = new JoinGroupNode();
    	g2.addChild(sp(3));
    	g2.addChild(sp(4));
    	
    	final IGroupNode union = new UnionNode();
    	union.addChild(g1);
    	union.addChild(g2);
    	
    	final QueryRoot query = new QueryRoot(union);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    /**
     * select *
     * where {
     *   predicate1 .
     *   filter2 .
     *   { 
     *     predicate3 .
     *     predicate4 .
     *   } union {
     *     predicate5 .
     *     predicate6 . 
     *   }
     * }
     */
    public void testNestedUnion() {
    	
    	final IGroupNode g1 = new JoinGroupNode();
    	g1.addChild(sp(3));
    	g1.addChild(sp(4));
    	
    	final IGroupNode g2 = new JoinGroupNode();
    	g2.addChild(sp(5));
    	g2.addChild(sp(6));
    	
    	final IGroupNode union = new UnionNode();
    	union.addChild(g1);
    	union.addChild(g2);
    	
    	final IGroupNode root = new JoinGroupNode();
    	root.addChild(sp(1));
    	root.addChild(filter(2));
    	root.addChild(union);
    	
    	final QueryRoot query = new QueryRoot(root);

    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }
    
    public void testEverything() {
    	
    	final IGroupNode root = new JoinGroupNode();
    	
    	root.addChild(sp(1)).addChild(sp(2)).addChild(filter(3));
    	
    	final JoinGroupNode j1 = new JoinGroupNode(false);
    	j1.addChild(sp(4)).addChild(sp(5)).addChild(filter(6));
    	
    	final JoinGroupNode j2 = new JoinGroupNode(false);
    	j2.addChild(sp(7)).addChild(sp(8)).addChild(filter(9));
    	
    	final UnionNode u1 = new UnionNode(false);
    	u1.addChild(j1).addChild(j2);
    	root.addChild(u1);
    	
    	final JoinGroupNode j3 = new JoinGroupNode(true);
    	j3.addChild(sp(10)).addChild(sp(11)).addChild(filter(12));
    	root.addChild(j3);
    	
    	final QueryRoot query = new QueryRoot(root);
    	
    	query.setDistinct(true);
    	query.addProjectionVar(Var.var("s"));
    	query.addProjectionVar(Var.var("p"));
    	query.setOffset(10);
    	query.setLimit(100);
    	query.addOrderBy(new OrderByNode(Var.var("s"), true));
    	query.addOrderBy(new OrderByNode(Var.var("p"), false));
    	
    	if (log.isInfoEnabled())
    		log.info("\n"+query.toString());
    	
    }

    public StatementPatternNode sp(final int id) {
    	return new StatementPatternNode(pred(id));
    }
    
    public FilterNode filter(final int id) {
    	return new FilterNode(ve(id));
    }
    
    public Predicate pred(final int id) {
    	return new Predicate(BOpBase.NOARGS, 
			NV.asMap(new NV(BOp.Annotations.BOP_ID, id))
    	);
    }
    
    public IValueExpression ve(final int id) {
    	
    	return new Filter(id);
		
    }
    
    private static final class Filter extends XSDBooleanIVValueExpression {
    	
    	public Filter(final int id) {
    		super(BOpBase.NOARGS, 
        			NV.asMap(new NV(BOp.Annotations.BOP_ID, id)));
    	}
    	
		protected boolean accept(IBindingSet bs) {
			return true;
		}
		
    }
    
}
