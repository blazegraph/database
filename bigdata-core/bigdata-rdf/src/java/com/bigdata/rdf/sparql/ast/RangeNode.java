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

package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.internal.constraints.Range;
import com.bigdata.rdf.internal.constraints.RangeBOp;

/**
 * It's a value expression because it does eventually evaluate to a value -
 * a {@link Range} value.
 */
public class RangeNode extends ASTBase {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1954230684539505857L;

	public interface Annotations extends ASTBase.Annotations {
		
        /**
         * The variable whose range is restricted by the associated
         * {@link #FROM} and/or {@link #TO} filters.
         */
		String VAR = RangeNode.class.getName() + ".var";
		
		/** 
		 * The inclusive lower bound. 
		 */
		String FROM = RangeNode.class.getName() + ".from";
		
		/** 
		 * The exclusive upper bound. 
		 */
		String TO = RangeNode.class.getName() + ".to";
		
		/**
		 * The physical bop to be executed.
		 */
		String RANGE_BOP = RangeNode.class.getName() + ".bop";
		
	}
	
	/**
	 * Set the bounds later.
	 */
	public RangeNode(final VarNode var) {
		this(BOp.NOARGS, NV.asMap(
				new NV(Annotations.VAR, var)
				));
	}

	/**
	 * Set the bounds now.
	 */
	public RangeNode(final VarNode var, final IValueExpressionNode from,
			final IValueExpressionNode to) {
		this(BOp.NOARGS, NV.asMap(
				new NV(Annotations.VAR, var),
				new NV(Annotations.FROM, from),
				new NV(Annotations.TO, to)
				));
	}
	
	public RangeNode(final BOp[] args, final Map<String, Object> anns) {
		super(args, anns);
	}
	
	public RangeNode(final RangeNode bop) {
		super(bop);
	}
	
	public void setFrom(final ValueExpressionNode ve) {
		setProperty(Annotations.FROM, ve);
	}
	
	public void setTo(final ValueExpressionNode ve) {
		setProperty(Annotations.TO, ve);
	}
	
	public ValueExpressionNode from() {
		return (ValueExpressionNode) getProperty(Annotations.FROM);
	}
	
	public ValueExpressionNode to() {
		return (ValueExpressionNode) getProperty(Annotations.TO);
	}
	
	public VarNode var() {
		return (VarNode) getProperty(Annotations.VAR);
	}
	
	public RangeBOp getRangeBOp() {
		return (RangeBOp) getProperty(Annotations.RANGE_BOP);
	}
	
	public void setRangeBOp(final RangeBOp bop) {
		setProperty(Annotations.RANGE_BOP, bop);
	}

}
