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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.InGraphBinarySearchFilter;
import com.bigdata.rdf.spo.InGraphHashSetFilter;

/**
 * Abstract base class for "IN" {@link IConstraint} implementations.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: INConstraint.java 4286 2011-03-09 17:36:10Z mrpersonick $
 *
 *          FIXME Reconcile this with {@link InGraphBinarySearchFilter} and
 *          {@link InGraphHashSetFilter} and also with the use of an in-memory
 *          join against the incoming binding sets to handle SPARQL data sets.
 */
abstract public class InBOp extends XSDBooleanIVValueExpression {

	private static final long serialVersionUID = -774833617971700165L;

	public interface Annotations extends BOp.Annotations {

        String NOT = (InBOp.class.getName() + ".not").intern();

    }

	private static BOp[] mergeArguments(IValueExpression<? extends IV> var,IConstant<? extends IV>...set){
	    BOp args[]=new BOp[1+(set!=null?set.length:0)];
	    args[0]=var;
	    for(int i=0;i<set.length;i++){
	        args[i+1]=set[i];
	    }
        return args;
	}

	public InBOp(boolean not,IValueExpression<? extends IV> var,IConstant<? extends IV>...set) {
	    this(mergeArguments(var,set),NV.asMap(new NV(Annotations.NOT, Boolean.valueOf(not) )));
	}

    /**
     * @param op
     */
    public InBOp(final InBOp op) {
        super(op);
    }

    /**
     * @param args
     * @param annotations
     */
    public InBOp(BOp[] args, Map<String, Object> annotations) {

        super(args, annotations);

        if (getProperty(Annotations.NOT) == null)
            throw new IllegalArgumentException();

        final IValueExpression<? extends IV> var = get(0);

        if (var == null)
            throw new IllegalArgumentException();

        if(arity()<2){
            throw new IllegalArgumentException();
        }

    }

    /**
     * @see Annotations#VARIABLE
     */
    @SuppressWarnings("unchecked")
    public IVariable<IV> getVariable() {

        return (IVariable<IV>) get(0);

    }

    /**
     * @see Annotations#SET
     */
    @SuppressWarnings("unchecked")
    public IConstant<IV>[] getSet() {
        IConstant<IV>[] set=new IConstant[arity()-1];
        for(int i=1;i<arity();i++){
            set[i-1]=(IConstant<IV>)get(i);
        }
        return set;

    }

}
