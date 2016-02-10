/*

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
package com.bigdata.rdf.internal.constraints;

import java.util.Map;

import org.openrdf.model.Literal;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Return the language tag of the literal argument.
 */
@SuppressWarnings("rawtypes")
public class LangBOp extends IVValueExpression<IV> 
		implements INeedsMaterialization {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7391999162162545704L;
	
//	private static final transient Logger log = Logger.getLogger(LangBOp.class);

    public LangBOp(final IValueExpression<? extends IV> x,
            final GlobalAnnotations globals) {

        super(x, globals);

    }
    
    /**
     * Required shallow copy constructor.
     */
    public LangBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null)
            throw new IllegalArgumentException();

        if (getProperty(Annotations.NAMESPACE) == null)
            throw new IllegalArgumentException();

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public LangBOp(final LangBOp op) {
     
        super(op);
        
    }

    @Override
    public IV get(final IBindingSet bs) {

        final Literal literal = getAndCheckLiteralValue(0, bs);

        String langTag = literal.getLanguage();

        if (langTag == null) {

            langTag = "";

        }

        final BigdataValueFactory vf = getValueFactory();

        final BigdataValue lang = vf.createLiteral(langTag);

        return super.asIV(lang, bs);

    }

    @Override
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.SOMETIMES;
        
    }
    
}
