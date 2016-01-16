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
package com.bigdata.rdf.internal.constraints;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Implements the now() operator.
 */
public class NowBOp extends IVValueExpression<IV> implements INeedsMaterialization{

    /**
	 *
	 */
    private static final long serialVersionUID = 9136864442064392445L;

    public interface Annotations extends ImmutableBOp.Annotations {

    }

    public NowBOp(final GlobalAnnotations globals) {

        this(BOp.NOARGS, anns(globals));

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    public NowBOp(final BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     *
     * @param op
     */
    public NowBOp(final NowBOp op) {

        super(op);

    }

    final public IV get(final IBindingSet bs) {

        final Calendar cal = Calendar.getInstance();
        final Date now = cal.getTime();
        final GregorianCalendar c = new GregorianCalendar();
        c.setTime(now);
        try {
            final XMLGregorianCalendar date = 
                    DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
            return super.asIV(getValueFactory().createLiteral(date), bs);
        } catch (DatatypeConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {

        return "now()";

    }

    public Requirement getRequirement() {
        return Requirement.NEVER;
    }

}
