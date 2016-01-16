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
import java.util.UUID;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.ImmutableBOp;
import com.bigdata.bop.NV;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * Implements the now() operator.
 */
public class UUIDBOp extends IVValueExpression<IV> implements INeedsMaterialization{

    /**
	 *
	 */
    private static final long serialVersionUID = 9136864442064392445L;

    public interface Annotations extends ImmutableBOp.Annotations {

        String STR = UUIDBOp.class.getName() + ".str";
        
    }

    public UUIDBOp(final GlobalAnnotations globals, final boolean str) {

        this(BOp.NOARGS, anns(globals, new NV(Annotations.STR, str)));

    }

    /**
     * Required shallow copy constructor.
     *
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    public UUIDBOp(final BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     *
     * @param op
     */
    public UUIDBOp(final UUIDBOp op) {

        super(op);

    }

    final public IV get(final IBindingSet bs) {

        final BigdataValueFactory vf = super.getValueFactory();
        
        final UUID uuid = UUID.randomUUID();
        
        if (str()) {
            
            final BigdataLiteral l = vf.createLiteral(uuid.toString());
            
            return DummyConstantNode.toDummyIV(l);
            
        } else {
            
            final BigdataURI uri = vf.createURI("urn:uuid:" + uuid.toString());
            
            return DummyConstantNode.toDummyIV(uri);
            
        }
        
    }

    public boolean str() {
        
        return (boolean) getProperty(Annotations.STR);
        
    }
    
    public String toString() {

        return str() ? "struuid()" : "uuid()";

    }

    public Requirement getRequirement() {
        return Requirement.NEVER;
    }

}
