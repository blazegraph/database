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
package com.bigdata.bop.rdf.aggregate;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.AbstractLiteralBOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization.Requirement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.rdf.sparql.ast.DummyConstantNode;

/**
 * Operator combines the string values over the presented binding sets for the
 * given variable. Missing values are ignored. The initial value is an empty
 * plain literal.
 *
 * @author thompsonbry
 */
public class GROUP_CONCAT extends AggregateBase<IV> implements IAggregate<IV> {

    /**
	 *
	 */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends AggregateBase.Annotations {
        
        /**
         * The namespace of the lexicon relation.
         */
        public String NAMESPACE = AbstractLiteralBOp.class.getName() + ".namespace";

        /**
         * Required string property provides the separator used when combining
         * the {@link IValueExpression} computed for each solution within the
         * group.
         *
         * Use basic string to match sparql scalarValues param
         */
        String SEPARATOR = "separator";

        /**
         * The maximum #of values to concatenate (positive integer and
         * <code>-1</code> to indicate no bound) (default
         * {@value #DEFAULT_VALUE_LIMIT})
         */
        String VALUE_LIMIT = GROUP_CONCAT.class.getName() + ".valueLimit";

        /**
         * The default indicates no limit.
         */
        final int DEFAULT_VALUE_LIMIT = -1;

        /**
         * The maximum #of characters permitted in the generated value (positive
         * integer and <code>-1</code> to indicate no bound) (default
         * {@value #DEFAULT_CHARACTER_LIMIT}).
         */
        String CHARACTER_LIMIT = GROUP_CONCAT.class.getName()
                + ".characterLimit";

        /**
         * The default indicates no limit.
         */
        final int DEFAULT_CHARACTER_LIMIT = -1;

    }

    public GROUP_CONCAT(GROUP_CONCAT op) {
        super(op);
    }

    public GROUP_CONCAT(BOp[] args, Map<String, Object> annotations) {

        super(args, annotations);
        
        getRequiredProperty(Annotations.NAMESPACE);
//        if (getProperty(Annotations.NAMESPACE) == null)
//            throw new IllegalArgumentException();

    }

    /**
     *
     * @param var
     *            The variable whose values will be combined.
     * @param sep
     *            The separator string (note that a space (0x20) is the default
     *            in the SPARQL recommendation).
     */
    public GROUP_CONCAT(final boolean distinct,
            final IValueExpression<IV> expr, final String namespace,
            final String sep) {

        this(new BOp[] { expr }, NV.asMap(//
//                new NV(Annotations.FUNCTION_CODE, FunctionCode.GROUP_CONCAT),//
                new NV(Annotations.DISTINCT, distinct),//
                new NV(Annotations.NAMESPACE, namespace),//
                new NV(Annotations.SEPARATOR, sep)//
                ));

    }

    private String sep() {
        if (sep == null) {
            sep = (String) getRequiredProperty(Annotations.SEPARATOR);
        }
        return sep;
    }

    private transient String sep;

    private int valueLimit() {
        if (valueLimit == 0) {
            valueLimit = getProperty(Annotations.VALUE_LIMIT,
                    Annotations.DEFAULT_VALUE_LIMIT);
        }
        return valueLimit;
    }

    private transient int valueLimit;

    private int characterLimit() {
        if (characterLimit == 0) {
            characterLimit = getProperty(Annotations.CHARACTER_LIMIT,
                    Annotations.DEFAULT_CHARACTER_LIMIT);
        }
        return characterLimit;
    }

    private transient int characterLimit;



    private BigdataValueFactory getValueFactory(){
        if (vf == null) {
            final String namespace = (String) getRequiredProperty(Annotations.NAMESPACE);
            vf = BigdataValueFactoryImpl.getInstance(namespace);
        }
        return vf;
    }

    protected transient BigdataValueFactory vf;

    /**
     * The running concatenation of observed bound values.
     * <p>
     * Note: This field is guarded by the monitor on the {@link GROUP_CONCAT}
     * instance.
     */
    private transient StringBuilder aggregated = null;

    /**
     * The #of values in {@link #aggregated}.
     * <p>
     * Note: This field is guarded by the monitor on the {@link GROUP_CONCAT}
     * instance.
     */
    private transient long nvalues = 0;

    /**
     * <code>false</code> unless either the value limit and/or the character
     * length limit has been exceeded.
     */
    private transient boolean done = false;

    private Throwable firstCause = null;

    synchronized public void reset() {

        aggregated = null;

        nvalues = 0;

        done = false;

        firstCause = null;

        // cache stuff.
        sep();
        valueLimit();
        characterLimit();
        getValueFactory();

    }

    synchronized public IV done() {

        if (firstCause != null) {

            throw new RuntimeException(firstCause);

        }

        if (aggregated == null)
            return DummyConstantNode.toDummyIV(vf.createLiteral(""));

        return DummyConstantNode
                .toDummyIV(vf.createLiteral(aggregated.toString()));

    }

    synchronized public IV get(final IBindingSet bindingSet) {

        try {

            return doGet(bindingSet);

        } catch (Throwable t) {

            if (firstCause == null) {

                firstCause = t;

            }

            throw new RuntimeException(t);

        }

    }

    private IV doGet(final IBindingSet bindingSet) {

        final IValueExpression<IV<?, ?>> expr = (IValueExpression<IV<?, ?>>) get(0);

        final IV<?, ?> iv = expr.get(bindingSet);

        if (iv != null && !done) {

            final String str;
            if (iv.isInline() && !iv.isExtension()) {
                str = iv.getInlineValue().toString();
            } else {
                str = iv.getValue().stringValue();
            }

            if (aggregated == null)
                aggregated = new StringBuilder(str);
            else {
                aggregated.append(sep);
                aggregated.append(str);
            }

            nvalues++;

            if (characterLimit != -1 && aggregated.length() >= characterLimit) {
                // Exceeded the character length limit.
                aggregated.setLength(characterLimit()); // truncate.
                done = true;
            } else if (valueLimit != -1 && nvalues >= valueLimit) {
                // Exceeded the value limit.
                done = true;
            }

        }

        // Note: Nothing returned until done().
        return null;

    }

    /**
     * We always need to have the materialized values.
     */
    public Requirement getRequirement() {

        return INeedsMaterialization.Requirement.ALWAYS;

    }

}
