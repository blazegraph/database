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

import java.security.MessageDigest;
import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;

/**
 * A Digest expression involving a {@link IValueExpression} operand. The
 * operation to be applied to the operands is specified by the
 * {@link DigestBOp.Annotations#OP} annotation.
 */
public class DigestBOp extends IVValueExpression<IV> implements INeedsMaterialization {

    private static final long serialVersionUID = 9136864442064392445L;

    public interface Annotations extends IVValueExpression.Annotations {

        /**
         * The operation to be applied to the left operand (required). The value
         * of this annotation is a {@link DigestOp}, such as
         * {@link DigestOp#MD5}.
         * 
         * @see DigestOp
         */
        String OP = DigestBOp.class.getName() + ".op";

    }

    public enum DigestOp {
        MD5, SHA1, SHA224, SHA256, SHA384, SHA512;

    }

    /**
     * 
     * @param left
     *            The left operand.
     * @param right
     *            The right operand.
     * @param op
     *            The annotation specifying the operation to be performed on
     *            those operands.
     */
    public DigestBOp(final IValueExpression<? extends IV> left,
            final DigestOp op, final GlobalAnnotations globals) {

        this(new BOp[] { left }, anns(globals, new NV(Annotations.OP, op)));

    }

    /**
     * Required shallow copy constructor.
     * 
     * @param args
     *            The operands.
     * @param op
     *            The operation.
     */
    public DigestBOp(final BOp[] args, final Map<String, Object> anns) {

        super(args, anns);

        if (args.length != 1 || args[0] == null
                || getProperty(Annotations.OP) == null) {

            throw new IllegalArgumentException();

        }

    }

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     *
     * @param op
     */
    public DigestBOp(final DigestBOp op) {

        super(op);

    }

    private static final char[] hexChar = { '0', '1', '2', '3', '4', '5', '6',
            '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    public static String toHexString(final byte[] buf) {

        final StringBuilder strBuf = new StringBuilder(buf.length * 2);
        
        for (int i = 0; i < buf.length; i++) {
        
            strBuf.append(hexChar[(buf[i] & 0xf0) >>> 4]); // fill left with
            
            // zero bits
            strBuf.append(hexChar[buf[i] & 0x0f]);
            
        }

        return strBuf.toString();
        
    }

	@Override
	public Requirement getRequirement() {
		return Requirement.SOMETIMES;
	}

	@Override
    public IV get(final IBindingSet bs) throws SparqlTypeErrorException {

        final IV iv = getAndCheckLiteral(0, bs);
        //Recreate since they are not thread safe
        MessageDigest md = null;
        final Literal lit = asLiteral(iv);
        if (QueryEvaluationUtil.isStringLiteral(lit)) {
            try {
                String label = lit.getLabel();
                switch (op()) {
                case MD5:
                    md = MessageDigest.getInstance("MD5");
                    break;
                case SHA1:
                    md = MessageDigest.getInstance("SHA-1");
                    break;
                case SHA224:
                    md = MessageDigest.getInstance("SHA-224");
                    break;
                case SHA256:
                    md = MessageDigest.getInstance("SHA-256");
                    break;
                case SHA384:
                    md = MessageDigest.getInstance("SHA-384");
                    break;
               case SHA512:
                    md = MessageDigest.getInstance("SHA-512");
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
                byte[] bytes = label.getBytes("UTF-8");
                md.update(bytes);
                byte[] digest = md.digest();
                final BigdataLiteral str = getValueFactory().createLiteral(toHexString(digest));
                return super.asIV(str, bs);
            } catch (Exception e) {
                throw new SparqlTypeErrorException();
            }
        }
        throw new SparqlTypeErrorException();
    }


    public DigestOp op() {
        return (DigestOp) getRequiredProperty(Annotations.OP);
    }

    public String toString() {

        final StringBuilder sb = new StringBuilder();
        sb.append(op());
        sb.append("(").append(get(0)).append(")");
        return sb.toString();

    }

}
