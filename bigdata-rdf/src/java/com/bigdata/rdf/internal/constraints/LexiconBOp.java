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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.internal.impl.AbstractInlineIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;

public abstract class LexiconBOp extends IVValueExpression<IV> implements INeedsMaterialization {
    private static final long serialVersionUID = 5379172979357494107L;
    static final transient Logger log = Logger.getLogger(LexiconBOp.class);
    
    private transient BigdataValueFactory vf;
    
    public interface Annotations extends BOp.Annotations {

        /**
         * The namespace of the lexicon relation.
         */
        String NAMESPACE = LexiconBOp.class.getName() + ".namespace";

    }
    
    /**
     * 
     * @param x
     *            The inner value expression.
     * @param lex
     *            The namespace of the lexicon relation.
     */
    public LexiconBOp(final IValueExpression<? extends IV> x, final String lex) {

        this(new BOp[] { x }, NV.asMap(new NV(Annotations.NAMESPACE, lex)));

    }

    public LexiconBOp(BOp[] args, Map anns) {
        super(args, anns);
        if (getProperty(Annotations.NAMESPACE) == null)
            throw new IllegalArgumentException();
    }

    public LexiconBOp(LexiconBOp op) {
        super(op);
    }

    public IV get(final IBindingSet bs) {

        final IV iv = get(0).get(bs);

        if (log.isDebugEnabled()) {
            log.debug(iv);
        }

        // not yet bound
        if (iv == null)
            throw new SparqlTypeErrorException.UnboundVarException();
        if(vf==null){
            synchronized(this){
                if(vf==null){
        final String namespace = (String) getRequiredProperty(Annotations.NAMESPACE);

        // use to create my simple literals
                    vf = BigdataValueFactoryImpl.getInstance(namespace);

                }
            }
        }

        if(!iv.isLiteral())
            throw new SparqlTypeErrorException();
        
        if (!iv.isInline() && !iv.hasValue())
            throw new NotMaterializedException();

        
        return generateIV(vf, iv, bs);

    }

    protected BigdataLiteral literalValue(IV iv) {
       
        if (iv.isInline()) {
         
            final BigdataURI datatype = vf.asValue(iv.getDTE().getDatatypeURI());
         
            return vf.createLiteral( ((Value) iv).stringValue(),datatype);
       
        } else if (iv.hasValue()) {
      
            return ((BigdataLiteral) iv.getValue());
      
        } else {
      
            throw new NotMaterializedException();
      
        }
        
    }
    
    protected abstract IV generateIV(final BigdataValueFactory vf, final IV iv, final IBindingSet bs) throws SparqlTypeErrorException;

    volatile transient Set<IVariable<IV>> terms;

    public Set<IVariable<IV>> getTermsToMaterialize() {

        if (terms == null) {

            terms = new LinkedHashSet<IVariable<IV>>();

            for (BOp bop : args()) {

                if (bop instanceof IVariable){
                    terms.add((IVariable<IV>) bop);
                }

            }

        }
        return terms;

    }

}
