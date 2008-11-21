/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Nov 18, 2008
 */

package com.bigdata.rdf.rules;

import it.unimi.dsi.mg4j.io.OutputBitStream;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.io.IStreamSerializer;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * A class that provides (relatively) compact serialization for a chunk of
 * {@link IBindingSet}s.
 * 
 * FIXME This implementation is not complete and is not tested.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo for a general purpose solution, this could use extSer for a compact
 *       serialization and that will work well with joins binding variables with
 *       many different data types. the set of serializers actually used would
 *       have to be sent as part of the serialized state as any assigned
 *       classIds.
 * 
 * @todo unit tests and performance comparisons of serialization options.
 * 
 * @todo factor out the variable declarations. binding sets coming from joins
 *       tend to have natural order based on the sequence in which the variables
 *       are becoming bound. Leverage that order to produce a much more compact
 *       serialization since duplicate bindings can be elided.
 * 
 * @todo given all the options for {@link IBindingSet} (and {@link ISolution})
 *       serialization, these classes really need to version their own
 *       serialization formats.
 * 
 * @todo for RDF, this can of course be optimized quite a bit since we know that
 *       all the values are long integers.
 * 
 * @todo do a version for {@link ISolution}s as well that accepts the same
 *       constraints (variable bindings are long integers), but also factor out
 *       the rule (which is optional) and the materialized element (which will
 *       be an {@link SPO} and which is also optional). Note that all
 *       {@link ISolution}s that are (de-)serialized together will share the
 *       same flags for whether the rule, bindingset, or element are present.
 *       The serialization of the binding sets themselves can be done with the
 *       {@link SPOBindingSetSerializer}.
 */
public class SPOBindingSetSerializer implements
        IStreamSerializer<IBindingSet[]> {

    /**
     * 
     */
    private SPOBindingSetSerializer() {
    }

    public static final transient IStreamSerializer<IBindingSet[]> INSTANCE = new SPOBindingSetSerializer();

    public IBindingSet[] deserialize(ObjectInput in) {

        return (IBindingSet[]) SerializerUtil.STREAMS.deserialize(in);
        
    }

    public void serialize(final ObjectOutput out, final IBindingSet[] obj) {

//        final int n = obj.length;
//
//        // #of elements to be written.
//        LongPacker.packLong(out, n);
//        
//        if (n == 0) {
//
//            // done.
//            return;
//
//        }
//        
//        /*
//         * Build a map whose keys are the distinct variables used across the
//         * bindingSets and whose values are the unique integers in [0:nvars-1]
//         * assigned to each distinct variable.
//         * 
//         * Note: We need to collect the variables from each BindingSet in case
//         * there is an OPTIONAL and no all variables are bound in each
//         * BindingSet or a UNION with different variables showing up in
//         * different rules.
//         * 
//         * Note: A LinkedHashSet preserves the order in which we encounter the
//         * variables. This becomes the order in which we query the bindingSets
//         * for their bound values and therefore also controls our prefix
//         * compression.
//         */
//        final LinkedHashMap<IVariable, Integer> vars = new LinkedHashMap<IVariable, Integer>();
//        int nvars = 0;
//        {
//
//            for (int i = 0; i < n; i++) {
//
//                final Iterator<IVariable> itr = obj[i].vars();
//
//                while (itr.hasNext()) {
//
//                    vars.put(itr.next(), nvars++);
//
//                }
//
//            }
//            
//        }
//        
//        /*
//         * The bit length of the code.
//         * 
//         * Note: The code for a Variable is simply its index in the vars[].
//         */
//        final int codeBitLength = (int) Math.ceil(Math.log(nvars) / LOG2);
//
//        assert codeBitLength > 0 : "nbindingSets=" + n + ", nvars=" + nvars
//                + ", codeBitLength=" + codeBitLength;
//
//        {
//
//            /*
//             * write the header {nsymbols, codeBitLength}.
//             */
//            LongPacker.packLong(out, nvars);
//            LongPacker.packLong(out, codeBitLength);
//
//            /*
//             * write the dictionary:
//             * 
//             * {packed(symbol) -> bits(code)}*
//             * 
//             * The entries are written in the order in which they were
//             * encountered.
//             */
//            {
//
//                final Iterator<Map.Entry<IVariable, Integer>> itr = vars.entrySet()
//                        .iterator();
//
//                while (itr.hasNext()) {
//
//                    Map.Entry<IVariable, Integer> entry = itr.next();
//
//                    out.writeObject(entry.getKey());
//
//                    LongPacker.packLong(out, entry.getValue());
//
//                }
//
//            }
//
//        }
//
//        /*
//         * Note: We defer the creation of the bit stream until after we have
//         * written out the dictionary since the IVariables have String names and
//         * we need an ObjectOutput on which to write them (they also override
//         * Serializable APIs for canonical serialization).
//         */
//
//        final OutputBitStream obs = new OutputBitStream((OutputStream) out,
//                0/* unbuffered */, false/* reflectionTest */);
//
//        /*
//         * Output any bindings that have changed since the last bindingSet
//         * together with their variable identifier.
//         */
//        {
//
//            IBindingSet last = null;
//
//            for (int i = 0; i < n; i++) {
//
//                X();
//
//            }
//            
//        }

        SerializerUtil.STREAMS.serialize(out, obj);

    }

//    /**
//     * The natural log of 2.
//     */
//    final static transient private double LOG2 = Math.log(2);

}
