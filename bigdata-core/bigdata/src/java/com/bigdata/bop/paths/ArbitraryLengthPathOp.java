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
/*
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.paths;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.solutions.JVMDistinctBindingSetsOp;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode.Annotations;

/**
 * @see {@link ArbitraryLengthPathTask}
 */
public class ArbitraryLengthPathOp extends PipelineOp {

    private static final Logger log = Logger.getLogger(ArbitraryLengthPathOp.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends com.bigdata.bop.PipelineOp.Annotations {

        /**
         * The subquery representing the path between left and right.
         */
        String SUBQUERY = Annotations.class.getName() + ".subquery";
        
        /**
         * The left term - can be a variable or a constant.
         */
        String LEFT_TERM = Annotations.class.getName() + ".leftTerm";

        /**
         * The right term - can be a variable or a constant.
         */
        String RIGHT_TERM = Annotations.class.getName() + ".rightTerm";

        /**
         * The left transitivity variable.
         */
        String TRANSITIVITY_VAR_LEFT = Annotations.class.getName() + ".transitivityVarLeft";

        /**
         * The right transitivity variable.
         */
        String TRANSITIVITY_VAR_RIGHT = Annotations.class.getName() + ".transitivityVarRight";
        
        /**
         * The lower bound on the number of rounds to run.  Can be zero (0) or
         * one (1).  A lower bound of zero is a special kind of path - the
         * Zero Length Path.  A zero length path connects a vertex to itself
         * (in graph parlance).  In the context of arbitrary length paths it
         * means we bind the input onto the output regardless of whether they
         * are actually connected via the path or not.
         */
        String LOWER_BOUND =  Annotations.class.getName() + ".lowerBound";

        /**
         * The upper bound on the number of rounds to run.
         */
        String UPPER_BOUND =  Annotations.class.getName() + ".upperBound";
        
        /**
         * The vars projected into this ALP op.
         */
        String PROJECT_IN_VARS =  Annotations.class.getName() + ".projectInVars";
        
        /**
         * The initial capacity of the {@link ConcurrentHashMap} used to impose 
         * the distinct filter (required to avoid duplicates).
         * 
         * @see #DEFAULT_INITIAL_CAPACITY
         */
        String INITIAL_CAPACITY = HashMapAnnotations.class.getName()
                + ".initialCapacity";

        int DEFAULT_INITIAL_CAPACITY = 16;

        /**
         * The load factor of the {@link ConcurrentHashMap} used to impose the
         * distinct filter (required to avoid duplicates).
         * 
         * @see #DEFAULT_LOAD_FACTOR
         */
        String LOAD_FACTOR = HashMapAnnotations.class.getName() + ".loadFactor";

        float DEFAULT_LOAD_FACTOR = .75f;
        
        /**
         * The middle term - can be a variable or a constant.  Only used
         * when edge var is present.
         */
        String MIDDLE_TERM = Annotations.class.getName() + ".middleTerm";

        /**
         * The edge variable. This is an extension that allows the caller
         * to understand how a node was visited (what was the edge that
         * led to it).  If set, this will allow in duplicate solutions in
         * violation of the SPARQL spec (if a node can be reached by more than
         * one path there will be one solution emitted per path).  Used
         * in conjunction with the middle term.  If the middle term is a var,
         * the edge var must not be the same var, since the middle var is
         * used to calculate transitive closure.
         */
        String EDGE_VAR = Annotations.class.getName() + ".edgeVar";
        
        /**
         * A list of intermediate variables (IVariables) used by the ALP op
         * that should be dropped from the solutions after each round.
         */
        String DROP_VARS = Annotations.class.getName() + ".dropVars";
        
    }

    /**
     * Deep copy constructor.
     */
    public ArbitraryLengthPathOp(final ArbitraryLengthPathOp op) {
        super(op);
    }
    
    /**
     * Shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public ArbitraryLengthPathOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        getRequiredProperty(Annotations.SUBQUERY);

        getRequiredProperty(Annotations.LEFT_TERM);

        getRequiredProperty(Annotations.RIGHT_TERM);

        getRequiredProperty(Annotations.TRANSITIVITY_VAR_LEFT);

        getRequiredProperty(Annotations.TRANSITIVITY_VAR_RIGHT);

        getRequiredProperty(Annotations.LOWER_BOUND);

        getRequiredProperty(Annotations.UPPER_BOUND);

        getRequiredProperty(Annotations.PROJECT_IN_VARS);

        getRequiredProperty(Annotations.DROP_VARS);

    }
    
    public ArbitraryLengthPathOp(final BOp[] args, NV... annotations) {

        this(args, NV.asMap(annotations));
        
    }

    @Override
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ArbitraryLengthPathTask(this, context));
        
    }
    
}
