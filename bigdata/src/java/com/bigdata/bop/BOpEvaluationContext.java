package com.bigdata.bop;

import com.bigdata.bop.bset.ConditionalRoutingOp;
import com.bigdata.bop.join.PipelineJoin;
import com.bigdata.bop.solutions.DistinctBindingSetOp;
import com.bigdata.bop.solutions.SliceOp;

/**
 * Type safe enumeration indicates where an operator may be evaluated. Operators
 * fall into several distinct categories based on whether or not their inputs
 * need to be made available on specific nodes ({@link #HASHED} or
 * {@link #SHARDED}), whether they can be evaluated anywhere their inputs may
 * exist ({@link #ANY}), or whether they must be evaluated at the query
 * controller ({@link #CONTROLLER}).
 * <p>
 * Note: All operators are evaluated locally when running against a standalone
 * database.
 */
public enum BOpEvaluationContext {

    /**
     * The operator may be evaluated anywhere, including piecewise evaluation on
     * any node of the cluster where its inputs are available. This is used for
     * operators which do not need to concentrate or coordinate their inputs
     * such as {@link ConditionalRoutingOp}.
     */
    ANY,
    /**
     * The input to the operator must be mapped across nodes using a hash
     * partitioning schema and the operator must be evaluated on each hash
     * partition. This is used for operators such as
     * {@link DistinctBindingSetOp}.
     */
    HASHED,
    /**
     * The input to the operator must be mapped across the shards on which the
     * operator must read or write and the operator must be evaluated shard wise
     * on the services having access to each shard. For example,
     * {@link PipelineJoin}.
     */
    SHARDED,
    /**
     * The operator must be evaluated on the query controller. For example,
     * {@link SliceOp} may not be evaluated piecewise.
     */
    CONTROLLER;

}