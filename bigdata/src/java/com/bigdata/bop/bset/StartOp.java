package com.bigdata.bop.bset;

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * A version of {@link CopyBindingSetOp} which is always evaluated on the query
 * controller.
 */
public class StartOp extends CopyBindingSetOp {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public StartOp(StartOp op) {
		super(op);
	}

	public StartOp(BOp[] args, Map<String, Object> annotations) {

	    super(args, annotations);
        
	    switch (getEvaluationContext()) {
        case CONTROLLER:
            break;
        default:
            throw new UnsupportedOperationException(
                    Annotations.EVALUATION_CONTEXT + "="
                            + getEvaluationContext());
        }

	}

}
