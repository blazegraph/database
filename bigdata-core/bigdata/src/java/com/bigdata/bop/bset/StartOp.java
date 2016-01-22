package com.bigdata.bop.bset;

import java.util.Map;

import com.bigdata.bop.BOp;
import com.bigdata.bop.PipelineOp;

/**
 * A version of {@link CopyOp} which is always evaluated on the query
 * controller.
 * <p>
 * Note: {@link CopyOp} and {@link StartOp} are the same. {@link StartOp} exists
 * solely to reflect its functional role at the end of the query pipeline.
 * <p>
 * Note: {@link StartOp} is generally NOT required in a query plan. It is more
 * of a historical artifact than something that we actually need to have in the
 * query pipeline.  It is perfectly possible to have the query pipeline begin
 * with any of the {@link PipelineOp pipeline operators}.
 */
public class StartOp extends CopyOp {

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
