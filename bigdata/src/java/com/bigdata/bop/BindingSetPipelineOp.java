package com.bigdata.bop;

/**
 * Interface for evaluating pipeline operations producing and consuming chunks
 * of binding sets.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public interface BindingSetPipelineOp extends PipelineOp<IBindingSet> {

    public interface Annotations extends PipelineOp.Annotations {

        /**
         * The value of the annotation is the {@link BOp.Annotations#BOP_ID} of
         * the ancestor in the operator tree which serves as an alternative sink
         * for binding sets.
         */
        String ALT_SINK_REF = BindingSetPipelineOp.class.getName()
                + ".altSinkRef";

    }

}
