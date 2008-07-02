package com.bigdata.relation.rule.eval;

import com.bigdata.relation.accesspath.EmptyChunkedIterator;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IStep;


/**
 * Provides execution for an "empty" program.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyProgramTask implements IProgramTask {

    final ActionEnum action;

    final IStep program;

    /**
     * 
     * @param action
     * @param step
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             unless the <i>step</i> is an empty {@link IProgram}.
     */
    public EmptyProgramTask(ActionEnum action, IStep step) {

        if (action == null)
            throw new IllegalArgumentException();

        if (step == null)
            throw new IllegalArgumentException();

        if (step.isRule() || ((IProgram)step).stepCount() != 0) {

            throw new IllegalArgumentException();

        }

        this.action = action;

        this.program = step;

    }

    public Object call() {

        if (action.isMutation()) {

            return Long.valueOf(0L);

        } else {

            return new EmptyChunkedIterator<ISolution>(null/* keyOrder */);

        }

    }

}