package com.bigdata.join;


/**
 * Provides execution for an "empty" program.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class EmptyProgramTask implements IProgramTask {

    final ActionEnum action;

    final IProgram program;

    public EmptyProgramTask(ActionEnum action, IProgram program) {

        if (action == null)
            throw new IllegalArgumentException();

        if (program == null)
            throw new IllegalArgumentException();

        if (program.isRule() || program.stepCount() != 0) {

            throw new IllegalArgumentException();

        }

        this.action = action;

        this.program = program;

    }

    public Object call() {

        if (action.isMutation()) {

            return Long.valueOf(0L);

        } else {

            return new EmptyChunkedIterator<ISolution>(null/* keyOrder */);

        }

    }

}