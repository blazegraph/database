package com.bigdata.util.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Class enumerates all {@link ExecutionException}s thrown for a set of
 * tasks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ExecutionExceptions extends ExecutionException {

    /**
     * 
     */
    private static final long serialVersionUID = -9141020515037822837L;

    private final List<ExecutionException> causes;

    public List<ExecutionException> causes() {

        return Collections.unmodifiableList(causes);

    }

    public ExecutionExceptions(final List<ExecutionException> causes) {

        super(causes.size() + " errors : " + causes.toString());

        this.causes = causes;

    }

    public ExecutionExceptions(final String message,
            final List<ExecutionException> causes) {

        super(message + causes.toString());

        this.causes = causes;

    }

}
