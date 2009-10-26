package com.bigdata.util.concurrent;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Class enumerates all exceptions thrown for a set of tasks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ExecutionExceptions extends ExecutionException implements
        Iterable<Throwable> {

    /**
     * 
     */
    private static final long serialVersionUID = -9141020515037822837L;

    private final List<? extends Throwable> causes;

    public List<? extends Throwable> causes() {

        return Collections.unmodifiableList(causes);

    }

    public ExecutionExceptions(final List<? extends Throwable> causes) {

        super(causes.size() + " errors : " + causes.toString());

        this.causes = causes;

    }

    public ExecutionExceptions(final String message,
            final List<? extends Throwable> causes) {

        super(message + causes.toString());

        this.causes = causes;

    }

    public Iterator<Throwable> iterator() {
        
        return (Iterator<Throwable>) causes.iterator();
        
    }

}
