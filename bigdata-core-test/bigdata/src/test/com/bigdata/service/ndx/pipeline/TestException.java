package com.bigdata.service.ndx.pipeline;

/**
 * Instances of this class are thrown from within the fixture under test in
 * order to provoke various kinds of error handling.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class TestException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public TestException() {

    }

    public TestException(String msg) {

        super(msg);

    }

    public TestException(Throwable cause) {

        super(cause);

    }

    public TestException(String msg, Throwable cause) {

        super(msg, cause);

    }

}