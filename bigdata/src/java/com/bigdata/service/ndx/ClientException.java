package com.bigdata.service.ndx;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;

/**
 * Exposes a linked list of retry exceptions leading to the failure of an
 * {@link AbstractDataServiceProcedureTask}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClientException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 7802533953100817726L;

    private List<Throwable> causes;

    /**
     * The list of causes, one per failed attempt.
     * 
     * @return
     * 
     * @see #getCause()
     */
    @SuppressWarnings("unchecked")
    public List<Throwable> getCauses() {

        if (causes == null)
            return Collections.EMPTY_LIST;

        return causes;

    }

    /**
     * The final exception thrown which caused the task to fail. Normally
     * this will indicate that the retry count has been exceeded and
     * {@link #getCauses()} will report the underlying problem(s) which the
     * task encountered.
     * 
     * @see #getCauses()
     */
    public Throwable getCause() {

        return super.getCause();

    }

    //        /**
    //         *  
    //         */
    //        public ClientException() {
    //            super();
    //        }

    /**
     * @param msg
     * @param cause
     */
    public ClientException(String msg, Throwable cause) {

        this(msg,cause,null);

    }

    /**
     * @param msg
     * @param cause
     */
    public ClientException(String msg, Throwable cause, List<Throwable> causes) {

        super(msg
                + (causes == null ? "" : ", with " + causes.size() + " causes="
                        + causes), cause);

        this.causes = causes;

    }

    /**
     * @param msg
     */
    public ClientException(String msg, List<Throwable> causes) {

        super(msg
                + (causes == null ? "" : ", with " + causes.size() + " causes="
                        + causes));

        this.causes = causes;

    }

    //        /**
    //         * @param cause
    //         */
    //        public ClientException(Throwable cause, List<Throwable> causes) {
    //
    //            super((causes==null?"":", causes="+causes),cause);
    //
    //            this.causes = causes;
    //            
    //        }

    public void printStackTrace() {

        super.printStackTrace();

        int i = 0;
        
        for(final Throwable t : getCauses() ) {
            
            System.err.println("cause#" + i);

            t.printStackTrace();
            
            i++;
            
        }

    }

    public void printStackTrace(PrintStream s) {

        super.printStackTrace(s);
        
        int i = 0;
        
        for(final Throwable t : getCauses() ) {
            
            s.println("cause#" + i);

            t.printStackTrace(s);
            
            i++;
            
        }
        
    }

    public void printStackTrace(PrintWriter w) {

        super.printStackTrace(w);
        
        int i = 0;
        
        for(final Throwable t : getCauses() ) {
            
            w.println("cause#" + i);

            t.printStackTrace(w);
         
            i++;
            
        }

    }

}
