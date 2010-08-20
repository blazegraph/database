package com.bigdata.rdf.magic;

import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.MappedProgram;
import com.bigdata.rdf.rules.RuleRdfs11;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A program that uses the fix point of the configured rules to compute the
 * forward closure of the database. Since there is no inherent order among
 * the rules in a fix point program, this program can be easily extended by
 * adding additional rules.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SimpleClosure extends BaseClosure {

    public SimpleClosure(AbstractTripleStore db) {
        
        super( db );
        
    }
    
    public MappedProgram getProgram(String db, String focusStore) {

        final MappedProgram program = new MappedProgram("simpleClosure",
                focusStore, true/* parallel */, true/* closure */);

        program.addStep(new RuleRdfs11(db,vocab));
        
        return program;

    }
    
}