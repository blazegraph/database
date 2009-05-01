package com.bigdata.rdf.iris;

import com.bigdata.rdf.rules.BaseClosure;
import com.bigdata.rdf.rules.MappedProgram;
import com.bigdata.rdf.rules.RuleOwlEquivalentClass;
import com.bigdata.rdf.rules.RuleOwlEquivalentProperty;
import com.bigdata.rdf.rules.RuleOwlInverseOf1;
import com.bigdata.rdf.rules.RuleOwlInverseOf2;
import com.bigdata.rdf.rules.RuleOwlSameAs1;
import com.bigdata.rdf.rules.RuleOwlSameAs1b;
import com.bigdata.rdf.rules.RuleOwlSameAs2;
import com.bigdata.rdf.rules.RuleOwlSameAs3;
import com.bigdata.rdf.rules.RuleOwlTransitiveProperty1;
import com.bigdata.rdf.rules.RuleOwlTransitiveProperty2;
import com.bigdata.rdf.rules.RuleRdf01;
import com.bigdata.rdf.rules.RuleRdfs02;
import com.bigdata.rdf.rules.RuleRdfs03;
import com.bigdata.rdf.rules.RuleRdfs04a;
import com.bigdata.rdf.rules.RuleRdfs04b;
import com.bigdata.rdf.rules.RuleRdfs05;
import com.bigdata.rdf.rules.RuleRdfs06;
import com.bigdata.rdf.rules.RuleRdfs07;
import com.bigdata.rdf.rules.RuleRdfs08;
import com.bigdata.rdf.rules.RuleRdfs09;
import com.bigdata.rdf.rules.RuleRdfs10;
import com.bigdata.rdf.rules.RuleRdfs11;
import com.bigdata.rdf.rules.RuleRdfs12;
import com.bigdata.rdf.rules.RuleRdfs13;
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