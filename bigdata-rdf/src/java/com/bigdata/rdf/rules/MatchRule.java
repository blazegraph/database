package com.bigdata.rdf.rules;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IN;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.Rule;

/**
 * Rule supporting {@link LocalTripleStore#match(Literal[], URI[], URI)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MatchRule extends Rule<SPO> {

    /**
     * 
     */
    private static final long serialVersionUID = -5002902183499739018L;

    public MatchRule(String relationName, Vocabulary vocab,
            IVariable<Long> lit, IConstant<Long>[] preds,
            IConstant<Long> cls) {

    super(  "matchRule", //
            new SPOPredicate(relationName, var("s"), var("t"), lit), //
            new SPOPredicate[] {
                //
                new SPOPredicate(relationName, var("s"), var("p"), lit),
                //
                new SPOPredicate(new String[] { relationName },
                                var("s"), //
                                vocab.getConstant(RDF.TYPE),//
                                var("t"), //
                                null, // context
                                false, //optional
                                ExplicitSPOFilter.INSTANCE,// filter
                                null // expander
                                ),
                //
                new SPOPredicate(relationName, var("t"), vocab
                                .getConstant(RDFS.SUBCLASSOF), cls) //
                },
            new IConstraint[] {
                new IN(var("p"), preds) // p IN preds
                });

    }
    
}