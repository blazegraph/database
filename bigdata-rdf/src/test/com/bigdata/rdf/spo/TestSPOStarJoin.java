package com.bigdata.rdf.spo;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryEvaluationException;

import com.bigdata.bop.Constant;
import com.bigdata.bop.HashBindingSet;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.Var;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURIImpl;
import com.bigdata.rdf.rules.RuleContextEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.AbstractTripleStoreTestCase;
import com.bigdata.rdf.vocab.NoVocabulary;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

public class TestSPOStarJoin extends AbstractTripleStoreTestCase {

    /**
     * 
     */
    public TestSPOStarJoin() {
    }

    /**
     * @param name
     */
    public TestSPOStarJoin(String name) {
        
        super(name);
        
    }

    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();
        
        props.setProperty(AbstractTripleStore.Options.STATEMENT_IDENTIFIERS, 
                "false");
        
        props.setProperty(AbstractTripleStore.Options.AXIOMS_CLASS, 
                NoAxioms.class.getName());
        
        props.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, 
                NoVocabulary.class.getName());
        
        return props;
        
    }
    
    public void testStarJoin1() throws Exception {
        
        final AbstractTripleStore store = getStore(getProperties());
        
        try {
/*            
            ?frameClass a   FrameClass
            ?frameClass frame:ontologyClass ?class
            ?frameClass frame:frameProperty ?frameProperty

            SPOPredicate(?frameClass,_,_)       
                star    frame:ontologyClass ?class
                        frame:frameProperty ?frameProperty
            
            fc1 frame:ontologyClass class1
            fc1 frame:frameProperty fp1
            fc2 frame:ontologyClass class1
            fc2 frame:frameProperty fp1
            fc2 frame:frameProperty fp2
            
            solutions               
            {fc1,class1,fp1}        
            {fc2,class1,fp1}        
            {fc2,class1,fp2}        
*/            
            final String ns = "http://www.bigdata.com/rdf#";
            
            final ValueFactory vf = store.getValueFactory();
            
            final BigdataURIImpl fc1 = (BigdataURIImpl) vf.createURI(ns+"fc1");
            final BigdataURIImpl fc2 = (BigdataURIImpl) vf.createURI(ns+"fc2");
            final BigdataURIImpl fp1 = (BigdataURIImpl) vf.createURI(ns+"fp1");
            final BigdataURIImpl fp2 = (BigdataURIImpl) vf.createURI(ns+"fp2");
            final BigdataURIImpl class1 = (BigdataURIImpl) vf.createURI(ns+"class1");
            final BigdataURIImpl ontologyClass = (BigdataURIImpl) vf.createURI(ns+"ontologyClass");
            final BigdataURIImpl frameProperty = (BigdataURIImpl) vf.createURI(ns+"frameProperty");
            final BigdataURIImpl frameClass = (BigdataURIImpl) vf.createURI(ns+"FrameClass");
            
            store.addStatement(fc1, RDF.TYPE, frameClass);
            store.addStatement(fc2, RDF.TYPE, frameClass);
            store.addStatement(fc1, ontologyClass, class1);
            store.addStatement(fc1, frameProperty, fp1);
            store.addStatement(fc2, ontologyClass, class1);
            store.addStatement(fc2, frameProperty, fp1);
            store.addStatement(fc2, frameProperty, fp2);
            
            store.commit();
            
            System.err.println(store.dumpStore());
            
            final SPOPredicate pred = new SPOPredicate(
                    store.getSPORelation().getNamespace(),
                    Var.var("frameClass"),
                    new Constant<IV>(store.getIV(RDF.TYPE)),
                    new Constant<IV>(frameClass.getIV())
                    );
            
            final SPOStarJoin starJoin = new SPOStarJoin(
                    store.getSPORelation().getNamespace(),
                    Var.var("frameClass")
                    );
            
            starJoin.addStarConstraint(new SPOStarJoin.SPOStarConstraint(
                    new Constant(ontologyClass.getIV()),
                    Var.var("class")));
            
            starJoin.addStarConstraint(new SPOStarJoin.SPOStarConstraint(
                    new Constant(frameProperty.getIV()),
                    Var.var("frameProperty")));
            
            final Rule rule = new Rule(
                    "testStarJoin",
                    null, // head
                    new IPredicate[] { pred, starJoin }, 
                    null // constraints
                    );
            
            final IEvaluationPlanFactory planFactory = 
                DefaultEvaluationPlanFactory2.INSTANCE;

            final IJoinNexusFactory joinNexusFactory = store
                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.BINDINGS, 
                            null, // filter
                            false, // justify
                            false, // backchain
                            planFactory//
                    );

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(store
                    .getIndexManager());

            final IChunkedOrderedIterator<ISolution> itr1 = 
                joinNexus.runQuery(rule);
            /*
            while (itr1.hasNext()) {
                
                ISolution solution = itr1.next();
                
                IBindingSet bs = solution.getBindingSet();
                
                System.err.println(bs);
                
            }
            */
            Collection<IBindingSet> answer = new LinkedList<IBindingSet>();
            answer.add(createBindingSet(
                    new Binding(Var.var("frameClass"), new Constant<IV>(fc1.getIV())),
                    new Binding(Var.var("class"), new Constant<IV>(class1.getIV())),
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp1.getIV()))
                    ));
            answer.add(createBindingSet(
                    new Binding(Var.var("frameClass"), new Constant<IV>(fc2.getIV())),
                    new Binding(Var.var("class"), new Constant<IV>(class1.getIV())),
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp1.getIV()))
                    ));
            answer.add(createBindingSet(
                    new Binding(Var.var("frameClass"), new Constant<IV>(fc2.getIV())),
                    new Binding(Var.var("class"), new Constant<IV>(class1.getIV())),
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp2.getIV()))
                    ));
            
            compare(itr1, answer);
            
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }

    public void testStarJoin2() throws Exception {
        
        final AbstractTripleStore store = getStore(getProperties());
        
        try {
/*            
            ?frameProp frame:ontologyProperty ?value .      
            ?frameProp frame:multiValued ?multiValued1 .        
            OPTIONAL { ?frameProp frame:propertyRange ?range1 . } .     
            
            SPOPredicate(?frameProp,_,_)       
            star    frame:ontologyProperty ?value   
                    frame:multiValued ?multiValued1 
                    OPTIONAL frame:propertyRange ?range1    
            
            fp1 frame:ontologyProperty  v1
            fp1 frame:multiValued   mv1
            fp1 frame:multiValued   mv2
            
            fp2 frame:ontologyProperty  v2
            fp2 frame:multiValued   mv1
            fp2 frame:multiValued   mv2
            fp2 frame:propertyRange r1
            fp2 frame:propertyRange r2
            
            solutions               
            {fp1,v1,mv1}
            {fp1,v1,mv2}
            {fp2,v2,mv1,r1}
            {fp2,v2,mv1,r2}
            {fp2,v2,mv2,r1}
            {fp2,v2,mv2,r2}
*/            
            final String ns = "http://www.bigdata.com/rdf#";
            
            final ValueFactory vf = store.getValueFactory();
            
            final BigdataURIImpl fp1 = (BigdataURIImpl) vf.createURI(ns+"fp1");
            final BigdataURIImpl fp2 = (BigdataURIImpl) vf.createURI(ns+"fp2");
            final BigdataURIImpl ontologyProperty = (BigdataURIImpl) vf.createURI(ns+"ontologyProperty");
            final BigdataURIImpl multiValued = (BigdataURIImpl) vf.createURI(ns+"multiValued");
            final BigdataURIImpl propertyRange = (BigdataURIImpl) vf.createURI(ns+"propertyRange");
            final BigdataURIImpl v1 = (BigdataURIImpl) vf.createURI(ns+"v1");
            final BigdataURIImpl v2 = (BigdataURIImpl) vf.createURI(ns+"v2");
            final BigdataURIImpl mv1 = (BigdataURIImpl) vf.createURI(ns+"mv1");
            final BigdataURIImpl mv2 = (BigdataURIImpl) vf.createURI(ns+"mv2");
            final BigdataURIImpl r1 = (BigdataURIImpl) vf.createURI(ns+"r1");
            final BigdataURIImpl r2 = (BigdataURIImpl) vf.createURI(ns+"r2");
            
            store.addStatement(fp1, RDF.TYPE, RDFS.RESOURCE);
            store.addStatement(fp1, ontologyProperty, v1);
            store.addStatement(fp1, multiValued, mv1);
            store.addStatement(fp1, multiValued, mv2);
            store.addStatement(fp2, RDF.TYPE, RDFS.RESOURCE);
            store.addStatement(fp2, ontologyProperty, v2);
            store.addStatement(fp2, multiValued, mv1);
            store.addStatement(fp2, multiValued, mv2);
            store.addStatement(fp2, propertyRange, r1);
            store.addStatement(fp2, propertyRange, r2);
            
            store.commit();
            
            System.err.println(store.dumpStore());
            
            final SPOPredicate pred = new SPOPredicate(
                    store.getSPORelation().getNamespace(),
                    Var.var("frameProperty"),
                    new Constant<IV>(store.getIV(RDF.TYPE)),
                    new Constant<IV>(store.getIV(RDFS.RESOURCE))
                    );
            
            final SPOStarJoin starJoin = new SPOStarJoin(
                    store.getSPORelation().getNamespace(),
                    Var.var("frameProperty")
                    );
            
            starJoin.addStarConstraint(new SPOStarJoin.SPOStarConstraint(
                    new Constant(ontologyProperty.getIV()),
                    Var.var("value")));
            
            starJoin.addStarConstraint(new SPOStarJoin.SPOStarConstraint(
                    new Constant(multiValued.getIV()),
                    Var.var("multiValued1")));
            
            starJoin.addStarConstraint(new SPOStarJoin.SPOStarConstraint(
                    new Constant(propertyRange.getIV()),
                    Var.var("range1"),
                    true));
            
            final Rule rule = new Rule(
                    "testStarJoin",
                    null, // head
                    new IPredicate[] { pred, starJoin }, 
                    null // constraints
                    );
            
            final IEvaluationPlanFactory planFactory = 
                DefaultEvaluationPlanFactory2.INSTANCE;

            final IJoinNexusFactory joinNexusFactory = store
                    .newJoinNexusFactory(RuleContextEnum.HighLevelQuery,
                            ActionEnum.Query, IJoinNexus.BINDINGS, 
                            null, // filter
                            false, // justify
                            false, // backchain
                            planFactory//
                    );

            final IJoinNexus joinNexus = joinNexusFactory.newInstance(store
                    .getIndexManager());

            final IChunkedOrderedIterator<ISolution> itr1 = 
                joinNexus.runQuery(rule);
            /*
            while (itr1.hasNext()) {
                
                ISolution solution = itr1.next();
                
                IBindingSet bs = solution.getBindingSet();
                
                System.err.println(bs);
                
            }
            */
            Collection<IBindingSet> answer = new LinkedList<IBindingSet>();
            answer.add(createBindingSet(
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp1.getIV())),
                    new Binding(Var.var("value"), new Constant<IV>(v1.getIV())),
                    new Binding(Var.var("multiValued1"), new Constant<IV>(mv1.getIV()))
                    ));
            answer.add(createBindingSet(
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp1.getIV())),
                    new Binding(Var.var("value"), new Constant<IV>(v1.getIV())),
                    new Binding(Var.var("multiValued1"), new Constant<IV>(mv2.getIV()))
                    ));
            answer.add(createBindingSet(
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp2.getIV())),
                    new Binding(Var.var("value"), new Constant<IV>(v2.getIV())),
                    new Binding(Var.var("multiValued1"), new Constant<IV>(mv1.getIV())),
                    new Binding(Var.var("range1"), new Constant<IV>(r1.getIV()))
                    ));
            answer.add(createBindingSet(
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp2.getIV())),
                    new Binding(Var.var("value"), new Constant<IV>(v2.getIV())),
                    new Binding(Var.var("multiValued1"), new Constant<IV>(mv2.getIV())),
                    new Binding(Var.var("range1"), new Constant<IV>(r1.getIV()))
                    ));
            answer.add(createBindingSet(
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp2.getIV())),
                    new Binding(Var.var("value"), new Constant<IV>(v2.getIV())),
                    new Binding(Var.var("multiValued1"), new Constant<IV>(mv1.getIV())),
                    new Binding(Var.var("range1"), new Constant<IV>(r2.getIV()))
                    ));
            answer.add(createBindingSet(
                    new Binding(Var.var("frameProperty"), new Constant<IV>(fp2.getIV())),
                    new Binding(Var.var("value"), new Constant<IV>(v2.getIV())),
                    new Binding(Var.var("multiValued1"), new Constant<IV>(mv2.getIV())),
                    new Binding(Var.var("range1"), new Constant<IV>(r2.getIV()))
                    ));
            
            compare(itr1, answer);
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }

    protected IBindingSet createBindingSet(final IBinding... bindings) {
        final IBindingSet bindingSet = new HashBindingSet();
        if (bindings != null) {
            for (IBinding b : bindings) {
                bindingSet.set(b.getVar(), b.getVal());
            }
        }
        return bindingSet;
    }
    
    protected void compare(final IChunkedOrderedIterator<ISolution> result,
            final Collection<IBindingSet> answer)
            throws QueryEvaluationException {

        try {
            
            final Collection<IBindingSet> extraResults = new LinkedList<IBindingSet>();
            Collection<IBindingSet> missingResults = new LinkedList<IBindingSet>();
    
            int resultCount = 0;
            int nmatched = 0;
            while (result.hasNext()) {
                ISolution solution = result.next();
                IBindingSet bindingSet = solution.getBindingSet();
                resultCount++;
                boolean match = false;
                if(log.isInfoEnabled())
                    log.info(bindingSet);
                Iterator<IBindingSet> it = answer.iterator();
                while (it.hasNext()) {
                    if (it.next().equals(bindingSet)) {
                        it.remove();
                        match = true;
                        nmatched++;
                        break;
                    }
                }
                if (match == false) {
                    extraResults.add(bindingSet);
                }
            }
            missingResults = answer;
    
            for (IBindingSet bs : extraResults) {
                if (log.isInfoEnabled()) {
                    log.info("extra result: " + bs);
                }
            }
            
            for (IBindingSet bs : missingResults) {
                if (log.isInfoEnabled()) {
                    log.info("missing result: " + bs);
                }
            }
            
            if (!extraResults.isEmpty() || !missingResults.isEmpty()) {
                fail("matchedResults=" + nmatched + ", extraResults="
                        + extraResults.size() + ", missingResults="
                        + missingResults.size());
            }

        } finally {
            
            result.close();
            
        }
        
    }
    
}
