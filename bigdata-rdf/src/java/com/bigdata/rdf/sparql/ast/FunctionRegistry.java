package com.bigdata.rdf.sparql.ast;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.FN;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.rdf.aggregate.GROUP_CONCAT;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.AndBOp;
import com.bigdata.rdf.internal.constraints.BNodeBOp;
import com.bigdata.rdf.internal.constraints.CoalesceBOp;
import com.bigdata.rdf.internal.constraints.CompareBOp;
import com.bigdata.rdf.internal.constraints.ComputedIN;
import com.bigdata.rdf.internal.constraints.ConcatBOp;
import com.bigdata.rdf.internal.constraints.DatatypeBOp;
import com.bigdata.rdf.internal.constraints.DateBOp;
import com.bigdata.rdf.internal.constraints.DateBOp.DateOp;
import com.bigdata.rdf.internal.constraints.DigestBOp;
import com.bigdata.rdf.internal.constraints.DigestBOp.DigestOp;
import com.bigdata.rdf.internal.constraints.EBVBOp;
import com.bigdata.rdf.internal.constraints.EncodeForURIBOp;
import com.bigdata.rdf.internal.constraints.FalseBOp;
import com.bigdata.rdf.internal.constraints.FuncBOp;
import com.bigdata.rdf.internal.constraints.IfBOp;
import com.bigdata.rdf.internal.constraints.InHashBOp;
import com.bigdata.rdf.internal.constraints.IriBOp;
import com.bigdata.rdf.internal.constraints.IsBNodeBOp;
import com.bigdata.rdf.internal.constraints.IsBoundBOp;
import com.bigdata.rdf.internal.constraints.IsLiteralBOp;
import com.bigdata.rdf.internal.constraints.IsNumericBOp;
import com.bigdata.rdf.internal.constraints.IsURIBOp;
import com.bigdata.rdf.internal.constraints.LangBOp;
import com.bigdata.rdf.internal.constraints.LangMatchesBOp;
import com.bigdata.rdf.internal.constraints.LcaseBOp;
import com.bigdata.rdf.internal.constraints.MathBOp;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.constraints.NotBOp;
import com.bigdata.rdf.internal.constraints.NumericBOp;
import com.bigdata.rdf.internal.constraints.NumericBOp.NumericOp;
import com.bigdata.rdf.internal.constraints.OrBOp;
import com.bigdata.rdf.internal.constraints.RandBOp;
import com.bigdata.rdf.internal.constraints.RegexBOp;
import com.bigdata.rdf.internal.constraints.SameTermBOp;
import com.bigdata.rdf.internal.constraints.SparqlTypeErrorBOp;
import com.bigdata.rdf.internal.constraints.StrAfterBOp;
import com.bigdata.rdf.internal.constraints.StrBOp;
import com.bigdata.rdf.internal.constraints.StrBeforeBOp;
import com.bigdata.rdf.internal.constraints.StrcontainsBOp;
import com.bigdata.rdf.internal.constraints.StrdtBOp;
import com.bigdata.rdf.internal.constraints.StrendsBOp;
import com.bigdata.rdf.internal.constraints.StrlangBOp;
import com.bigdata.rdf.internal.constraints.StrlenBOp;
import com.bigdata.rdf.internal.constraints.StrstartsBOp;
import com.bigdata.rdf.internal.constraints.SubstrBOp;
import com.bigdata.rdf.internal.constraints.TrueBOp;
import com.bigdata.rdf.internal.constraints.UcaseBOp;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;
import com.bigdata.rdf.internal.constraints.XsdStrBOp;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;
import com.bigdata.rdf.store.BD;

/**
 * Registry for built-in and external SPARQL functions.
 * <p>
 * Note: Use an alternative namespace for functions which do not have official
 * namespaces.
 * 
 * @see <a href="http://www.w3.org/2005/xpath-functions/">XPath functions</a>
 * @see <a
 *      href="http://lists.w3.org/Archives/Public/public-rdf-dawg/2006AprJun/att-0138/sparql-function-uri.html">Sparql
 *      functions</a>
 * 
 *      TODO We should restrict registration of functions in the
 *      {@link BD#NAMESPACE} to static initialization.  The code
 *      already does not permit replacement of registered functions.
 */
public class FunctionRegistry {

    public interface Annotations extends AggregateBase.Annotations{
    }

	private static ConcurrentMap<URI, Factory> factories = new ConcurrentHashMap<URI, Factory>();

	public static final String SPARQL_FUNCTIONS = "http://www.w3.org/2006/sparql-functions#";
	public static final String XPATH_FUNCTIONS = "http://www.w3.org/2005/xpath-functions#";

	/**
     * Functions in SPARQL 1.0 for which there is not yet any official URI.
     */
    public static final String SPARQL10_UNDEFINED_FUNCTIONS = "http://www.bigdata.com/sparql-1.0-undefined-functions";

    /**
     * Functions in SPARQL 1.1 for which there is not yet any official URI.
     */
    public static final String SPARQL11_UNDEFINED_FUNCTIONS = "http://www.bigdata.com/sparql-1.1-undefined-functions";

    public static final URI BOUND = new URIImpl(SPARQL_FUNCTIONS+"bound");
    public static final URI IS_LITERAL = new URIImpl(SPARQL_FUNCTIONS+"isLiteral");
    public static final URI IS_BLANK = new URIImpl(SPARQL_FUNCTIONS+"isBlank");
    public static final URI IS_URI = new URIImpl(SPARQL_FUNCTIONS+"isURI");
    public static final URI IS_IRI = new URIImpl(SPARQL_FUNCTIONS+"isIRI");
    public static final URI IS_NUMERIC = new URIImpl(SPARQL_FUNCTIONS+"isNumeric");

    public static final URI STR = new URIImpl(SPARQL_FUNCTIONS+"str");
    public static final URI LANG = new URIImpl(SPARQL_FUNCTIONS+"lang");
    public static final URI DATATYPE = new URIImpl(SPARQL_FUNCTIONS+"datatype");
    public static final URI LANG_MATCHES = new URIImpl(SPARQL_FUNCTIONS+"langMatches");
    public static final URI REGEX = new URIImpl(XPATH_FUNCTIONS+"matches");
    public static final URI OR = new URIImpl(SPARQL_FUNCTIONS+"logical-or");
    public static final URI AND = new URIImpl(SPARQL_FUNCTIONS+"logical-and");
    public static final URI NOT = new URIImpl(XPATH_FUNCTIONS+"not");
    public static final URI SAME_TERM = new URIImpl(SPARQL_FUNCTIONS+"sameTerm");
    public static final URI CONCAT = new URIImpl(SPARQL_FUNCTIONS+"concat");
    public static final URI COALESCE = new URIImpl(SPARQL_FUNCTIONS+"coalesce");

    public static final URI IN = new URIImpl(SPARQL_FUNCTIONS+"in");
    public static final URI NOT_IN = new URIImpl(SPARQL_FUNCTIONS+"notIn");

    public static final URI IF = new URIImpl(SPARQL_FUNCTIONS+"if");

    public static final URI NOW = new URIImpl(SPARQL_FUNCTIONS+"now");
    public static final URI YEAR = new URIImpl(SPARQL_FUNCTIONS+"year-from-dateTime");
    public static final URI MONTH = new URIImpl(SPARQL_FUNCTIONS+"month-from-dateTime");
    public static final URI DAY = new URIImpl(SPARQL_FUNCTIONS+"day-from-dateTime");
    public static final URI HOURS = new URIImpl(SPARQL_FUNCTIONS+"hours-from-dateTime");
    public static final URI MINUTES = new URIImpl(SPARQL_FUNCTIONS+"minutes-from-dateTime");
    public static final URI SECONDS = new URIImpl(SPARQL_FUNCTIONS+"seconds-from-dateTime");
    public static final URI TIMEZONE = new URIImpl(SPARQL_FUNCTIONS+"tz");

    public static final URI MD5 = new URIImpl(SPARQL_FUNCTIONS+"md5");
    public static final URI SHA1 = new URIImpl(SPARQL_FUNCTIONS+"sha1");
    public static final URI SHA224 = new URIImpl(SPARQL_FUNCTIONS+"sha224");
    public static final URI SHA256 = new URIImpl(SPARQL_FUNCTIONS+"sha256");
    public static final URI SHA384 = new URIImpl(SPARQL_FUNCTIONS+"sha384");
    public static final URI SHA512 = new URIImpl(SPARQL_FUNCTIONS+"sha512");

    public static final URI STR_DT = new URIImpl(SPARQL_FUNCTIONS+"strdt");
    public static final URI STR_LANG = new URIImpl(SPARQL_FUNCTIONS+"strlang");
    public static final URI LCASE = FN.LOWER_CASE;//new URIImpl(XPATH_FUNCTIONS+"lower-case");
    public static final URI UCASE = FN.UPPER_CASE;//new URIImpl(XPATH_FUNCTIONS+"upper-case");
    public static final URI ENCODE_FOR_URI = FN.ENCODE_FOR_URI;//new URIImpl(SPARQL_FUNCTIONS+"encodeForUri");
    public static final URI STR_LEN = FN.STRING_LENGTH;//new URIImpl(XPATH_FUNCTIONS+"string-length");
    public static final URI SUBSTR = FN.SUBSTRING;//new URIImpl(SPARQL_FUNCTIONS+"substr");
    public static final URI CONTAINS = FN.CONTAINS;
    public static final URI STARTS_WITH = FN.STARTS_WITH; 
    public static final URI ENDS_WITH = FN.ENDS_WITH;
    public static final URI STR_AFTER = FN.SUBSTRING_AFTER; // FIXME implement. See StrAfter
    public static final URI STR_BEFORE = FN.SUBSTRING_BEFORE; // FIXME implement. See StrBefore
    public static final URI REPLACE = FN.REPLACE; // FIXME implement. See Replace.

    /**
     * The IRI function, as defined in <a
     * href="http://www.w3.org/TR/sparql11-query/#SparqlOps">SPARQL 1.1 Query
     * Language for RDF</a>.
     */
    public static final URI IRI = new URIImpl(SPARQL11_UNDEFINED_FUNCTIONS
            + "iri");

    /**
     * The BNODE()/BNODE(Literal) function as defined in <a
     * href="http://www.w3.org/TR/sparql11-query/#SparqlOps">SPARQL 1.1 Query
     * Language for RDF</a>.
     */
    public static final URI BNODE = new URIImpl(SPARQL11_UNDEFINED_FUNCTIONS+ "bnode");

    /**
     * The EXISTS(graphPattern) function as defined in <a
     * href="http://www.w3.org/TR/sparql11-query/#SparqlOps">SPARQL 1.1 Query
     * Language for RDF</a>.
     */
    public static final URI EXISTS = new URIImpl(SPARQL11_UNDEFINED_FUNCTIONS
            + "exists");

    /**
     * The NOT EXISTS(graphPattern) function as defined in <a
     * href="http://www.w3.org/TR/sparql11-query/#SparqlOps">SPARQL 1.1 Query
     * Language for RDF</a>.
     */
    public static final URI NOT_EXISTS = new URIImpl(
            SPARQL11_UNDEFINED_FUNCTIONS + "not-exists");

    public static final URI EQ = new URIImpl(XPATH_FUNCTIONS+"equal-to");
    public static final URI NE = new URIImpl(XPATH_FUNCTIONS+"not-equal-to");
    public static final URI GT = new URIImpl(XPATH_FUNCTIONS+"greater-than");
    public static final URI GE = new URIImpl(XPATH_FUNCTIONS+"greater-than-or-equal-to");
    public static final URI LT = new URIImpl(XPATH_FUNCTIONS+"less-than");
    public static final URI LE = new URIImpl(XPATH_FUNCTIONS+"less-than-or-equal-to");

    public static final URI ADD = new URIImpl(XPATH_FUNCTIONS+"numeric-add");
    public static final URI SUBTRACT = new URIImpl(XPATH_FUNCTIONS+"numeric-subtract");
    public static final URI MULTIPLY = new URIImpl(XPATH_FUNCTIONS+"numeric-multiply");
    public static final URI DIVIDE = new URIImpl(XPATH_FUNCTIONS+"numeric-divide");

    public static final URI ABS   = FN.NUMERIC_ABS;//new URIImpl(XPATH_FUNCTIONS+"numeric-abs");
    public static final URI ROUND = FN.NUMERIC_ROUND;//new URIImpl(SPARQL_FUNCTIONS+"numeric-round");
    public static final URI CEIL  = FN.NUMERIC_CEIL;//new URIImpl(SPARQL_FUNCTIONS+"numeric-ceil");
    public static final URI FLOOR = FN.NUMERIC_FLOOR;//new URIImpl(SPARQL_FUNCTIONS+"numeric-floor");
    public static final URI RAND  = new URIImpl(SPARQL_FUNCTIONS+"numeric-rand");

    public static final URI AVERAGE = new URIImpl(SPARQL_FUNCTIONS+"average");
    public static final URI COUNT = new URIImpl(SPARQL_FUNCTIONS+"count");
    public static final URI GROUP_CONCAT = new URIImpl(SPARQL_FUNCTIONS+"groupConcat");
    public static final URI MAX = new URIImpl(SPARQL_FUNCTIONS+"max");
    public static final URI MIN = new URIImpl(SPARQL_FUNCTIONS+"min");
    public static final URI SAMPLE = new URIImpl(SPARQL_FUNCTIONS+"sample");
    public static final URI SUM = new URIImpl(SPARQL_FUNCTIONS+"sum");

    public static final URI XSD_BOOL = XMLSchema.BOOLEAN;
    public static final URI XSD_DT = XMLSchema.DATETIME;
    public static final URI XSD_DEC = XMLSchema.DECIMAL;
    public static final URI XSD_DBL = XMLSchema.DOUBLE;
    public static final URI XSD_FLT = XMLSchema.FLOAT;
    public static final URI XSD_INT = XMLSchema.INTEGER;
    public static final URI XSD_STR = XMLSchema.STRING;

    static {
        add(AVERAGE, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

//                final IValueExpression ve = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);
                
                return new com.bigdata.bop.rdf.aggregate.AVERAGE(new BOp[]{ve}, scalarValues);

            }
        });

        add(COUNT, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

//                final IValueExpression ve = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

                return new com.bigdata.bop.rdf.aggregate.COUNT(new BOp[]{ve}, scalarValues);

            }
        });

        add(GROUP_CONCAT, new GroupConcatFactory());

        add(MAX, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                IValueExpression<? extends IV> expressions[]=new IValueExpression[args.length];
                for(int i=0;i<args.length;i++){
                    expressions[i] = AST2BOpUtility.toVE(lex, args[i]);
                }
                return new com.bigdata.bop.rdf.aggregate.MAX(expressions, scalarValues);
            }
        });

        add(MIN, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                IValueExpression<? extends IV> expressions[]=new IValueExpression[args.length];
                for(int i=0;i<args.length;i++){
                    expressions[i] = AST2BOpUtility.toVE(lex, args[i]);
                }
                return new com.bigdata.bop.rdf.aggregate.MIN(expressions, scalarValues);

            }
        });

        add(SAMPLE, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

//              final IValueExpression ve = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

                return new com.bigdata.bop.rdf.aggregate.SAMPLE(false,ve);

            }
        });

        add(SUM, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                IValueExpression<? extends IV> expressions[]=new IValueExpression[args.length];
                for(int i=0;i<args.length;i++){
                    expressions[i] = AST2BOpUtility.toVE(lex, args[i]);
                }
                return new com.bigdata.bop.rdf.aggregate.SUM(false,expressions);

            }
        });
		// add the bigdata built-ins

		add(BOUND, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, VarNode.class);

                final IVariable<IV> var = ((VarNode) args[0])
                        .getValueExpression();

                return new IsBoundBOp(var, lex);

			}
		});

		add(IS_LITERAL, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

//              final IValueExpression ve = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

				return new IsLiteralBOp(ve, lex);

			}
		});

		add(IS_BLANK, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

//              final IValueExpression ve = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

				return new IsBNodeBOp(ve, lex);

			}
		});

		add(IS_URI, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

//				final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

                return new IsURIBOp(ve, lex);

			}
		});
		add(IS_IRI, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

//                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

				return new IsURIBOp(ve, lex);

			}
		});
		add(IS_NUMERIC, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

//                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

                return new IsNumericBOp(ve, lex);

            }
        });

		add(STR, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

//				final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

				return new StrBOp(ve, lex);

			}
		});

		add(ENCODE_FOR_URI,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

//                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

                return new EncodeForURIBOp(ve, lex);

            }
        });

		add(LCASE,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(lex, args[0]);
                return new LcaseBOp(ve, lex);

            }
        });

		add(UCASE,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(lex, args[0]);
                return new UcaseBOp(ve, lex);

            }
        });
		add(STR_DT,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(lex, args[0]);
                final IValueExpression<? extends IV> type = AST2BOpUtility.toVE(lex, args[1]);
                return new StrdtBOp(ve,type ,lex);

            }
        });
		add(STR_LANG,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = AST2BOpUtility.toVE(lex, args[0]);
                final IValueExpression<? extends IV> lang = AST2BOpUtility.toVE(lex, args[1]);
                return new StrlangBOp(var,lang ,lex);

            }
        });
		add(STR_LEN,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = AST2BOpUtility.toVE(lex, args[0]);
                return new StrlenBOp(var,lex);

            }
        });
		add(SUBSTR,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = AST2BOpUtility.toVE(
                        lex, args[0]);

//                if (args.length == 2) {
//                	
//                } else {
                	
	                final IValueExpression<? extends IV> start = AST2BOpUtility
	                        .toVE(lex, args[1]);
	                
	                final IValueExpression<? extends IV> length = args.length >= 3 ? AST2BOpUtility
	                        .toVE(lex, args[2]) : null;
	                
	                return new SubstrBOp(var, start, length, lex);
	                
//                }

            }
        });
        add(CONTAINS,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,
                        ValueExpressionNode.class);

                final IValueExpression<? extends IV> x = AST2BOpUtility.toVE(
                        lex, args[0]);

                final IValueExpression<? extends IV> y = AST2BOpUtility.toVE(
                        lex, args[1]);

                return new StrcontainsBOp(x, y, lex);

            }
        });
		add(LANG, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> ve =
					AST2BOpUtility.toVE(lex, args[0]);
				return new LangBOp(ve, lex);

			}
		});
		add(CONCAT, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);
                IValueExpression<? extends IV> expressions[]=new IValueExpression[args.length];
                for(int i=0;i<args.length;i++){
                    expressions[i] = AST2BOpUtility.toVE(lex, args[i]);
                }
                return new ConcatBOp(lex,expressions);

            }
        });
		add(COALESCE, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);
                IValueExpression<? extends IV> expressions[]=new IValueExpression[args.length];
                for(int i=0;i<args.length;i++){
                    expressions[i] = AST2BOpUtility.toVE(lex, args[i]);
                }
                return new CoalesceBOp(expressions);

            }
        });
		add(IF, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class,ValueExpressionNode.class);
                final IValueExpression<? extends IV> conditional = AST2BOpUtility.toVE(lex, args[0]);
                final IValueExpression<? extends IV> expression1 = AST2BOpUtility.toVE(lex, args[1]);
                final IValueExpression<? extends IV> expression2 = AST2BOpUtility.toVE(lex, args[2]);
                return new IfBOp(conditional,expression1,expression2);
            }
        });
		add(DATATYPE, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> ve =
					AST2BOpUtility.toVE(lex, args[0]);
				return new DatatypeBOp(ve, lex);

			}
		});

		add(RAND,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {
                if(args!=null&&args.length>0){
                    throw new IllegalArgumentException("wrong # of args");
                }
                return new RandBOp();

            }
        });

		add(LANG_MATCHES, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args,
						ValueExpressionNode.class, ValueExpressionNode.class);

				final IValueExpression<? extends IV> left =
					AST2BOpUtility.toVE(lex, args[0]);

				final IValueExpression<? extends IV> right =
					AST2BOpUtility.toVE(lex, args[1]);
				
				return new LangMatchesBOp(left, right, lex);

			}
		});

		add(REGEX, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args,
						ValueExpressionNode.class, ValueExpressionNode.class);

				final IValueExpression<? extends IV> var =
					AST2BOpUtility.toVE(lex, args[0]);
				
				final IValueExpression<? extends IV> pattern =
					AST2BOpUtility.toVE(lex, args[1]);

				if (args.length == 2) {

					return new RegexBOp(var, pattern, lex);

				} else {

					final IValueExpression<? extends IV> flags =
						AST2BOpUtility.toVE(lex, args[2]);

					return new RegexBOp(var, pattern, flags, lex);

				}

			}
		});

		add(AND, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args,
						ValueExpressionNode.class, ValueExpressionNode.class);

				IValueExpression<? extends IV> left =
					AST2BOpUtility.toVE(lex, args[0]);
				
				if (!(left instanceof XSDBooleanIVValueExpression)) {
				
				    left = new EBVBOp(left, lex);
				    
				}
				
				IValueExpression<? extends IV> right =
					AST2BOpUtility.toVE(lex, args[1]);
				
				if (!(right instanceof XSDBooleanIVValueExpression)) {
				
				    right = new EBVBOp(right, lex);
				    
				}

				return new AndBOp(left, right, lex);

			}
		});

		add(OR, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args,
						ValueExpressionNode.class, ValueExpressionNode.class);

				IValueExpression<? extends IV> left =
					AST2BOpUtility.toVE(lex, args[0]);
				
				if (!(left instanceof XSDBooleanIVValueExpression)) {
				
				    left = new EBVBOp(left, lex);
				    
				}
				
				IValueExpression<? extends IV> right =
					AST2BOpUtility.toVE(lex, args[1]);

				if (!(right instanceof XSDBooleanIVValueExpression)) {
				
				    right = new EBVBOp(right, lex);
				    
				}

				return new OrBOp(left, right, lex);

			}
		});

		add(NOT, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

                IValueExpression<? extends IV> arg = AST2BOpUtility.toVE(lex, args[0]);

				if (!(arg instanceof XSDBooleanIVValueExpression)) {
				
				    arg = new EBVBOp(arg, lex);
				    
				}

				return new NotBOp(arg, lex);

			}
		});

		add(IRI,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

//                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

                return new IriBOp(ve, lex);

            }
        });

		add(BNODE,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

            	if (args.length == 0) {
            		
            		return new BNodeBOp(lex);
            		
            	} else {
                
            		checkArgs(args, ValueExpressionNode.class);

//                final IValueExpression<? extends IV> var = args[0].getValueExpression();
	                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);
	
	                return new BNodeBOp(ve, lex);

            	}
            	
            }
            
        });

		add(STARTS_WITH,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = AST2BOpUtility.toVE(
                        lex, args[0]);
                
                final IValueExpression<? extends IV> token = AST2BOpUtility
                        .toVE(lex, args[1]);
	                
                return new StrstartsBOp(var, token, lex);
	                
            }
        });
		
		add(ENDS_WITH,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = AST2BOpUtility.toVE(
                        lex, args[0]);
                
                final IValueExpression<? extends IV> token = AST2BOpUtility
                        .toVE(lex, args[1]);
	                
                return new StrendsBOp(var, token, lex);
	                
            }
        });
		
		add(STR_BEFORE,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class, ValueExpressionNode.class);

                final IValueExpression<? extends IV> arg1 = AST2BOpUtility.toVE(
                        lex, args[0]);

                final IValueExpression<? extends IV> arg2 = AST2BOpUtility.toVE(
                		lex, args[1]);
                
                return new StrBeforeBOp(arg1, arg2, lex);

            }
        });
		
		add(STR_AFTER,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class, ValueExpressionNode.class);

                final IValueExpression<? extends IV> arg1 = AST2BOpUtility.toVE(
                        lex, args[0]);

                final IValueExpression<? extends IV> arg2 = AST2BOpUtility.toVE(
                		lex, args[1]);
                
                return new StrAfterBOp(arg1, arg2, lex);

            }
        });
		
		add(IN, new InFactory(false/*not*/));

        add(NOT_IN, new InFactory(true/*not*/));

		add(SAME_TERM, SameTermFactory.INSTANCE);

		add(EQ, new CompareFactory(CompareOp.EQ));

		add(NE, new CompareFactory(CompareOp.NE));

		add(GT, new CompareFactory(CompareOp.GT));

		add(GE, new CompareFactory(CompareOp.GE));

		add(LT, new CompareFactory(CompareOp.LT));

		add(LE, new CompareFactory(CompareOp.LE));

		add(ADD, new MathFactory(MathOp.PLUS));

		add(SUBTRACT, new MathFactory(MathOp.MINUS));

		add(MULTIPLY, new MathFactory(MathOp.MULTIPLY));

		add(DIVIDE, new MathFactory(MathOp.DIVIDE));

		add(ABS, new NumericFactory(NumericOp.ABS));

		add(ROUND, new NumericFactory(NumericOp.ROUND));

		add(CEIL, new NumericFactory(NumericOp.CEIL));

		add(FLOOR, new NumericFactory(NumericOp.FLOOR));

	    add(XSD_BOOL, new CastFactory(XMLSchema.BOOLEAN.toString()));

	    add(XSD_DT, new CastFactory(XMLSchema.DATETIME.toString()));

	    add(XSD_DEC, new CastFactory(XMLSchema.DECIMAL.toString()));

	    add(XSD_DBL, new CastFactory(XMLSchema.DOUBLE.toString()));

	    add(XSD_FLT, new CastFactory(XMLSchema.FLOAT.toString()));

	    add(XSD_INT, new CastFactory(XMLSchema.INTEGER.toString()));

	    /*
	     * Changed the xsd:string cast operator to use XsdStrBOp instead of the
	     * Sesame cast function.
	     */
//	    add(XSD_STR, new CastFactory(XMLSchema.STRING.toString()));
		add(XSD_STR, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

//				final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

				return new XsdStrBOp(ve, lex);

			}
		});


	    add(YEAR,new DateFactory(DateOp.YEAR));

	    add(MONTH,new DateFactory(DateOp.MONTH));

	    add(DAY,new DateFactory(DateOp.DAY));

	    add(HOURS,new DateFactory(DateOp.HOURS));

	    add(MINUTES,new DateFactory(DateOp.MINUTES));

	    add(SECONDS,new DateFactory(DateOp.SECONDS));

	    add(TIMEZONE,new DateFactory(DateOp.TIMEZONE));

	    add(MD5,new DigestFactory(DigestOp.MD5));

	    add(SHA1,new DigestFactory(DigestOp.SHA1));

	    add(SHA224,new DigestFactory(DigestOp.SHA224));

	    add(SHA256,new DigestFactory(DigestOp.SHA256));

	    add(SHA384,new DigestFactory(DigestOp.SHA384));

        add(SHA512,new DigestFactory(DigestOp.SHA512));

        add(EXISTS, new ExistsFactory(true));

        add(NOT_EXISTS, new ExistsFactory(false));
       
    }

    public static boolean containsFunction(URI functionUri){
        
        return factories.containsKey(functionUri);
        
    }

    /**
     * Verify type constraints.
     * 
     * @param args
     *            The arguments to some function.
     * @param types
     *            The type constraints. If there are more arguments given than
     *            constraints, then the last constraint in this vararg parameter
     *            will be used to validate the additional arguments.
     * @throws IllegalArgumentException
     *             if the type constraints are violated.
     */
    public static final void checkArgs(final ValueExpressionNode[] args,
    		final Class... types) {

        if (args.length < types.length) {

            throw new IllegalArgumentException("wrong # of args");

    	}

    	for (int i = 0; i < args.length; i++) {

    	    if (!types[i >= types.length ? types.length - 1 : i]
                    .isAssignableFrom(args[i].getClass())) {
            
    	        throw new IllegalArgumentException("wrong type for arg# " + i
                        + ": " + args[i].getClass());
            
    	    }
    	    
    	}

    }

    /**
     * Convert a {@link FunctionNode} into an {@link IValueExpression}.
     * 
     * @param lex
     *            The namespace of the lexicon relation.
     * @param functionURI
     *            The function URI.
     * @param scalarValues
     *            Scalar values for the function (optional). This is used for
     *            things like the <code>separator</code> in GROUP_CONCAT.
     * @param args
     *            The function arguments.
     *            
     * @return The {@link IValueExpression}.
     */
    public static final IValueExpression<? extends IV> toVE(
			final String lex, final URI functionURI,
			final Map<String,Object> scalarValues,
			final ValueExpressionNode... args) {

        if (functionURI == null)
            throw new IllegalArgumentException("functionURI is null");
        
		final Factory f = factories.get(functionURI);

        if (f == null) {
            /*
             * TODO If we eagerly translate FunctionNodes in the AST to IV value
             * expressions then we should probably attach a function which will
             * result in a runtime type error when it encounters value
             * expression for a function URI which was not known to the backend.
             * However, if we handle this translation lazily then this might not
             * be an issue.
             */
            throw new IllegalArgumentException("unknown function: "
                    + functionURI);
        }

		return f.create(lex, scalarValues, args);

	}

    /**
     * Register a factory for a function.
     * 
     * @param functionURI
     *            The function URI.
     * @param factory
     *            The factory.
     * 
     * @throws UnsupportedOperationException
     *             if there is already a {@link Factory} registered for that
     *             URI.
     */
    public static final void add(final URI functionURI, final Factory factory) {

        if (factories.putIfAbsent(functionURI, factory) != null) {

            throw new UnsupportedOperationException("Already declared.");

	    }

	}

    /**
     * Register an alias for a functionURI which has already been declared.
     * 
     * @param functionURI
     *            The function URI.
     * @param aliasURI
     *            The alias.
     * 
     * @throws UnsupportedOperationException
     *             if the function URI has not been declared.
     * @throws UnsupportedOperationException
     *             if the alias URI has already been declared.
     */
    public static final void addAlias(final URI functionURI, final URI aliasURI) {

        if (!factories.containsKey(functionURI)) {

            throw new UnsupportedOperationException("FunctionURI:"+functionURI+ " not present.");

        }

        if (factories.putIfAbsent(aliasURI, factories.get(functionURI)) != null) {

            throw new UnsupportedOperationException("Already declared.");

        }
    }

    /**
     * Remove a registered function {@link Factory}.
     * 
     * @param functionURI
     *            The {@link URI} of the function.
     *            
     * @return The factory -or- <code>null</code> if there was no function
     *         registered for that {@link URI}.
     */
    public static Factory remove(final URI functionURI) {
        
        return factories.remove(functionURI);
        
    }
    
    /**
     * An interface for creating {@link IValueExpression}s from a function URI
     * and its arguments.
     */
	public static interface Factory {

        /**
         * Create an {@link IValueExpression} instance.
         * 
         * @param lex
         *            The namespace of the lexicon.
         * @param scalarValues
         *            The scalar arguments (used by some {@link IAggregate}s).
         * @param args
         *            The function arguments.
         * @return The {@link IValueExpression}.
         */
		IValueExpression<? extends IV> create(
				final String lex,
				final Map<String,Object> scalarValues,
				final ValueExpressionNode... args);

	}

	public static class CompareFactory implements Factory {

		private final CompareOp op;

		public CompareFactory(final CompareOp op) {
			this.op = op;
		}

        public IValueExpression<? extends IV> create(final String lex,
                final Map<String, Object> scalarValues,
                final ValueExpressionNode... args) {

            checkArgs(args,
					ValueExpressionNode.class, ValueExpressionNode.class);

			final IValueExpression<? extends IV> left =
				AST2BOpUtility.toVE(lex, args[0]);
			
			final IValueExpression<? extends IV> right =
				AST2BOpUtility.toVE(lex, args[1]);

            if (left.equals(right)
                    && (left instanceof IVariable || left instanceof Constant)
                    && (right instanceof IVariable || right instanceof Constant)) {
                
                if (op == CompareOp.EQ || op == CompareOp.LE
                        || op == CompareOp.GE) {

	    			return TrueBOp.INSTANCE;

	    		} else {

	    			return FalseBOp.INSTANCE;

	    		}

	    	}

	    	/*
	         * If we are dealing with a URI constant:
	         *
	         * We can use SparqlTypeErrorBOp for any operator other than EQ, NE
	         *
	         * If it's a real term:
	         *
	         * We can use SameTermBOp
	         *
	         * If it's not a real term:
	         *
	         * The only time we actually need to evaluate this is when the other
	         * operand is a DatatypeBOp. All other times, we can return FalseBOp for
	         * EQ and TrueBOp for NE.
	         *
	    	 */

	    	if (left instanceof Constant) {

	    		final IV iv = ((Constant<? extends IV>) left).get();

	    		if (iv.isURI()) {

	    	    	if (!(op == CompareOp.EQ || op == CompareOp.NE)) {

//	    	    		return new SparqlTypeErrorBOp(new CompareBOp(left, right, op));
	    	    		return SparqlTypeErrorBOp.INSTANCE;

	    	    	}

	    	    	/*
	    	    	 * DatatypeBOp is the only bop that can cast things to a URI,
	    	    	 * and that URI might not be in the database. Thus there are no
	    	    	 * optimizations we can do: we can't check term equality, and
	    	    	 * we can't assume automatic false or true.
	    	    	 */
		    		if (!(right instanceof DatatypeBOp)) {

		    			if (!iv.isNullIV()) {

			    			// if it's a real term we can use SameTermBOp
		    				return new SameTermBOp(left, right, op, lex);

		    			} else {

		    				// if it's not a real term then we can substitute false
		    				// for EQ and true for NE
		    				if (op == CompareOp.EQ) {

//	    	        			return new FalseBOp(new CompareBOp(left, right, op));
		    					return FalseBOp.INSTANCE;

		    				} else {

//	    	        			return new TrueBOp(new CompareBOp(left, right, op));
		    					return TrueBOp.INSTANCE;

		    				}

		    			}

		    		}

	    		}

	    	}

	    	if (right instanceof Constant) {

	    		final IV iv = ((Constant<? extends IV>) right).get();

	    		if (iv.isURI()) {

	    	    	if (!(op == CompareOp.EQ || op == CompareOp.NE)) {

//	    	    		return new SparqlTypeErrorBOp(new CompareBOp(left, right, op));
	    	    		return SparqlTypeErrorBOp.INSTANCE;

	    	    	}

	    	    	/*
	    	    	 * DatatypeBOp is the only bop that can cast things to a URI,
	    	    	 * and that URI might not be in the database. Thus there are no
	    	    	 * optimizations we can do: we can't check term equality, and
	    	    	 * we can't assume automatic false or true.
	    	    	 */
		    		if (!(left instanceof DatatypeBOp)) {

		    			if (!iv.isNullIV()) {

			    			// if it's a real term we can use SameTermBOp
		    				return new SameTermBOp(left, right, op, lex);

		    			} else {

		    				// if it's not a real term then we can substitute false
		    				// for EQ and true for NE
		    				if (op == CompareOp.EQ) {

//	    	        			return new FalseBOp(new CompareBOp(left, right, op));
		    					return FalseBOp.INSTANCE;

		    				} else {

//	    	        			return new TrueBOp(new CompareBOp(left, right, op));
		    					return TrueBOp.INSTANCE;

		    				}

		    			}

		    		}

	    		}

	    	}

	        return new CompareBOp(left, right, op, lex);

		}

	}

	public static class SameTermFactory implements Factory {

	    public static final SameTermFactory INSTANCE = new SameTermFactory();
	    
		public IValueExpression<? extends IV> create(
				final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

			checkArgs(args,
					ValueExpressionNode.class, ValueExpressionNode.class);

			final IValueExpression<? extends IV> left =
				AST2BOpUtility.toVE(lex, args[0]);
			
			final IValueExpression<? extends IV> right =
				AST2BOpUtility.toVE(lex, args[1]);

			/*
			 * If a constant operand in the SameTerm op uses a value not found in
			 * the database, we end up in one of two possible situations:
			 *
			 * 1. If the constant operand is a URI, there is no possible way for
			 * SameTerm to evaluate to true, unless the other operand is a
			 * DatatypeBOp. (This is because DatatypeBOp will stamp phony TermId IVs
			 * for the datatypes for inline numerics and math operations.) So if the
			 * other operand is not a DatatypeBOp, we can just return a FalseBOp
			 * that wraps the SameTermBOp that would have happened (this wrapping is
			 * purely informational).
			 *
			 * 2. If the constant operand is not a URI, we need to defer to the
			 * CompareBOp, which knows how to do value comparisons. SameTermBOp only
			 * works on IVs.
			 */
	    	if (left instanceof Constant) {

	    		final IV iv = ((Constant<? extends IV>) left).get();

	    		if (!iv.isInline() && iv.isNullIV()) {
//	    		if (iv.isTermId() && iv.getTermId() == TermId.NULL) {

	    			if (iv.isURI() && !(right instanceof DatatypeBOp)) {

	    				return FalseBOp.INSTANCE;

	    			} else {

	    				return new CompareBOp(left, right, CompareOp.EQ, lex);

	    			}

	    		}

	    	}

	    	if (right instanceof Constant) {

	    		final IV iv = ((Constant<? extends IV>) right).get();

	    		if (!iv.isInline() && iv.isNullIV()) {
//	    		if (iv.isTermId() && iv.getTermId() == TermId.NULL) {

	    			if (iv.isURI() && !(left instanceof DatatypeBOp)) {

	    				return FalseBOp.INSTANCE;

	    			} else {

	    				return new CompareBOp(left, right, CompareOp.EQ, lex);

	    			}

	    		}

	    	}

	        return new SameTermBOp(left, right, CompareOp.EQ, lex);

		}

	}

	public static class MathFactory implements Factory {

		private final MathOp op;

		public MathFactory(final MathOp op) {
			this.op = op;
		}

		public IValueExpression<? extends IV> create(
				final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

			checkArgs(args,
					ValueExpressionNode.class, ValueExpressionNode.class);

			final IValueExpression<? extends IV> left =
				AST2BOpUtility.toVE(lex, args[0]);
			final IValueExpression<? extends IV> right =
				AST2BOpUtility.toVE(lex, args[1]);

			return new MathBOp(left, right, op,lex);

		}

	}

	public static class NumericFactory implements Factory {

        private final NumericOp op;

        public NumericFactory(final NumericOp op) {
            this.op = op;
        }

        public IValueExpression<? extends IV> create(
                final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

            checkArgs(args,
                    ValueExpressionNode.class);

            final IValueExpression<? extends IV> left =
                AST2BOpUtility.toVE(lex, args[0]);

            return new NumericBOp(left,  op);

        }

    }
	public static class DigestFactory implements Factory {

        private final DigestOp op;

        public DigestFactory(final DigestOp op) {
            this.op = op;
        }

        public IValueExpression<? extends IV> create(
                final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

            checkArgs(args,
                    ValueExpressionNode.class);

            final IValueExpression<? extends IV> left =
                AST2BOpUtility.toVE(lex, args[0]);

            return new DigestBOp(left,  op, lex);

        }

    }

	public static class DateFactory implements Factory {

        private final DateOp op;

        public DateFactory(final DateOp op) {
            this.op = op;
        }

        public IValueExpression<? extends IV> create(
                final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

            checkArgs(args,
                    ValueExpressionNode.class);

            final IValueExpression<? extends IV> left =
                AST2BOpUtility.toVE(lex, args[0]);

            return new DateBOp(left,  op);

        }

    }

	public static class CastFactory implements Factory {

		private final String uri;

		public CastFactory(final String uri) {
			this.uri = uri;
		}

		public IValueExpression<? extends IV> create(
				final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

			final IValueExpression<? extends IV>[] bops =
				new IValueExpression[args.length];

			for (int i = 0; i < args.length; i++) {
				bops[i] = AST2BOpUtility.toVE(lex, args[i]);
			}

			return new FuncBOp(bops, uri, lex);

		}

	}

	public static class GroupConcatFactory implements Factory {
	   
	    public interface Annotations extends GROUP_CONCAT.Annotations{
	    }

	    public GroupConcatFactory() {
        }

	    public IValueExpression<? extends IV> create(final String lex,
	            final Map<String,Object> scalarValues,
                final ValueExpressionNode... args) {

            checkArgs(args, ValueExpressionNode.class);
            
            final LinkedHashMap<String, Object> tmp = new LinkedHashMap<String, Object>(
                    scalarValues);

            if (!scalarValues.containsKey(Annotations.SEPARATOR)) {
                
                // Per the spec.
                tmp.put(Annotations.SEPARATOR, " ");

            }

            tmp.put(Annotations.NAMESPACE, lex);
            
            final IValueExpression ve = AST2BOpUtility.toVE(lex, args[0]);

            return new com.bigdata.bop.rdf.aggregate.GROUP_CONCAT(
                    new BOp[] { ve }, tmp);

        }
	    
	}

    /**
     * <code>NumericExpression IN ArgList</code> is an infix operator. The left
     * argument in the syntax must be provided as the first argument to this
     * factory. The ArgList must be provided as the [1...nargs] arguments to the
     * factory.
     * <p>
     * This optimizes IN/0 (foo IN()) as FALSE.
     * <p>
     * This optimizes IN/1 (foo IN(valueExpr) as SameTerm.
     * <p>
     * This optimizes expr IN Constants using a hash set over the constants.
     */
    public static class InFactory implements Factory {

        private final boolean not;

        /**
         * 
         * @param not
         *            <code>true</code> iff this is NOT IN.
         */
        public InFactory(final boolean not) {

            this.not = not;

        }

        public IValueExpression<? extends IV> create(final String lex,
                final Map<String, Object> scalarValues,
                final ValueExpressionNode... args) {

            if (args.length == 0) {
                /*
                 * The first argument is the left hand side of the infix IN
                 * operator.
                 */
                throw new IllegalArgumentException();
            }
            
            if (args.length == 1) {

                /*
                 * "foo IN()" is always false. 
                 * 
                 * "foo NOT IN()" is always true.
                 */
                return not ? TrueBOp.INSTANCE : FalseBOp.INSTANCE;

            }

            if (args.length == 2) {

                /*
                 * "foo IN(bar)" is SameTerm(foo,bar) if bar is a URI, otherwise CompareBOp.
                 */

//            	final IValueExpression<? extends IV> val = AST2BOpUtility.toVE(lex, args[1]);
            	
//                final IValueExpression ret = SameTermFactory.INSTANCE.create(
//                        lex, scalarValues, args);
            	
            	// compare factory is smart enough to optimize for SameTerm when necessary
            	final IValueExpression ret = new CompareFactory(CompareOp.EQ).create(
                      lex, scalarValues, args);

                if (not)
                    return new NotBOp(ret, lex);

                return ret;

            }

            try {

                /*
                 * First, attempt to use an optimized variant. The args MUST be
                 * [var,constant(s)].
                 */

                checkArgs(args, ValueExpressionNode.class, ConstantNode.class);

                final IValueExpression<? extends IV> arg = AST2BOpUtility.toVE(lex, args[0]);

                final IConstant<? extends IV> set[] = new IConstant[args.length - 1];

                try {
                
                	for (int i = 1; i < args.length; i++) {

                		set[i - 1] = ((ConstantNode) args[i]).getValueExpression();
                		
                		final IV iv = set[i - 1].get();
                		
                		if (iv.isLiteral()) {
                			
                			throw new IllegalArgumentException("must use CompareBOps for literals");
                			
                		}

                	}
                	
                	return new InHashBOp(lex, not, arg, set);
                	
                } catch (IllegalArgumentException ex) {
                	
                	IValueExpression ret = null;
                	
                	final CompareFactory compare = new CompareFactory(CompareOp.EQ);
                	
                	for (int i = 1; i < args.length; i++) {
                		
                		if (ret == null) {
                			
                            ret = compare.create(lex, scalarValues, args[0],
                                    args[i]);

                        } else {

                            ret = new OrBOp(ret, compare.create(lex,
                                    scalarValues, args[0], args[i]), lex);

                		}
                		
                	}
                	
                    if (not)
                        return new NotBOp(ret, lex);

                    return ret;
                    
                }

            } catch (IllegalArgumentException iae) {

                /*
                 * Use a variant which handles value expressions for the members
                 * of the set. The first member of the list is taken to be the
                 * valueExpr which is then tested against each other member of
                 * the list.
                 */

                checkArgs(args, ValueExpressionNode.class,
                        ValueExpressionNode.class);

                final IValueExpression<? extends IV> set[] = new IValueExpression[args.length];

                for (int i = 0; i < args.length; i++) {

                    set[i] = AST2BOpUtility.toVE(lex, args[i]);

                }

                return new ComputedIN(not, lex, set);

            }

        }

    }

    /**
     * Factory for EXISTS() and NOT EXISTS(). The EXISTS() node in the AST must
     * be marked with the group graph pattern as an annotation. The
     * {@link FunctionNode} will be a simple test of an anonymous variable.
     * EXISTS tests the variable for <code>true</code>. Not exists tests the
     * variable for <code>false</code>. The name of that anonymous variable is
     * the sole {@link ValueExpressionNode} passed into this factory.
     * <p>
     * The "guts" of the EXISTS logic is translated by an {@link IASTOptimizer}
     * into an ASK subquery (an instance of {@link SubqueryRoot}). This will be
     * executed as a special BOp that bind the success or failure of that ASK
     * subquery on the anonymous variable. The "ASK" subquery does not fail, it
     * just binds the truth state of the ASK subquery on the anonymous variable.
     * The {@link IValueExpression} returned by this factory just tests the
     * truth state of that anonymous variable.
     */
    public static class ExistsFactory implements Factory {

        final private boolean exists;

        public ExistsFactory(boolean exists) {

            this.exists = exists;

        }

        @Override
        public IValueExpression<? extends IV> create(String lex,
                Map<String, Object> scalarValues, ValueExpressionNode... args) {

            if (args.length != 1)
                throw new IllegalArgumentException();

            final VarNode anonvar = (VarNode) args[0];

            return exists ? anonvar.getValueExpression() : new NotBOp(
                    anonvar.getValueExpression(), lex);

        }

    }

}
