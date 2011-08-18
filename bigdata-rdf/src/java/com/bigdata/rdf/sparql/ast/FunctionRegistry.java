package com.bigdata.rdf.sparql.ast;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.AggregateOperator;
import org.openrdf.query.algebra.Compare.CompareOp;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IValueExpression;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.aggregate.AggregateBase;
import com.bigdata.bop.rdf.aggregate.GROUP_CONCAT;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.AndBOp;
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
import com.bigdata.rdf.internal.constraints.StrBOp;
import com.bigdata.rdf.internal.constraints.StrdtBOp;
import com.bigdata.rdf.internal.constraints.StrlangBOp;
import com.bigdata.rdf.internal.constraints.StrlenBOp;
import com.bigdata.rdf.internal.constraints.SubstrBOp;
import com.bigdata.rdf.internal.constraints.TrueBOp;
import com.bigdata.rdf.internal.constraints.UcaseBOp;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;

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
 */
public class FunctionRegistry {

    public interface Annotations extends AggregateBase.Annotations{
    }

	private static ConcurrentMap<URI, Factory> factories = new ConcurrentHashMap<URI, Factory>();

	public static final String SPARQL_FUNCTIONS = "http://www.w3.org/2006/sparql-functions#";
	public static final String XPATH_FUNCTIONS = "http://www.w3.org/2005/xpath-functions#";

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
    public static final URI LCASE = new URIImpl(SPARQL_FUNCTIONS+"lcase");
    public static final URI UCASE = new URIImpl(SPARQL_FUNCTIONS+"ucase");
    public static final URI ENCODE_FOR_URI = new URIImpl(SPARQL_FUNCTIONS+"encodeForUri");
    public static final URI STR_LEN = new URIImpl(SPARQL_FUNCTIONS+"strlen");
    public static final URI SUBSTR = new URIImpl(SPARQL_FUNCTIONS+"substr");

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

    public static final URI ABS= new URIImpl(SPARQL_FUNCTIONS+"numeric-abs");
    public static final URI ROUND= new URIImpl(SPARQL_FUNCTIONS+"numeric-round");
    public static final URI CEIL = new URIImpl(SPARQL_FUNCTIONS+"numeric-ceil");
    public static final URI FLOOR = new URIImpl(SPARQL_FUNCTIONS+"numeric-floor");
    public static final URI RAND = new URIImpl(SPARQL_FUNCTIONS+"numeric-rand");

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

                final IValueExpression ve = args[0].getValueExpression();
                return new com.bigdata.bop.rdf.aggregate.AVERAGE(new BOp[]{ve}, scalarValues);

            }
        });

        add(COUNT, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression ve = args[0].getValueExpression();
                return new com.bigdata.bop.rdf.aggregate.COUNT(new BOp[]{ve}, scalarValues);

            }
        });

        add(GROUP_CONCAT, new GroupConcatFactory());

        add(MAX, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression ve = args[0].getValueExpression();
                return new com.bigdata.bop.rdf.aggregate.MAX(new BOp[]{ve}, scalarValues);

            }
        });

        add(MIN, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression ve = args[0].getValueExpression();
                return new com.bigdata.bop.rdf.aggregate.MIN(new BOp[]{ve}, scalarValues);

            }
        });

        add(SAMPLE, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression ve = args[0].getValueExpression();
                return new com.bigdata.bop.rdf.aggregate.SAMPLE(false,ve);

            }
        });

        add(SUM, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression ve = args[0].getValueExpression();
                return new com.bigdata.bop.rdf.aggregate.SUM(false,ve);

            }
        });
		// add the bigdata built-ins

		add(BOUND, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, VarNode.class);

				final IVariable<IV> var = ((VarNode)args[0]).getVar();
				return new IsBoundBOp(var);

			}
		});

		add(IS_LITERAL, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> var = args[0].getValueExpression();
				return new IsLiteralBOp(var);

			}
		});

		add(IS_BLANK, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> var = args[0].getValueExpression();
				return new IsBNodeBOp(var);

			}
		});

		add(IS_URI, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> var = args[0].getValueExpression();
				return new IsURIBOp(var);

			}
		});
		add(IS_IRI, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
				return new IsURIBOp(var);

			}
		});
		add(IS_NUMERIC, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                return new IsNumericBOp(var);

            }
        });

		add(STR, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> var = args[0].getValueExpression();
				return new StrBOp(var, lex);

			}
		});

		add(ENCODE_FOR_URI,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                return new EncodeForURIBOp(var, lex);

            }
        });

		add(LCASE,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                return new LcaseBOp(var, lex);

            }
        });

		add(UCASE,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                return new UcaseBOp(var, lex);

            }
        });
		add(STR_DT,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression<? extends IV> type = args[1].getValueExpression();
                return new StrdtBOp(var,type ,lex);

            }
        });
		add(STR_LANG,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression<? extends IV> lang = args[1].getValueExpression();
                return new StrlangBOp(var,lang ,lex);

            }
        });
		add(STR_LEN,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                return new StrlenBOp(var,lex);

            }
        });
		add(SUBSTR,new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                final IValueExpression<? extends IV> var = args[0].getValueExpression();
                final IValueExpression<? extends IV> start = args[1].getValueExpression();
                final IValueExpression<? extends IV> length = args.length>2?args[2].getValueExpression():null;
                return new SubstrBOp(var,start ,length,lex);

            }
        });
		add(LANG, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> ve =
					args[0].getValueExpression();
				return new LangBOp(ve, lex);

			}
		});
		add(CONCAT, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class);
                IValueExpression<? extends IV> expressions[]=new IValueExpression[args.length];
                for(int i=0;i<args.length;i++){
                    expressions[i] = args[i].getValueExpression();
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
                    expressions[i] = args[i].getValueExpression();
                }
                return new CoalesceBOp(expressions);

            }
        });
		add(IF, new Factory() {
            public IValueExpression<? extends IV> create(final String lex,
                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class,ValueExpressionNode.class);
                final IValueExpression<? extends IV> conditional = args[0].getValueExpression();
                final IValueExpression<? extends IV> expression1 = args[1].getValueExpression();
                final IValueExpression<? extends IV> expression2 = args[2].getValueExpression();
                return new IfBOp(conditional,expression1,expression2);
            }
        });
		add(DATATYPE, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				final IValueExpression<? extends IV> ve =
					args[0].getValueExpression();
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
					args[0].getValueExpression();
				final IValueExpression<? extends IV> right =
					args[1].getValueExpression();
				return new LangMatchesBOp(left, right);

			}
		});

		add(REGEX, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args,
						ValueExpressionNode.class, ValueExpressionNode.class);

				final IValueExpression<? extends IV> var =
					args[0].getValueExpression();
				final IValueExpression<? extends IV> pattern =
					args[1].getValueExpression();

				if (args.length == 2) {

					return new RegexBOp(var, pattern);

				} else {

					final IValueExpression<? extends IV> flags =
						args[2].getValueExpression();

					return new RegexBOp(var, pattern, flags);

				}

			}
		});

		add(AND, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args,
						ValueExpressionNode.class, ValueExpressionNode.class);

				IValueExpression<? extends IV> left =
					args[0].getValueExpression();
				if (!(left instanceof XSDBooleanIVValueExpression)) {
					left = new EBVBOp(left);
				}
				IValueExpression<? extends IV> right =
					args[1].getValueExpression();
				if (!(right instanceof XSDBooleanIVValueExpression)) {
					right = new EBVBOp(right);
				}

				return new AndBOp(left, right);

			}
		});

		add(OR, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args,
						ValueExpressionNode.class, ValueExpressionNode.class);

				IValueExpression<? extends IV> left =
					args[0].getValueExpression();
				if (!(left instanceof XSDBooleanIVValueExpression)) {
					left = new EBVBOp(left);
				}
				IValueExpression<? extends IV> right =
					args[1].getValueExpression();
				if (!(right instanceof XSDBooleanIVValueExpression)) {
					right = new EBVBOp(right);
				}

				return new OrBOp(left, right);

			}
		});

		add(NOT, new Factory() {
			public IValueExpression<? extends IV> create(final String lex,
					Map<String, Object> scalarValues, final ValueExpressionNode... args) {

				checkArgs(args, ValueExpressionNode.class);

				IValueExpression<? extends IV> arg =
					args[0].getValueExpression();
				if (!(arg instanceof XSDBooleanIVValueExpression)) {
					arg = new EBVBOp(arg);
				}

				return new NotBOp(arg);

			}
		});

		  add(IN, new Factory() {
	            public IValueExpression<? extends IV> create(final String lex,
	                    Map<String, Object> scalarValues, final ValueExpressionNode... args) {

	                try{
	                    checkArgs(args, VarNode.class,ConstantNode.class);
	                    IValueExpression<? extends IV> arg =
	                            args[0].getValueExpression();

	                    IConstant<? extends IV> set[]=new IConstant[args.length-1];
	                    for(int i=1;i<args.length;i++){
	                            set[i-1]=((ConstantNode)args[i]).getVal();
	                    }

	                    return new InHashBOp(false,arg, set);
	                }catch(IllegalArgumentException iae){
	                    checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                        IValueExpression<? extends IV> set[]=new IValueExpression[args.length];
                        for(int i=0;i<args.length;i++){
                                set[i]=((ValueExpressionNode)args[i]).getValueExpression();
                        }

                        return new ComputedIN(false,set);
	                }



	            }
	        });
		  add(NOT_IN, new Factory() {
              public IValueExpression<? extends IV> create(final String lex,
                      Map<String, Object> scalarValues, final ValueExpressionNode... args) {

                  try{
                      checkArgs(args, VarNode.class,ConstantNode.class);
                      IValueExpression<? extends IV> arg =
                              args[0].getValueExpression();

                      IConstant<? extends IV> set[]=new IConstant[args.length-1];
                      for(int i=1;i<args.length;i++){
                              set[i-1]=((ConstantNode)args[i]).getVal();
                      }

                      return new InHashBOp(true,arg, set);
                  }catch(IllegalArgumentException iae){
                      checkArgs(args, ValueExpressionNode.class,ValueExpressionNode.class);

                      IValueExpression<? extends IV> set[]=new IValueExpression[args.length];
                      for(int i=0;i<args.length;i++){
                              set[i]=((ValueExpressionNode)args[i]).getValueExpression();
                      }

                      return new ComputedIN(true,set);
                  }



              }
          });
		add(SAME_TERM, new SameTermFactory());

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

	    add(XSD_STR, new CastFactory(XMLSchema.STRING.toString()));

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

    }

    public static final void checkArgs(final ValueExpressionNode[] args,
    		final Class... types) {

    	if (args.length < types.length) {
    		throw new IllegalArgumentException("wrong # of args");
    	}

    	for (int i = 0; i < args.length; i++) {
    		if (!types[i>=types.length?types.length-1:i].isAssignableFrom(args[i].getClass())) {
    			throw new IllegalArgumentException(
    					"wrong type for arg# " + i + ": " + args[i].getClass());
    		}
    	}

    }

	public static final IValueExpression<? extends IV> toVE(
			final String lex, final URI functionURI,
			final Map<String,Object> scalarValues,
			final ValueExpressionNode... args) {

		final Factory f = factories.get(functionURI);

		if (f == null) {
			throw new IllegalArgumentException("unknown function: " + functionURI);
		}

		return f.create(lex, scalarValues, args);

	}

    public static final void add(final URI functionURI, final Factory factory) {

        if (factories.putIfAbsent(functionURI, factory) != null) {

            throw new UnsupportedOperationException("Already declared.");

	    }

	}

    public static final void addAlias(final URI functionURI, final URI aliasURI) {

        if (!factories.containsKey(functionURI)) {

            throw new UnsupportedOperationException("FunctionURI:"+functionURI+ " not present.");

        }

        if (factories.putIfAbsent(aliasURI, factories.get(functionURI)) != null) {

            throw new UnsupportedOperationException("Already declared.");

        }
    }

	public static interface Factory {

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

		public IValueExpression<? extends IV> create(
				final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

			checkArgs(args,
					ValueExpressionNode.class, ValueExpressionNode.class);

			final IValueExpression<? extends IV> left =
				args[0].getValueExpression();
			final IValueExpression<? extends IV> right =
				args[1].getValueExpression();


			if (left.equals(right)&&(left instanceof IVariable||left instanceof Constant)&&(right instanceof IVariable||right instanceof Constant)) {
	    		if (op == CompareOp.EQ || op==CompareOp.LE || op==CompareOp.GE) {

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
		    				return new SameTermBOp(left, right, op);

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
		    				return new SameTermBOp(left, right, op);

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

	        return new CompareBOp(left, right, op);

		}

	}

	public static class SameTermFactory implements Factory {

		public IValueExpression<? extends IV> create(
				final String lex, Map<String, Object> scalarValues, final ValueExpressionNode... args) {

			checkArgs(args,
					ValueExpressionNode.class, ValueExpressionNode.class);

			final IValueExpression<? extends IV> left =
				args[0].getValueExpression();
			final IValueExpression<? extends IV> right =
				args[1].getValueExpression();

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

//	    				return new FalseBOp(new SameTermBOp(left, right));
	    				return FalseBOp.INSTANCE;

	    			} else {

	    				return new CompareBOp(left, right, CompareOp.EQ);

	    			}

	    		}

	    	}

	    	if (right instanceof Constant) {

	    		final IV iv = ((Constant<? extends IV>) right).get();
	            if (!iv.isInline() && iv.isNullIV()) {
//	    		if (iv.isTermId() && iv.getTermId() == TermId.NULL) {

	    			if (iv.isURI() && !(left instanceof DatatypeBOp)) {

//	    				return new FalseBOp(new SameTermBOp(left, right));
	    				return FalseBOp.INSTANCE;

	    			} else {

	    				return new CompareBOp(left, right, CompareOp.EQ);

	    			}

	    		}

	    	}

	        return new SameTermBOp(left, right);

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
				args[0].getValueExpression();
			final IValueExpression<? extends IV> right =
				args[1].getValueExpression();

			return new MathBOp(left, right, op);

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
                args[0].getValueExpression();

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
                args[0].getValueExpression();

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
                args[0].getValueExpression();

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
				bops[i] = args[i].getValueExpression();
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

            checkArgs(args, ValueExpressionNode.class,ConstantNode.class);
            if(!scalarValues.containsKey(Annotations.SEPARATOR)){
                throw new IllegalArgumentException("missing scalar value:"+Annotations.SEPARATOR);
            }

            final IValueExpression ve = args[0].getValueExpression();

            return new com.bigdata.bop.rdf.aggregate.GROUP_CONCAT(new BOp[]{ve},scalarValues);

        }
	}
}
