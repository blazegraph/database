package benchmark.model;

import benchmark.vocabulary.BSBM;

public class ProductFeature extends BSBMResource {
	private int nr;
	private String label;
	private String comment;
	
	public ProductFeature(int nr, String label, String comment)
	{
		this.nr 		= nr;
		this.label		= label;
		this.comment	= comment;
	}

	public int getNr() {
		return nr;
	}

	public void setNr(int nr) {
		this.nr = nr;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	@Override
    public String toString()
	{
		return getURIref(nr);
	}
	
	public static String getURIref(int featureNr)
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(BSBM.INST_NS);
		s.append("ProductFeature");
		s.append(featureNr);
		s.append(">");
		return s.toString();
	}
	
	public static String getPrefixed(int featureNr) {
		StringBuffer s = new StringBuffer();
		s.append(BSBM.INST_PREFIX);
		s.append("ProductFeature");
		s.append(featureNr);
		return s.toString();
	}
}
