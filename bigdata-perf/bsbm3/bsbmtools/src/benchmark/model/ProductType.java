package benchmark.model;


import java.io.Serializable;
import java.util.*;

import benchmark.vocabulary.BSBM;

@SuppressWarnings("serial")
public class ProductType extends BSBMResource implements Serializable{
	private int nr;
	private String label;
	private String comment;
	private ProductType parent;
	private Vector<Integer> features;//possible features

	//private boolean isLeaf;
	private int depth;
	
	public ProductType(int nr, String label, String comment, ProductType parent)
	{
		this.nr 		= nr;
		this.parent		= parent;
		this.label		= label;
		this.comment	= comment;
		
		if(parent!=null)
			depth = parent.depth + 1;
		else
			depth = 0;

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

	public ProductType getParent() {
		return parent;
	}

	public void setParent(ProductType parent) {
		this.parent = parent;
	}

	public Vector<Integer> getFeatures() {
		return features;
	}

	public void setFeatures(Vector<Integer> features) {
		this.features = features;
	}


	public int getDepth() {
		return depth;
	}
	
	@Override
    public String toString()
	{
		StringBuffer s = new StringBuffer();
		s.append("<");
		s.append(BSBM.INST_NS);
		s.append("ProductType");
		s.append(nr);
		s.append(">");
		return s.toString();
	}
	
	public String getPrefixed() {
		StringBuffer s = new StringBuffer();
		s.append(BSBM.INST_PREFIX);
		s.append("ProductType");
		s.append(nr);
		return s.toString();
	}
}
