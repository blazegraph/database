package com.bigdata.rdf.sail.sop;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.bop.IVariable;

public class SOpTree implements Iterable<SOp> {

	private IVariable<?>[] required;
	
	private final Collection<SOp> sops;
	
	public final Map<Integer, SOpGroup> allGroups;
	
	public final Map<Integer, SOpGroup> parents;
	
	public final Map<Integer, SOpGroups> children;
	
	public SOpTree(final Collection<SOp> sops) {
		this(sops, null);
	}
	
	public SOpTree(final Collection<SOp> sops, final IVariable<?>[] required) {
		this.required = required;
		this.sops = sops;
		this.allGroups = new LinkedHashMap<Integer, SOpGroup>();
		this.parents = new LinkedHashMap<Integer, SOpGroup>();
		this.children = new LinkedHashMap<Integer, SOpGroups>();
		
		final Map<Integer, List<SOp>> groups = 
			new LinkedHashMap<Integer, List<SOp>>();
		for (SOp sop : sops) {
			final int g = sop.getGroup();
			List<SOp> group = groups.get(g);
			if (group == null) {
				group = new LinkedList<SOp>();
				groups.put(g, group);
			}
			group.add(sop);
		}
		if (!groups.containsKey(0)) {
			// need a dummy root group
			groups.put(0, new LinkedList<SOp>());
		}
		
		for (Integer g : groups.keySet()) {
			final List<SOp> group = groups.get(g);
			final int pg = group.isEmpty() ? -1 : group.get(0).getParentGroup();
			allGroups.put(g, new SOpGroup(g, pg, group));
		}
		
		for (SOpGroup group : this.allGroups.values()) {
			final int g = group.getGroup();
			final int pg = group.getParentGroup();
			if (pg >= 0)
				parents.put(g, allGroups.get(pg));
		}

		for (SOpGroup me : this.allGroups.values()) {
			final int myGroup = me.getGroup();
			final List<SOpGroup> myChildren = new LinkedList<SOpGroup>();
			for (SOpGroup other : allGroups.values()) {
//					final int otherGroup = other.getGroup();
				final int itsParent = other.getParentGroup();
				if (myGroup == itsParent) {
					myChildren.add(other);
				}
			}
			if (myChildren.size() > 0) {
				children.put(myGroup, new SOpGroups(myChildren));
			}
		}
		
	}
	
//	@Override
	public Iterator<SOp> iterator() {
		return sops.iterator();
	}
	
	public Iterator<SOpGroup> groups() {
		return allGroups.values().iterator();
	}
	
	public SOpGroup getRoot() {
		return allGroups.get(0);
	}
	
	public SOpGroup getGroup(final int g) {
		return allGroups.get(g);
	}
	
	public SOpGroup getParent(SOpGroup group) {
		return parents.get(group.getGroup());
	}
	
	public SOpGroups getChildren(SOpGroup group) {
		return children.get(group.getGroup());
	}
	
	public void setRequiredVars(final IVariable<?>[] required) {
		this.required = required;
	}
	
	public IVariable<?>[] getRequiredVars() {
		return required;
	}

	public class SOpGroup implements Iterable<SOp> {

		private final int group, parent;
		
		private final Collection<SOp> sops;
		
		public SOpGroup(final int group, final int parent, 
				final Collection<SOp> sops) {
			this.group = group;
			this.parent = parent;
			this.sops = sops;
		}
		
//		@Override
		public Iterator<SOp> iterator() {
			return sops.iterator();
		}
		
		public int size() {
			return sops.size();
		}
		
		public SOp getSingletonSOp() {
			if (sops.size() != 1)
				throw new UnsupportedOperationException(
						"not a singleton group");
			
			return sops.iterator().next();
		}
		
		public int getGroup() {
			return group;
		}
		
		public int getParentGroup() {
			return parent;
		}
		
		public SOpGroup getParent() {
			return SOpTree.this.getParent(this);
		}
		
		public SOpGroups getChildren() {
			return SOpTree.this.getChildren(this);
		}
		
		public SOpTree getTree() {
			return SOpTree.this;
		}
		
		public boolean isRoot() {
			return group == 0;
		}
		
		public void pruneSOps(final Collection<SOp> sopsToPrune) {
			this.sops.removeAll(sopsToPrune);
		}
		
	}

	public class SOpGroups implements Iterable<SOpGroup> {

		private final Collection<SOpGroup> groups;
		
		public SOpGroups(final Collection<SOpGroup> groups) {
			this.groups = groups;
		}
		
//		@Override
		public Iterator<SOpGroup> iterator() {
			return groups.iterator();
		}
		
		public int size() {
			return groups.size();
		}
		
	}
	
	public String toString() {
        final StringBuilder sb = new StringBuilder();
        final String nl = System.getProperty("line.separator");
//        for (SOp sop : sops) {
//        	sb.append(sop).append(nl);
//        }
        sb.append("SOps by group:").append(nl);
        for (Map.Entry<Integer, SOpGroup> e : this.allGroups.entrySet()) {
        	final SOpGroup g = e.getValue();
        	sb.append(e.getKey() + ": g=" + g.getGroup() + " pg=" + g.getParentGroup());
        	sb.append(nl);
        	for (SOp sop : e.getValue()) {
        		sb.append("  " + sop).append(nl);
        	}
        }
        sb.append("SOp -> parent:").append(nl);
        for (Map.Entry<Integer, SOpGroup> e : this.parents.entrySet()) {
        	sb.append(e.getKey());
        	sb.append(" -> ");
        	sb.append(e.getValue() == null ? "null" : e.getValue().getGroup());
        	sb.append(nl);
        }
        sb.append("SOp -> children:").append(nl);
        for (Map.Entry<Integer, SOpGroups> e : this.children.entrySet()) {
        	final SOpGroups groups = e.getValue();
        	StringBuilder sb2 = new StringBuilder();
        	for (SOpGroup g : groups) {
        		sb2.append(g.getGroup()).append(", ");
        	}
        	sb2.setLength(sb2.length()-2);
        	sb.append(e.getKey() + " -> {" + sb2.toString() + "}");
        	sb.append(nl);
        }
        sb.setLength(sb.length()-1);
        return sb.toString();
	}

}

