package com.bigdata.bop.join;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;

public class ZeroMatchRecordingJVMHashIndex extends JVMHashIndex {

   private Set<IBindingSet> keysWithoutMatch;
   
   
   public ZeroMatchRecordingJVMHashIndex(IVariable<?>[] keyVars,
         boolean indexSolutionsHavingUnboundJoinVars, Map<Key, Bucket> map) {
      super(keyVars, indexSolutionsHavingUnboundJoinVars, map);
      
      keysWithoutMatch = new HashSet<IBindingSet>();
   }
   
   
   public void addKeysWithoutMatch(Collection<IBindingSet> keys) {
      keysWithoutMatch.addAll(keys);
   }
   
   public boolean isKeyWithoutMatch(IBindingSet bs) {
      return keysWithoutMatch.contains(bs);
   }
   

}
