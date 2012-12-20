package gpath.store.statistics;

import gpath.util.IterableHelper;

import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

public class IndexStatistics implements Statistics {

	public TreeMap<byte[], SortedSet<Long>> index;
	
	public IndexStatistics(TreeMap<byte[], SortedSet<Long>> index) {
		super();
		this.index = index;
	}

	@Override
	public long estimate(byte[] min, byte[] max) {
		long est=0;
		Set<Entry<byte[], SortedSet<Long>>> entrySet;
		if(min==null){
			if(max==null){
				entrySet = index.entrySet();
			}else{
				entrySet = index.headMap(max, true).entrySet();
			}
		}else{
			if(max==null){
				entrySet = index.tailMap(min, true).entrySet();
			}else{
				entrySet = index.subMap(min, true, max, true).entrySet();
			}
		}
		for(Entry<byte[], SortedSet<Long>> set: entrySet){
			est+=set.getValue().size();
		}
		return est;
	}

}
