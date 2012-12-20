package gpath.store.statistics;

import gpath.util.WritableHelper;

public class RangeStatistics implements Statistics {

	long start;
	long end;
	
	public RangeStatistics(long start, long end){
		this.start = start;
		this.end = end;
	}
	
	@Override
	public long estimate(byte[] min, byte[] max) {
		long start=WritableHelper.parseInt(min);
		long end=WritableHelper.parseInt(max);
		if(start<this.start)start = this.start;
		if(end>this.end)end = this.end;
		return end - start + 1;
	}

}
