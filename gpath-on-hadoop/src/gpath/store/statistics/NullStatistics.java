package gpath.store.statistics;

public class NullStatistics implements Statistics {

	public long all;
	
	
	
	public NullStatistics(long all) {
		super();
		this.all = all;
	}



	@Override
	public long estimate(byte[] min, byte[] max) {
		if(min==null&&max==null){
			return all;
		}else if(min==null||max==null){
			return all/2;
		}else{
			return all/3;
		}
	}

}
