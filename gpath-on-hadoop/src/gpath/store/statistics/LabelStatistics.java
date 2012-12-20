package gpath.store.statistics;

import java.util.Comparator;

public class LabelStatistics <T extends Comparator<byte[]>> implements Statistics {

	T comparator;
	long[] numbers; 
	byte[][] medians;
	
	public LabelStatistics(T comparator, long[] numbers, byte[][] medians){
		this.comparator = comparator;
		this.numbers = numbers;
		this.medians = medians;
	}
	
	public long estimate(byte[] min, byte[] max){
		long est=0;
		boolean enter = min == null;
		int i;
		for(i=0;i<numbers.length;i++){
			if(!enter){
				if(comparator.compare(medians[i], min)>0){
					enter = true;
					est += numbers[i];
				}
			}else if(enter){
				if(max!=null && comparator.compare(medians[i], max)>0){
					enter = false;
					break;
				}else{
					est += numbers[i];
				}
			}
		}
		if(enter){
			est+=numbers[i];
		}
		return est;
	}
	
}
