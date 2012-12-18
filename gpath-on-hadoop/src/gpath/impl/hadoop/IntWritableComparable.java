package gpath.impl.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntWritableComparable implements
		WritableComparable<IntWritableComparable> {

	int value;
	
	public int get(){
		return value;
	}
	
	public void set(int value){
		this.value = value;
	}
	
	public IntWritableComparable(){
		
	}
	
	public IntWritableComparable(int value){
		set(value);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(value);
	}

	@Override
	public int compareTo(IntWritableComparable other) {
		return value - other.value;
	}

}
