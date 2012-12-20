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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + value;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntWritableComparable other = (IntWritableComparable) obj;
		if (value != other.value)
			return false;
		return true;
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
