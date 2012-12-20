package gpath.impl.giraph;

import gpath.impl.hadoop.LabelValueWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GPathEdge implements Writable {
	
	LabelValueWritable inLabelValues;
	LabelValueWritable outLabelValues;
	boolean hasOutEdge;
	boolean hasInEdge;
	
	public GPathEdge() {
		super();
		this.hasOutEdge = false;
		this.hasInEdge = false;
		this.inLabelValues = new LabelValueWritable();
		this.outLabelValues = new LabelValueWritable();
	}

	public LabelValueWritable getInLabelValues() {
		return inLabelValues;
	}

	public void setInLabelValues(LabelValueWritable inLabelValues) {
		this.inLabelValues = inLabelValues;
	}

	public LabelValueWritable getOutLabelValues() {
		return outLabelValues;
	}

	public void setOutLabelValues(LabelValueWritable outLabelValues) {
		this.outLabelValues = outLabelValues;
	}

	public boolean isHasOutEdge() {
		return hasOutEdge;
	}

	public void setHasOutEdge(boolean hasOutEdge) {
		this.hasOutEdge = hasOutEdge;
	}

	public boolean isHasInEdge() {
		return hasInEdge;
	}

	public void setHasInEdge(boolean hasInEdge) {
		this.hasInEdge = hasInEdge;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		hasInEdge = in.readBoolean();
		hasOutEdge = in.readBoolean();
		if(hasInEdge) inLabelValues.readFields(in);
		if(hasOutEdge)outLabelValues.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(hasInEdge);
		out.writeBoolean(hasOutEdge);
		if(hasInEdge) inLabelValues.write(out);
		if(hasOutEdge) outLabelValues.write(out);
	}

}
