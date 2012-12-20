package gpath.impl.giraph;

import gpath.impl.hadoop.IntWritableComparable;
import gpath.impl.hadoop.LabelValueWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.EdgeListVertex;

public class GPathVertex extends EdgeListVertex<IntWritableComparable, LabelValueWritable, GPathEdge, GPathMessage> {

	GPathBSP bsp;
	
	public GPathVertex(GPathBSP bsp){
		this.bsp = bsp;
	}
	
	@Override
	public void compute(Iterable<GPathMessage> msgIterator) throws IOException {
		bsp.compute(getSuperstep(), this, msgIterator);
	}

}
