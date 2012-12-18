package gpath.impl.giraph;

import gpath.impl.hadoop.IntWritableComparable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.hadoop.io.MapWritable;

public class GPathVertex extends EdgeListVertex<IntWritableComparable, MapWritable, MapWritable, GPathMessage> {

	@Override
	public void compute(Iterator<GPathMessage> msgIterator) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
