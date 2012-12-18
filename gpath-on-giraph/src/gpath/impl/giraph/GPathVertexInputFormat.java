package gpath.impl.giraph;

import gpath.impl.hadoop.IntWritableComparable;
import gpath.impl.hadoop.LabelValueWritable;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.lib.SequenceFileVertexInputFormat;

public class GPathVertexInputFormat 
	extends SequenceFileVertexInputFormat<IntWritableComparable, LabelValueWritable, LabelValueWritable, GPathMessage, BasicVertex<IntWritableComparable, LabelValueWritable, LabelValueWritable, GPathMessage>>{

}
