package gpath.impl.hadoop;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class GPathSequenceFileInputFormat 
	extends SequenceFileInputFormat<IntWritableComparable, CompactVertexWritable> {

}
