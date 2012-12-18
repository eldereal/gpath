package gpath.impl.hadoop;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class GPathSequenceFileOutputFormat extends
		SequenceFileOutputFormat<IntWritableComparable, CompactVertexWritable> {

}
