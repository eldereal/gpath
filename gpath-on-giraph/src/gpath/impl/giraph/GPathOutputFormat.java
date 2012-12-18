package gpath.impl.giraph;

import gpath.impl.hadoop.IntWritableComparable;
import gpath.impl.hadoop.LabelValueWritable;

import java.io.IOException;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.SequenceFileVertexOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class GPathOutputFormat
		extends
		SequenceFileVertexOutputFormat<IntWritableComparable, LabelValueWritable, LabelValueWritable, BasicVertex<IntWritableComparable, LabelValueWritable, LabelValueWritable, ?>> {

	private static class GPathSequenceFileWriter
			extends
			SequenceFileVertexWriter<IntWritableComparable, LabelValueWritable, LabelValueWritable, BasicVertex<IntWritableComparable, LabelValueWritable, LabelValueWritable, ?>> {

		public GPathSequenceFileWriter(
				RecordWriter<IntWritableComparable, BasicVertex<IntWritableComparable, LabelValueWritable, LabelValueWritable, ?>> writer) {
			super(writer);			
		}

		@Override
		public void writeVertex(
				BasicVertex<IntWritableComparable, LabelValueWritable, LabelValueWritable, ?> v)
				throws IOException, InterruptedException {
			getRecordWriter().write(v.getVertexId(), v);			
		}
	}

	@Override
	public VertexWriter<IntWritableComparable, LabelValueWritable, LabelValueWritable> createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		RecordWriter<IntWritableComparable, BasicVertex<IntWritableComparable, LabelValueWritable, LabelValueWritable, ?>> recordWriter 
        	= seqFileOutputFormat.getRecordWriter(context);
		return new GPathSequenceFileWriter(recordWriter);
	}

}
