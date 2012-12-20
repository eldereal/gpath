package gpath.impl.giraph;

import gpath.impl.hadoop.CompactVertexWritable;
import gpath.impl.hadoop.IntWritableComparable;
import gpath.impl.hadoop.LabelValueWritable;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class GPathNullOutputFormat
		extends
		VertexOutputFormat<IntWritableComparable, LabelValueWritable, GPathEdge> {

	private static class GPathVertexWriter implements VertexWriter<IntWritableComparable, LabelValueWritable, GPathEdge> {

		private final RecordWriter<IntWritableComparable, CompactVertexWritable> writer;
		CompactVertexWritable cv = new CompactVertexWritable();
		int edgeId = 1;
		
		public GPathVertexWriter(
				RecordWriter<IntWritableComparable, CompactVertexWritable> writer) {
			this.writer = writer;
		}

		@Override
		public void writeVertex(
				Vertex<IntWritableComparable, LabelValueWritable, GPathEdge, ?> v)
				throws IOException, InterruptedException {
			cv.reset();
			cv.setId(v.getId().get());
			cv.setLabelValues(v.getValue());
			for(Edge<IntWritableComparable, GPathEdge> edge:v.getEdges()){
				GPathEdge e = edge.getValue();
				if(e.isHasOutEdge()){
					cv.addOutEdgeRaw(edge.getTargetVertexId().get(), -(edgeId++), e.getOutLabelValues());
				}
				if(e.isHasInEdge()){
					cv.addInEdgeRaw(edge.getTargetVertexId().get(), -(edgeId++), e.getOutLabelValues());
				}
			}
			writer.write(v.getId(), cv);			
		}

		@Override
		public void initialize(TaskAttemptContext context) throws IOException {
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			writer.close(context);
		}
	}

	private SequenceFileOutputFormat<IntWritableComparable, CompactVertexWritable> seqFileOutputFormat =
	          new SequenceFileOutputFormat<IntWritableComparable, CompactVertexWritable>();
	
	@Override
	public VertexWriter<IntWritableComparable, LabelValueWritable, GPathEdge> createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		RecordWriter<IntWritableComparable, CompactVertexWritable> recordWriter 
        	= seqFileOutputFormat.getRecordWriter(context);
		return new GPathVertexWriter(recordWriter);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		seqFileOutputFormat.checkOutputSpecs(context);
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return seqFileOutputFormat.getOutputCommitter(context);
	}

}
