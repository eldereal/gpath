package gpath.impl.giraph;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gpath.impl.hadoop.CompactVertexWritable;
import gpath.impl.hadoop.IntWritableComparable;
import gpath.impl.hadoop.LabelValueWritable;
import gpath.impl.hadoop.CompactVertexWritable.EdgeRepresentation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexInputFormat;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class GPathVertexInputFormat
		extends
		VertexInputFormat<IntWritableComparable, LabelValueWritable, GPathEdge, GPathMessage> {

	static class GPathVertexReader
			implements
			VertexReader<IntWritableComparable, LabelValueWritable, GPathEdge, GPathMessage> {

		private final RecordReader<IntWritableComparable, CompactVertexWritable> recordReader;
		private final GPathBSP bsp;
		
		public GPathVertexReader(
				RecordReader<IntWritableComparable, CompactVertexWritable> recordReader,
				GPathBSP bsp) {
			this.recordReader = recordReader;
			this.bsp = bsp;
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			recordReader.initialize(inputSplit, context);
		}

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return recordReader.nextKeyValue();
		}

		@Override
		public Vertex<IntWritableComparable, LabelValueWritable, GPathEdge, GPathMessage> getCurrentVertex()
				throws IOException, InterruptedException {
			IntWritableComparable vid = recordReader.getCurrentKey();
			CompactVertexWritable cv = recordReader.getCurrentValue();
			Map<IntWritableComparable, GPathEdge> edges=new HashMap<IntWritableComparable, GPathEdge>();
			for (Map.Entry<Integer, EdgeRepresentation> e : cv.getOutEdgesRaw()) {
				GPathEdge edge = new GPathEdge();
				edge.setHasOutEdge(true);
				edge.setOutLabelValues(e.getValue().labels);
				edges.put(new IntWritableComparable(e.getValue().otherVertex), edge);
			}
			for (Map.Entry<Integer, EdgeRepresentation> e : cv.getInEdgesRaw()) {
				GPathEdge edge;
				IntWritableComparable othervid = new IntWritableComparable(
						e.getValue().otherVertex);
				if (edges.containsKey(othervid)) {
					edge = edges.get(othervid);					
				} else {
					edge = new GPathEdge();
					edges.put(othervid, edge);
				}
				edge.setHasInEdge(true);
				edge.setInLabelValues(e.getValue().labels);
			}
			GPathVertex v = new GPathVertex(bsp);
			v.initialize(vid, cv.getLabelValues(), edges);
			return v;
		}

		@Override
		public void close() throws IOException {
			recordReader.close();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return recordReader.getProgress();
		}
	}

	protected SequenceFileInputFormat<IntWritableComparable, CompactVertexWritable> sequenceFileInputFormat = new SequenceFileInputFormat<IntWritableComparable, CompactVertexWritable>();

	GPathBSP bspInstance;
	
	@Override
	public List<InputSplit> getSplits(JobContext context, int numWorkers)
			throws IOException, InterruptedException {
		return sequenceFileInputFormat.getSplits(context);
	}

	@Override
	public VertexReader<IntWritableComparable, LabelValueWritable, GPathEdge, GPathMessage> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		if(bspInstance==null){
			Configuration configuration = context.getConfiguration();
			try{
				bspInstance = Conf.getBSPInstance(configuration);
			}catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		return new GPathVertexReader(
				sequenceFileInputFormat.createRecordReader(split, context), bspInstance);
	}

}
