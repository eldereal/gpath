package gpath.impl.hadoop;

import gpath.util.JarUtil;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.ToolRunner;

public class DblpGen {

	class DblpXmlMapper implements Mapper<LongWritable, Text, IntWritable, LogWritable>{

		@Override
		public void configure(JobConf conf) {
			
		}

		@Override
		public void close() throws IOException {
			
		}

		@Override
		public void map(LongWritable line, Text xml,
				OutputCollector<IntWritable, LogWritable> out,
				Reporter reporter) throws IOException {
			
		}
		
	}
	
	class DblpXmlReducer implements Reducer<IntWritable, LogWritable, IntWritableComparable, CompactVertexWritable>{

		CompactVertexWritable v;
		IntWritableComparable id;
		
		@Override
		public void configure(JobConf conf) {			
			v = new CompactVertexWritable();
			id = new IntWritableComparable();
		}

		@Override
		public void close() throws IOException {
			
		}

		@Override
		public void reduce(
				IntWritable key,
				Iterator<LogWritable> logs,
				OutputCollector<IntWritableComparable, CompactVertexWritable> out,
				Reporter reporter) throws IOException {
			v.reset();
			int id = key.get();
			v.setId(id);
			boolean added = false;
			for (Iterator<LogWritable> iterator = logs; iterator.hasNext();) {
				LogWritable logWritable = iterator.next();
				switch (logWritable.type) {
				case LogWritable.ADD_VERTEX:
					added = true;
					break;
				case LogWritable.ADD_EDGE:
					if(id==logWritable.id2){//this is starter node
						v.addOutEdge(logWritable.id3, logWritable.id1);
					}else if(id==logWritable.id3){
						v.addInEdge(logWritable.id2, logWritable.id1);
					}
					break;
				case LogWritable.SET_EDGE_LABEL:
					if(id==logWritable.id2){//this is starter node
						v.setOutEdgeLabel(logWritable.id1, logWritable.name, logWritable.data);						
					}else if(id==logWritable.id3){
						v.setInEdgeLabel(logWritable.id1, logWritable.name, logWritable.data);
					}
					break;
				case LogWritable.SET_VERTEX_LABEL:
					v.setLabel(logWritable.name, logWritable.data);
					break;
				}
			}
			if(added){
				this.id.set(id);
				out.collect(this.id, v);
			}			
		}	
	}
	
	public static void main(String[] args) throws IOException {
		JobConf jobConf = new JobConf(Conf.getDefaultConfiguration("local-mapreduce"));
		//jobConf.setInputFormat(XmlInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);
		jobConf.setMapperClass(DblpXmlMapper.class);
		jobConf.setReducerClass(DblpXmlReducer.class);
		jobConf.setMapOutputKeyClass(IntWritable.class);
		jobConf.setMapOutputValueClass(LogWritable.class);
		jobConf.setOutputKeyClass(IntWritableComparable.class);
		jobConf.setOutputValueClass(CompactVertexWritable.class);
		JarUtil.SetJobJar(jobConf, DblpGen.class);
		FileInputFormat.setInputPaths(jobConf, new Path("dblp/dblp-data.xml"));
		FileOutputFormat.setOutputPath(jobConf, new Path("gpath/persist"));
		JobClient.runJob(jobConf).waitForCompletion();
	}	
}


