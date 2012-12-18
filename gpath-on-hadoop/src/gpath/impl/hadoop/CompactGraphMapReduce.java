package gpath.impl.hadoop;

import gpath.util.JarUtil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class CompactGraphMapReduce {

	private static class CompactGraphMapper extends Mapper<NullWritable, LogWritable, IntWritableComparable, LogWritable>{
		
		IntWritableComparable i=new IntWritableComparable();
		
		protected void map(NullWritable key, LogWritable value, org.apache.hadoop.mapreduce.Mapper<NullWritable,LogWritable,IntWritableComparable,LogWritable>.Context context) throws java.io.IOException ,InterruptedException {
			switch (value.type) {
			case LogWritable.ADD_VERTEX:
				i.set(value.id1);
				context.write(i, value);
				break;
			case LogWritable.ADD_EDGE:
				i.set(value.id2);
				context.write(i, value);
				i.set(value.id3);
				context.write(i, value);
				break;
			case LogWritable.SET_VERTEX_LABEL:
				i.set(value.id1);
				context.write(i, value);
				break;
			case LogWritable.SET_EDGE_LABEL:
				i.set(value.id2);
				context.write(i, value);
				i.set(value.id3);
				context.write(i, value);
				break;
			default:				
				return;
			}			
		};
	}

	private static class CompactGraphReducer extends Reducer<IntWritableComparable, LogWritable, IntWritableComparable, CompactVertexWritable>{
		
		CompactVertexWritable v = new CompactVertexWritable();
		
		protected void reduce(IntWritableComparable key, java.lang.Iterable<LogWritable> values, org.apache.hadoop.mapreduce.Reducer<IntWritableComparable,LogWritable,IntWritableComparable,CompactVertexWritable>.Context ctx) throws java.io.IOException ,InterruptedException {
			v.reset();
			int id = key.get();
			v.setId(id);
			boolean added = false;
			for (LogWritable logWritable : values) {
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
				ctx.write(key, v);
			}
		};
		
	}

	static boolean run(Configuration configuration, Path logfile) throws IOException{
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(new Path("gpath/persist"))){
			fs.delete(new Path("gpath/persist"), true);
		}
		Job job = new Job(configuration);
		job.setMapperClass(CompactGraphMapper.class);
		job.setReducerClass(CompactGraphReducer.class);
		job.setInputFormatClass(GPathLogSequenceFileInputFormat.class);
		job.setOutputKeyClass(IntWritableComparable.class);
		job.setOutputValueClass(CompactVertexWritable.class);
		job.setOutputFormatClass(GPathSequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(IntWritableComparable.class);
		job.setMapOutputValueClass(LogWritable.class);
		job.setJobName("GPath mutation task");
		JarUtil.SetJobJar(job, CompactGraphMapReduce.class);
		SequenceFileInputFormat.addInputPath(job, logfile);
		SequenceFileOutputFormat.setOutputPath(job, new Path("gpath/persist"));
		
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			return job.isSuccessful();
		} catch (ClassNotFoundException e) {
			return false;
		}
		return job.isSuccessful();
	}
}
