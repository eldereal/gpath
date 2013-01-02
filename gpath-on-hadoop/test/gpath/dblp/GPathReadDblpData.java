package gpath.dblp;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gpath.impl.hadoop.CompactGraphMapReduce;
import gpath.impl.hadoop.CompactVertexWritable;
import gpath.impl.hadoop.Conf;
import gpath.impl.hadoop.GPathLogSequenceFileInputFormat;
import gpath.impl.hadoop.GPathSequenceFileOutputFormat;
import gpath.impl.hadoop.IntWritableComparable;
import gpath.impl.hadoop.LogWritable;
import gpath.impl.hadoop.CompactGraphMapReduce.CompactGraphMapper;
import gpath.impl.hadoop.CompactGraphMapReduce.CompactGraphReducer;
import gpath.util.JarUtil;
import gpath.util.WritableHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class GPathReadDblpData {

	public static class GPathReadDblpMapper extends Mapper<LongWritable, Text, IntWritableComparable, LogWritable>{
		
		private static final Pattern NodePattern = Pattern.compile("^Persion,(\\d+),(.*?)$");
		private static final Pattern EdgePattern = Pattern.compile("^Publication,(\\d+),(.*?)((,\\d+)*)$");
		
		IntWritableComparable integer = new IntWritableComparable();
		
		@Override
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,IntWritableComparable,LogWritable>.Context context) throws IOException ,InterruptedException {
			Matcher m;
			if((m = NodePattern.matcher(value.toString())).matches()){
				int id = Integer.parseInt(m.group(1));
				String name = m.group(2);
				context.write(integer.set(id), LogWritable.addVertex(id, "Person"));
				context.write(integer.set(id), LogWritable.setVertexLabel(id, "name", WritableHelper.toBytes(name)));
			}else if((m = EdgePattern.matcher(value.toString())).matches()){
				int id = Integer.parseInt(m.group(1));
				String name = m.group(2);
				String nodelist = m.group(3);
				String[] nodes = nodelist.substring(1).split(",");
				if(nodes.length>=2){
					for(int i = 0;i<nodes.length;i++){
						for (int j = i + 1; j < nodes.length; j++) {
							int id1 = Integer.parseInt(nodes[i]);
							int id2 = Integer.parseInt(nodes[j]);
							LogWritable log = LogWritable.addEdge(id, "Publication", id1, id2);
							context.write(integer.set(id1), log);
							context.write(integer.set(id2), log);
							log = LogWritable.setEdgeLabel(id, id1, id2, "name", WritableHelper.toBytes(name));
							context.write(integer.set(id1), log);
							context.write(integer.set(id2), log);
						}
					}
				}
			}
		}
	}
	
	static boolean run(Configuration configuration) throws IOException{
		FileSystem fs = FileSystem.get(configuration);
		if(fs.exists(new Path("gpath/persist"))){
			fs.delete(new Path("gpath/persist"), true);
		}
		Job job = new Job(configuration);
		job.setMapperClass(GPathReadDblpMapper.class);
		job.setReducerClass(CompactGraphReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(GPathSequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritableComparable.class);
		job.setOutputValueClass(CompactVertexWritable.class);
		job.setMapOutputKeyClass(IntWritableComparable.class);
		job.setMapOutputValueClass(LogWritable.class);
		job.setJobName("GPath mutation task");
		JarUtil.SetJobJar(job, GPathReadDblpMapper.class);
		SequenceFileInputFormat.addInputPath(job, new Path("dblp/flatten.txt"));
		SequenceFileOutputFormat.setOutputPath(job, new Path("gpath/persist"));
		
		try {
			return job.waitForCompletion(false);
		} catch (InterruptedException e) {
			return false;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration=Conf.getDefaultConfiguration("1.0.2");
		GPathReadDblpData.run(configuration);
	}

}
