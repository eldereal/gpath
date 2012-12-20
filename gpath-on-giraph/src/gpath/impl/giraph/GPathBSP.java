package gpath.impl.giraph;

import gpath.impl.hadoop.CompactVertexWritable;
import gpath.impl.hadoop.IntWritableComparable;
import gpath.task.AsyncResult;
import gpath.task.CallableAsyncResult;
import gpath.util.JarUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class GPathBSP implements Writable {

	//static final Pattern filenamePattern=Pattern.compile("part-r-\\d+");
	
	public abstract void compute(long superstep, GPathVertex vertex, Iterable<GPathMessage> msgIterator) throws IOException;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
	}
	
	private static GiraphJob configureJob(final String name,
			final Configuration conf) throws IOException {
		GiraphJob job = new GiraphJob(conf, name);
		FileSystem fs=FileSystem.get(conf);
		Path outPath=new Path("gpath/output");
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}	
		job.getConfiguration().setVertexClass(GPathVertex.class);
		job.getConfiguration().setVertexInputFormatClass(GPathVertexInputFormat.class);
		job.getConfiguration().setVertexOutputFormatClass(GPathVertexOutputFormat.class);
		job.getConfiguration().setWorkerConfiguration(1, 1, 100);
		Configuration iconf = job.getInternalJob().getConfiguration();
		iconf.setClass("mapred.output.value.class", CompactVertexWritable.class, Object.class);
		iconf.setClass("mapred.output.key.class", IntWritableComparable.class, Object.class);
		iconf.setClass("mapred.mapoutput.value.class", CompactVertexWritable.class, Object.class);
		iconf.setClass("mapred.mapoutput.key.class", IntWritableComparable.class, Object.class);
		JarUtil.SetJobJar(job.getInternalJob(), GPathBSP.class);
		for(FileStatus f:fs.listStatus(new Path("gpath/persist"))){
			if(f.getPath().getName().matches("part-r-\\d+")){
				FileInputFormat.addInputPath(job.getInternalJob(), f.getPath());
			}
		}				
		FileOutputFormat.setOutputPath(job.getInternalJob(), outPath);
		return job;
	}
	
	
	public static AsyncResult<Boolean> runBsp(final String name, final Configuration conf, final Class<? extends GPathBSP> bsp){
		return new CallableAsyncResult<Boolean>(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				Conf.setBSPClass(conf, bsp);
				GiraphJob job = configureJob(name, conf);
				return job.run(true);
			}			
		});
	}
	
	public static AsyncResult<Boolean> runBsp(final String name, final Configuration conf, final GPathBSP bsp){
		return new CallableAsyncResult<Boolean>(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				Conf.setBSPInstance(conf, bsp);
				GiraphJob job = configureJob(name, conf);
				return job.run(true);
			}
		});
	}	
	
}
