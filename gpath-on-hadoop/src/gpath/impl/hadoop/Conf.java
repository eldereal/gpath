package gpath.impl.hadoop;

import org.apache.hadoop.conf.Configuration;

public class Conf {

	public static Configuration getDefaultConfiguration(String env){
		Configuration conf=new Configuration();
		if(env.startsWith("0.23")||env.startsWith("2.")){
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			conf.set("yarn.resourcemanager.address", "localhost:18040");
			conf.set("mapreduce.framework.name", "yarn");
		}else if(env.startsWith("0.20")||env.startsWith("1.")){
			conf.set("fs.default.name", "hdfs://localhost:9000");
			conf.set("mapred.job.tracker", "localhost:9001");
		}else if(env.equalsIgnoreCase("local")){
			
		}else if(env.equalsIgnoreCase("local-mapreduce")){
			conf.set("fs.default.name", "hdfs://localhost:9000");
		}else{
			throw new IllegalArgumentException("Unrecognized environment: "+env);
		}
		return conf;
	}
	
}
