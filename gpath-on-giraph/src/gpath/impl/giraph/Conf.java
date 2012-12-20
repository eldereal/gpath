package gpath.impl.giraph;

import java.io.IOException;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Conf {

	public static final String GPATH_BSP_CLASS = "gpath.bsp.class";
	public static final String GPATH_BSP_INSTANCE = "gpath.bsp.instance";

	public static Configuration getDefaultConfiguration(String env){
		Configuration conf = gpath.impl.hadoop.Conf.getDefaultConfiguration(env);
		conf.set(GiraphConfiguration.ZOOKEEPER_LIST, "localhost:2181");
		if(env.equalsIgnoreCase("local")){
			conf.setBoolean(GiraphConfiguration.SPLIT_MASTER_WORKER, false);
		}else if(env.equalsIgnoreCase("local-mapreduce")){
			conf.setBoolean(GiraphConfiguration.SPLIT_MASTER_WORKER, false);
		}
		return conf;
	}
	
	public static GPathBSP getBSPInstance(Configuration conf)
			throws IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		String instance;
		String className;
		if ((instance = conf.get(GPATH_BSP_INSTANCE)) != null) {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream inputStream = null;
			try {
				inputStream = fs.open(new Path(instance));
				className = inputStream.readUTF();
				Class<? extends GPathBSP> clazz = Class.forName(className)
						.asSubclass(GPathBSP.class);
				GPathBSP bsp = clazz.newInstance();
				bsp.readFields(inputStream);
				return bsp;
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}
		} else if ((className = conf.get(GPATH_BSP_CLASS)) != null) {
			Class<? extends GPathBSP> clazz = Class.forName(className)
					.asSubclass(GPathBSP.class);
			return clazz.newInstance();
		} else {
			throw new IllegalArgumentException(GPATH_BSP_CLASS + " or "
					+ GPATH_BSP_INSTANCE + " must be configured.");
		}
	}

	public static void setBSPClass(Configuration conf,
			Class<? extends GPathBSP> clazz) {
		conf.set(GPATH_BSP_CLASS, clazz.getName());
	}

	public static void setBSPInstance(Configuration conf, GPathBSP instance)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("gpath/.bsp_" + Math.random());
		conf.set(GPATH_BSP_INSTANCE, path.toString());
		FSDataOutputStream out = null;
		try {
			out = fs.create(path, true);
			out.writeUTF(instance.getClass().getName());
			instance.write(out);
		} finally {
			if (out != null)
				out.close();
		}
	}
}
