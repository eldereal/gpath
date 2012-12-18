package gpath.impl.hadoop;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import gpath.graph.Edge;
import gpath.graph.Entity;
import gpath.graph.Vertex;
import gpath.task.AsyncResult;
import gpath.task.CallableAsyncResult;
import gpath.util.WritableHelper;

public class MutableGraph implements gpath.graph.MutableGraph {

	Configuration configuration;
	FileSystem fs;
	
	int maxVertexId = 1;
	int maxEdgeId = 1;
	
	LogWriter writer;
	
	public MutableGraph(Configuration configuration) throws IOException {
		super();
		this.configuration = configuration;
		this.fs = FileSystem.get(configuration);
		writer = new LogWriter(fs, configuration, new Path("gpath/log"));
	}

	@Override
	public AsyncResult<Boolean> commit() {
		return new CallableAsyncResult<Boolean>(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				writer.close();
				return CompactGraphMapReduce.run(configuration, new Path("gpath/log"));				
			}
		});
	}

	@Override
	public AsyncResult<Boolean> abort() {
		return new CallableAsyncResult<Boolean>(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				fs.delete(new Path("gpath/log"), true);
				return !fs.exists(new Path("gpath/log"));
			}			
		});
	}

	@Override
	public int createVertex(String type) throws IOException {
		writer.writeLog(LogWritable.addVertex(maxVertexId, type));
		writer.writeLog(LogWritable.setVertexLabel(maxVertexId, "type", WritableHelper.toBytes(type)));
		return maxVertexId ++;
	}

	@Override
	public int createEdge(String type, int start, int end) throws IOException {
		writer.writeLog(LogWritable.addEdge(-maxEdgeId, type, start, end));
		writer.writeLog(LogWritable.setEdgeLabel(-maxEdgeId, start, end, "type", WritableHelper.toBytes(type)));
		return -(maxEdgeId++);
	}

	@Override
	public int createEdge(String type, Vertex start, Vertex end) throws IOException {
		return createEdge(type, start.getId(), end.getId());
	}

	@Override
	public void setVertexLabel(int entity, String name, byte[] value) throws IOException {
		writer.writeLog(LogWritable.setVertexLabel(entity, name, value));
	}

	@Override
	public void setVertexLabel(Vertex entity, String name, byte[] value) throws IOException {
		setVertexLabel(entity.getId(), name, value);
	}
	
	@Override
	public void setEdgeLabel(int entity, int start, int end, String name, byte[] value) throws IOException {
		writer.writeLog(LogWritable.setEdgeLabel(entity, start, end, name, value));
	}

	@Override
	public void setEdgeLabel(Edge entity, String name, byte[] value) throws IOException {
		setEdgeLabel(entity.getId(), entity.getInVertex().getId(), entity.getOutVertex().getId(), name, value);
	}

	@Override
	public void deleteEntity(Entity e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteLabel(Entity e, String name) {
		// TODO Auto-generated method stub
		
	}

}
