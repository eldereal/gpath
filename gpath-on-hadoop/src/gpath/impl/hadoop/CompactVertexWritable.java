package gpath.impl.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import gpath.graph.Edge;
import gpath.graph.LabelValue;
import gpath.graph.Vertex;
import gpath.util.IterableHelper;
import gpath.util.WritableHelper;

import org.apache.hadoop.io.Writable;

public class CompactVertexWritable implements Writable, Vertex{

	private static final class InnerEdge implements Edge{

		int id;
		LabelValueWritable lvs;
		
		public InnerEdge(int id, LabelValueWritable lvs) {
			super();
			this.id = id;
			this.lvs = lvs;
		}

		@Override
		public int getId() {
			return id;
		}

		@Override
		public String getType() {
			if(lvs.containsKey("type")){
				return WritableHelper.parseString(lvs.get("type"));
			}else{
				return null;
			}
		}

		@Override
		public Iterable<LabelValue> getLabels() {
			return IterableHelper.select(lvs.entrySet(), new IterableHelper.Map<Map.Entry<String, byte[]>, LabelValue>() {

				@Override
				public LabelValue select(final Entry<String, byte[]> element) {
					return new LabelValue() {
						
						@Override
						public byte[] getValue() {
							return element.getValue();
						}
						
						@Override
						public String getName() {
							return element.getKey();
						}
					};
				}
			});
		}
		
		@Override
		public byte[] getLabel(String name) {
			return lvs.get(name);
		}

		@Override
		public int getLabelCount() {
			return lvs.size();
		}

		@Override
		public Vertex getInVertex() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Vertex getOutVertex() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Vertex getOtherVertex(Vertex one) {
			throw new UnsupportedOperationException();
		}
		
	}
	
	private static final IterableHelper.Map<Map.Entry<Integer, EdgeRepresentation>, Edge> innerEdgeConverter = new IterableHelper.Map<Map.Entry<Integer, EdgeRepresentation>, Edge>(){
		@Override
		public Edge select(Entry<Integer, EdgeRepresentation> element) {
			return new InnerEdge(element.getKey(), element.getValue().labels);
		}		
	};
	
	public static final class EdgeRepresentation implements Writable{
		public int otherVertex;
		public LabelValueWritable labels;
		
		public EdgeRepresentation(){
			labels = new LabelValueWritable();
		}
		
//		public EdgeRepresentation(int otherVertex, LabelValueWritable labels) {
//			super();
//			this.otherVertex = otherVertex;
//			this.labels = labels;
//		}

		@Override
		public void readFields(DataInput in) throws IOException {
			otherVertex = in.readInt();
			labels.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(otherVertex);
			labels.write(out);
		}
		
	}
	
	int id;	
	LabelValueWritable lvs;
	
	Map<Integer, EdgeRepresentation> inEdges = new HashMap<Integer, EdgeRepresentation>();
	Map<Integer, EdgeRepresentation> outEdges = new HashMap<Integer, EdgeRepresentation>();
	
	public CompactVertexWritable(){
		lvs = new LabelValueWritable();
	}
	
	@Override
	public int getId() {
		return id;
	}
	
	@Override
	public String getType() {
		if(lvs.containsKey("type")){
			return WritableHelper.parseString(lvs.get("type"));
		}else{
			return null;
		}
	}

	public LabelValueWritable getLabelValues(){
		return lvs;
	}
	
	public void setLabelValues(LabelValueWritable lvs){
		this.lvs = lvs;
	}
	
	@Override
	public Iterable<LabelValue> getLabels() {
		return IterableHelper.select(lvs.entrySet(), new IterableHelper.Map<Map.Entry<String, byte[]>, LabelValue>() {

			@Override
			public LabelValue select(final Entry<String, byte[]> element) {
				return new LabelValue() {
					
					@Override
					public byte[] getValue() {
						return element.getValue();
					}
					
					@Override
					public String getName() {
						return element.getKey();
					}
				};
			}
		});
	}

	@Override
	public byte[] getLabel(String name) {
		return lvs.get(name);
	}

	@Override
	public int getLabelCount() {
		return lvs.size();
	}
	
	public Iterable<Map.Entry<Integer, EdgeRepresentation>> getOutEdgesRaw(){
		return outEdges.entrySet();
	}
	
	public Iterable<Map.Entry<Integer, EdgeRepresentation>> getInEdgesRaw(){
		return inEdges.entrySet();
	}

	@Override
	public Iterable<Edge> getOutEdges() {
		return IterableHelper.select(outEdges.entrySet(), innerEdgeConverter);
	}

	@Override
	public Iterable<Edge> getInEdges() {
		return IterableHelper.select(inEdges.entrySet(), innerEdgeConverter);
	}

	@Override
	public Iterable<Edge> getEdges() {
		return IterableHelper.concat(getOutEdges(), getInEdges());
	}

	@Override
	public int getEdgeCount() {
		return outEdges.size() + inEdges.size();
	}
	
	@Override
	public int getOutEdgeCount() {
		return outEdges.size();
	}

	@Override
	public int getInEdgeCount() {
		return inEdges.size();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		lvs.readFields(in);
		inEdges.clear();
		outEdges.clear();
		int len = in.readInt();
		for (int i = 0; i < len; i++) {
			int key = in.readInt();
			EdgeRepresentation r=new EdgeRepresentation();
			r.readFields(in);
			outEdges.put(key, r);
		}
		len = in.readInt();
		for (int i = 0; i < len; i++) {
			int key = in.readInt();
			EdgeRepresentation r=new EdgeRepresentation();
			r.readFields(in);
			inEdges.put(key, r);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		lvs.write(out);
		out.writeInt(outEdges.size());
		for(Entry<Integer, EdgeRepresentation> e:outEdges.entrySet()){
			out.writeInt(e.getKey());
			e.getValue().write(out);
		}
		out.writeInt(inEdges.size());
		for(Entry<Integer, EdgeRepresentation> e:inEdges.entrySet()){
			out.writeInt(e.getKey());
			e.getValue().write(out);
		}
	}
	
	public void setId(int id){
		this.id = id;
	}

	public void setLabel(String key, byte[] value){
		lvs.put(key, value);
	}
	
	public void addOutEdge(int otherVertex, int edgeId){
		EdgeRepresentation r = new EdgeRepresentation();
		r.otherVertex = otherVertex;
		outEdges.put(edgeId, r);
	}
	
	public void addOutEdgeRaw(int otherVertex, int edgeId, LabelValueWritable lvs){
		EdgeRepresentation r = new EdgeRepresentation();
		r.otherVertex = otherVertex;
		r.labels = lvs;
		outEdges.put(edgeId, r);
	}
	
	public void addInEdge(int otherVertex, int edgeId){
		EdgeRepresentation r = new EdgeRepresentation();
		r.otherVertex = otherVertex;
		inEdges.put(edgeId, r);
	}
	
	public void addInEdgeRaw(int otherVertex, int edgeId, LabelValueWritable lvs){
		EdgeRepresentation r = new EdgeRepresentation();
		r.otherVertex = otherVertex;
		r.labels = lvs;
		inEdges.put(edgeId, r);
	}
	
	public void setOutEdgeLabel(int edgeId, String key, byte[] value){
		EdgeRepresentation r = outEdges.get(edgeId);
		if(r!=null)r.labels.put(key, value);
	}
	
	public void setInEdgeLabel(int edgeId, String key, byte[] value){
		EdgeRepresentation r = inEdges.get(edgeId);
		if(r!=null)r.labels.put(key, value);
	}
	
	public void reset(){
		lvs.clear();
		inEdges.clear();
		outEdges.clear();
	}
}
