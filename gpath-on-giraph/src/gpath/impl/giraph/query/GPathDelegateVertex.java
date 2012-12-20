package gpath.impl.giraph.query;

import java.util.Map;
import java.util.Map.Entry;

import gpath.graph.Edge;
import gpath.graph.LabelValue;
import gpath.graph.Vertex;
import gpath.impl.giraph.GPathEdge;
import gpath.impl.giraph.GPathVertex;
import gpath.impl.hadoop.IntWritableComparable;
import gpath.impl.hadoop.LabelValueWritable;
import gpath.util.IterableHelper;
import gpath.util.WritableHelper;

public class GPathDelegateVertex implements Vertex {

	private class DelegateInEdge implements Edge{

		LabelValueWritable lvs;
		int otherVertex;
		
		public DelegateInEdge(LabelValueWritable lvs, int otherVertex) {
			super();
			this.lvs = lvs;
			this.otherVertex = otherVertex;
		}

		@Override
		public int getId() {
			return 0;
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
			return GPathDelegateVertex.this;
		}

		@Override
		public Vertex getOtherVertex(Vertex one) {
			if(one.getId()==id){
				throw new UnsupportedOperationException();
			}else if(one.getId()==otherVertex){
				return GPathDelegateVertex.this;
			}else{
				throw new IllegalArgumentException();
			}
		}
		
	}
	
	private class DelegateOutEdge implements Edge{

		LabelValueWritable lvs;
		int otherVertex;
		
		public DelegateOutEdge(LabelValueWritable lvs, int otherVertex) {
			super();
			this.lvs = lvs;
			this.otherVertex = otherVertex;
		}

		@Override
		public int getId() {
			return 0;
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
		public Vertex getOutVertex() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Vertex getInVertex() {
			return GPathDelegateVertex.this;
		}

		@Override
		public Vertex getOtherVertex(Vertex one) {
			if(one.getId()==id){
				throw new UnsupportedOperationException();
			}else if(one.getId()==otherVertex){
				return GPathDelegateVertex.this;
			}else{
				throw new IllegalArgumentException();
			}
		}		
	}
	
	private final IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge>, Edge>
	inedge_converter = new IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable,GPathEdge>, Edge>() {

			@Override
			public Edge select(
					org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge> element) {
				return new DelegateInEdge(element.getValue().getInLabelValues(), element.getTargetVertexId().get());
			}
		};
		
	private final IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge>, Edge>
	outedge_converter = new IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable,GPathEdge>, Edge>() {

			@Override
			public Edge select(
					org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge> element) {
				return new DelegateOutEdge(element.getValue().getOutLabelValues(), element.getTargetVertexId().get());
			}
		};
	
	private static final IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge>, Boolean>
	inedge_selector = new IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable,GPathEdge>, Boolean>() {
		@Override
		public Boolean select(
				org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge> element) {
			return element.getValue().isHasInEdge();
		}
	};
	
	private static final IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge>, Boolean>
	outedge_selector = new IterableHelper.Map<org.apache.giraph.graph.Edge<IntWritableComparable,GPathEdge>, Boolean>() {
		@Override
		public Boolean select(
				org.apache.giraph.graph.Edge<IntWritableComparable, GPathEdge> element) {
			return element.getValue().isHasOutEdge();
		}
	};
		
	private GPathVertex v;
	private int id;
	private LabelValueWritable lvs;
	
	public GPathDelegateVertex(GPathVertex v){
		this.v = v;
		this.id = v.getId().get();
		this.lvs = v.getValue();
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
	public Iterable<Edge> getOutEdges() {
		return IterableHelper.select(IterableHelper.where(v.getEdges(), outedge_selector),
				outedge_converter);
	}

	@Override
	public Iterable<Edge> getInEdges() {
		return IterableHelper.select(IterableHelper.where(v.getEdges(), inedge_selector),
				inedge_converter);
	}

	@Override
	public Iterable<Edge> getEdges() {
		return IterableHelper.concat(getOutEdges(), getInEdges());
	}

	@Override
	public int getOutEdgeCount() {
		return IterableHelper.count(IterableHelper.where(v.getEdges(), outedge_selector));
	}

	@Override
	public int getInEdgeCount() {
		return IterableHelper.count(IterableHelper.where(v.getEdges(), inedge_selector));
	}

	@Override
	public int getEdgeCount() {
		return getOutEdgeCount() + getInEdgeCount();
	}

}
