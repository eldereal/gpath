package gpath.query.condition;

import gpath.graph.Entity;
import gpath.graph.Vertex;
import gpath.util.IterableHelper;

public class LargerThan extends BinaryCondition {
	
	public static final byte FLAG_BYTE=-3;
	
	@Override
	byte getFlagByte() {
		return FLAG_BYTE;
	}
	
	public LargerThan() {
		
	}
	
	public LargerThan(String label, AbstractValue value) {
		super(label, value);
	}
	
	@Override
	public String toString() {
		return getLabel()+">"+getValue();
	}

	@Override
	public boolean require(AbstractCondition other) {
		if(other instanceof NoRestriction){
			return true;
		}else if(other instanceof BinaryCondition){
			if(other instanceof LargerThan){
				return ((LargerThan) other).getValue().lessThanOrEqualTo(getValue());
			}else if(other instanceof LargerThanOrEqualTo){
				return ((LargerThanOrEqualTo) other).getValue().lessThanOrEqualTo(getValue());
			}else if(other instanceof NotEqualTo){
				return ((NotEqualTo) other).getValue().lessThanOrEqualTo(getValue());
			}else{
				return false;
			}
//		}else if(other instanceof Conjunction){
//			for (AbstractCondition condition : ((Conjunction) other).getConditions()) {
//				if(!require(condition)){
//					return false;
//				}
//			}
//			return true;
		}else{
			return false;
		}
	}

	@Override
	public boolean compatible(AbstractCondition other) {
		if(other instanceof EqualTo){
			return getValue().lessThan(((EqualTo) other).getValue());
		}else if(other instanceof LessThan){
			return getValue().lessThan(((LessThan) other).getValue());
		}else if(other instanceof LessThanOrEqualTo){
			return getValue().lessThan(((LessThanOrEqualTo) other).getValue());
		}else{
			return true;
		}
	}

	@Override
	public boolean test(Entity e) {
		if(getLabel().equalsIgnoreCase("id")){
			return getValue() instanceof IntNumberValue &&
					e.getId() > ((IntNumberValue)getValue()).getValue();
		}else if(getLabel().equalsIgnoreCase("degree")){
			return getValue() instanceof IntNumberValue &&
					e instanceof Vertex &&
					IterableHelper.count(((Vertex)e).getEdges()) > ((IntNumberValue)getValue()).getValue();
		}else if(getLabel().equalsIgnoreCase("indegree")){
			return getValue() instanceof IntNumberValue &&
					e instanceof Vertex &&
					IterableHelper.count(((Vertex)e).getInEdges()) > ((IntNumberValue)getValue()).getValue();
		}else if(getLabel().equalsIgnoreCase("outdegree")){
			return getValue() instanceof IntNumberValue &&
					e instanceof Vertex &&
					IterableHelper.count(((Vertex)e).getOutEdges()) > ((IntNumberValue)getValue()).getValue();
		}else{
			return getValue().lessThan(e.getLabel(getLabel()));
		}
	}
}
