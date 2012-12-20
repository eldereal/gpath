package gpath.query.expression;

import gpath.query.condition.AbstractCondition;
import gpath.query.condition.AbstractValue;
import gpath.query.condition.EqualTo;
import gpath.query.condition.LargerThan;
import gpath.query.condition.LargerThanOrEqualTo;
import gpath.query.condition.LessThan;
import gpath.query.condition.LessThanOrEqualTo;
import gpath.query.condition.NotEqualTo;
import gpath.query.visitor.Visitor;

public class Condition extends Expression {
	
	String label;
	String op;
	AbstractValue value;
	
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getOp() {
		return op;
	}
	public void setOp(String op) {
		this.op = op;
	}
	public AbstractValue getValue() {
		return value;
	}
	public void setValue(AbstractValue value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "["+label+op+value+"]";
	}
	
	@Override
	public <TR, TA> TR accept(Visitor<TR, TA> visitor, TA... arguments) {
		return visitor.visitCondition(this, arguments);
	}
	
	public AbstractCondition getCondition(){
		if(op.equals("=")){
			return new EqualTo(getLabel(), getValue());
		}else if(op.equals("<>")){
			return new NotEqualTo(getLabel(), getValue());
		}else if(op.equals("<=")){
			return new LessThanOrEqualTo(getLabel(), getValue());
		}else if(op.equals("<")){
			return new LessThan(getLabel(), getValue());
		}else if(op.equals(">=")){
			return new LargerThanOrEqualTo(getLabel(), getValue());
		}else if(op.equals(">")){
			return new LargerThan(getLabel(), getValue());
		}else{
			throw new IllegalArgumentException(op);
		}
	}
}
