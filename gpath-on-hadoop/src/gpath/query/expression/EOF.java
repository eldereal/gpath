package gpath.query.expression;

import gpath.query.visitor.Visitor;

public class EOF extends Entity { 
	
	@Override
	public <TR, TA> TR accept(Visitor<TR, TA> visitor, TA... arguments) {
		throw new RuntimeException("Cannot traverse a pseudo entity");
	}

	@Override
	public String toString() {
		return "#";
	}
	
}
