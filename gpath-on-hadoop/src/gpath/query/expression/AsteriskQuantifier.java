package gpath.query.expression;

import gpath.query.visitor.Visitor;

public class AsteriskQuantifier extends Quantifier {
	
	public AsteriskQuantifier(){
		
	}
	
	public AsteriskQuantifier(Expression quantified){
		setQuantified(quantified);
	}
	
	@Override
	public String toString() {
		return quantified + "*";
	}

	@Override
	public <TR, TA> TR accept(Visitor<TR, TA> visitor, TA... arguments) {
		return visitor.visitAsteriskQuantifier(this, arguments);
	}

	

	
}
