package gpath.query.expression;

import gpath.query.visitor.Visitor;

public class QuestionQuantifier extends Quantifier {
	
	@Override
	public String toString() {
		return quantified + "?";
	}

	@Override
	public <TR, TA> TR accept(Visitor<TR, TA> visitor, TA... arguments) {
		return visitor.visitQuestionQuantifier(this, arguments);
	}

}
