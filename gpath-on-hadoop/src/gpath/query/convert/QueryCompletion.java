package gpath.query.convert;

import gpath.query.expression.Expression;
import gpath.query.visitor.CompleteQueryVisitor;

public class QueryCompletion {

	public static Expression complete(Expression expression){
		return CompleteQueryVisitor.complete(expression);
	}
	
}
