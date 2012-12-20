package gpath.query.visitor;

import gpath.query.expression.AsteriskQuantifier;
import gpath.query.expression.Concat;
import gpath.query.expression.Condition;
import gpath.query.expression.Edge;
import gpath.query.expression.Order;
import gpath.query.expression.Parallel;
import gpath.query.expression.PlusQuantifier;
import gpath.query.expression.Query;
import gpath.query.expression.QuestionQuantifier;
import gpath.query.expression.Vertex;

public class FindFirstVertexVisitor implements Visitor<Vertex, Void> {

	@Override
	public Vertex visitQuery(Query query, Void... arguments) {
		return query.getExpression().accept(this, arguments);
	}

	@Override
	public Vertex visitVertex(Vertex vertex, Void... arguments) {
		return vertex;
	}

	@Override
	public Vertex visitEdge(Edge edge, Void... arguments) {
		return null;
	}

	@Override
	public Vertex visitAsteriskQuantifier(AsteriskQuantifier quantifier,
			Void... arguments) {
		return null;
	}

	@Override
	public Vertex visitQuestionQuantifier(QuestionQuantifier quantifier,
			Void... arguments) {
		return null;
	}

	@Override
	public Vertex visitPlusQuantifier(PlusQuantifier quantifier,
			Void... arguments) {
		return quantifier.getQuantified().accept(this, arguments);
	}

	@Override
	public Vertex visitConcat(Concat concat, Void... arguments) {
		return concat.getFirst().accept(this, arguments);
	}

	@Override
	public Vertex visitCondition(Condition cond, Void... arguments) {
		return null;
	}

	@Override
	public Vertex visitOrder(Order order, Void... arguments) {
		return null;
	}

	@Override
	public Vertex visitParallel(Parallel parallel, Void... arguments) {
		return null;
	}

}
