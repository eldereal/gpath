package gpath.query.visitor;

import gpath.query.expression.AsteriskQuantifier;
import gpath.query.expression.Concat;
import gpath.query.expression.Condition;
import gpath.query.expression.Edge;
import gpath.query.expression.Expression;
import gpath.query.expression.Order;
import gpath.query.expression.Parallel;
import gpath.query.expression.PlusQuantifier;
import gpath.query.expression.Query;
import gpath.query.expression.QuestionQuantifier;
import gpath.query.expression.Vertex;
import gpath.util.IterableHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TopLevelExpressionVisitor implements Visitor<List<Expression>, Void> {

	@Override
	public List<Expression> visitQuery(Query query, Void... arguments) {
		return query.getExpression().accept(this, arguments);
	}

	@Override
	public List<Expression> visitVertex(Vertex vertex, Void... arguments) {
		List<Expression> r = new ArrayList<Expression>();
		r.add(vertex);
		return r;
	}

	@Override
	public List<Expression> visitEdge(Edge edge, Void... arguments) {
		List<Expression> r = new ArrayList<Expression>();
		r.add(edge);
		return r;
	}

	@Override
	public List<Expression> visitAsteriskQuantifier(AsteriskQuantifier quantifier,
			Void... arguments) {
		List<Expression> r = new ArrayList<Expression>();
		r.add(quantifier);
		return r;
	}

	@Override
	public List<Expression> visitQuestionQuantifier(QuestionQuantifier quantifier,
			Void... arguments) {
		List<Expression> r = new ArrayList<Expression>();
		r.add(quantifier);
		return r;
	}

	@Override
	public List<Expression> visitPlusQuantifier(PlusQuantifier quantifier,
			Void... arguments) {
		List<Expression> r = new ArrayList<Expression>();
		r.add(quantifier);
		return r;
	}

	@Override
	public List<Expression> visitConcat(Concat concat, Void... arguments) {
		List<Expression> r = new ArrayList<Expression>();
		r.addAll(concat.getFirst().accept(this, arguments));
		r.addAll(concat.getSecond().accept(this, arguments));
		return r;
	}

	@Override
	public List<Expression> visitCondition(Condition cond, Void... arguments) {
		return null;
	}

	@Override
	public List<Expression> visitOrder(Order order, Void... arguments) {
		return null;
	}

	@Override
	public List<Expression> visitParallel(Parallel parallel, Void... arguments) {
		List<Expression> r = new ArrayList<Expression>();
		r.add(parallel);
		return r;
	}

}
