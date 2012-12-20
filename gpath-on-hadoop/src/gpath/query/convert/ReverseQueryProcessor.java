package gpath.query.convert;

import gpath.exception.BadQueryException;
import gpath.store.StatisticsGraphStore;
import gpath.exception.HdglException;
import gpath.query.condition.AbstractCondition;
import gpath.query.condition.BinaryCondition;
import gpath.query.condition.Conjunction;
import gpath.query.condition.EqualTo;
import gpath.query.condition.LargerThan;
import gpath.query.condition.LargerThanOrEqualTo;
import gpath.query.condition.LessThan;
import gpath.query.condition.LessThanOrEqualTo;
import gpath.query.condition.NotEqualTo;
import gpath.query.expression.Condition;
import gpath.query.expression.Expression;
import gpath.query.expression.Vertex;
import gpath.query.parser.QueryLexer;
import gpath.query.parser.QueryParser;
import gpath.query.stm.SimpleStateMachine;
import gpath.query.stm.StateMachine;
import gpath.query.stm.StateMachine.TransitionType;
import gpath.query.visitor.FindFirstVertexVisitor;
import gpath.query.visitor.FindLastVertexVisitor;
import gpath.query.visitor.ReverseCloneVisitor;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;

public class ReverseQueryProcessor implements QueryProcessor {

	StatisticsGraphStore graph;
	
	public ReverseQueryProcessor(StatisticsGraphStore graph){
		this.graph = graph;
	}
	
	long estimate(Condition[] cond, StatisticsGraphStore graph){
		long est;
		double frac=1;
		long total=graph.getVertexCount();
		if(cond==null){
			return total;
		}
		for(Condition sc:cond){
			frac *= estimate(sc.getCondition(), graph)/(double)total;
		}
		est = (long) (total*frac);
		if(est<=0)est=1;
		if(est>graph.getVertexCount())est=graph.getVertexCount();
		return est;
	}
	
	long estimate(AbstractCondition cond, StatisticsGraphStore graph){
		long est;
		if(cond instanceof EqualTo){
			byte[] val=((EqualTo) cond).getValue().toBytes();
			est = graph.getVertexStatistics(((EqualTo) cond).getLabel())
					.estimate(val, val);
		}else if(cond instanceof NotEqualTo){
			byte[] val=((NotEqualTo) cond).getValue().toBytes();
			est = graph.getVertexCount() - graph.getVertexStatistics(((EqualTo) cond).getLabel())
					.estimate(val, val);
		}else if(cond instanceof LessThan || cond instanceof LessThanOrEqualTo){
			byte[] val=((BinaryCondition) cond).getValue().toBytes();
			est = graph.getVertexStatistics(((BinaryCondition) cond).getLabel())
					.estimate(null, val);
		}else if(cond instanceof LargerThan || cond instanceof LargerThanOrEqualTo){
			byte[] val=((BinaryCondition) cond).getValue().toBytes();
			est = graph.getVertexStatistics(((BinaryCondition) cond).getLabel())
					.estimate(null, val);
		}else if(cond instanceof Conjunction){
			double frac=1;
			long total=graph.getVertexCount();
			for(AbstractCondition sc:((Conjunction) cond).getConditions()){
				frac *= estimate(sc, graph)/(double)total;
			}
			est = (long) (total*frac);
		}else{
			est=graph.getVertexCount();
		}
		if(est<=0)est=1;
		if(est>graph.getVertexCount())est=graph.getVertexCount();
		return est;
	}
	
	@Override
	public StateMachine compile(String query) throws BadQueryException {
		try{
			QueryLexer lexer = new QueryLexer(new ANTLRStringStream(query));
			QueryParser parser = new QueryParser(new TokenRewriteStream(lexer));
			Expression q = QueryCompletion.complete(parser.expression());
			StateMachine fstm = processExpression(q);
			return fstm;
		}catch (RecognitionException e) {
			throw new BadQueryException(query, e);
		}
	}

	public StateMachine processExpression(Expression q) throws HdglException {
		Vertex f = q.accept(new FindFirstVertexVisitor());
		Vertex l = q.accept(new FindLastVertexVisitor());
		long fest,lest;
		if(f == null){
			fest = graph.getVertexCount();
		}else{
			fest = estimate(f.getConditions(), graph);
		}
		if(l == null){
			lest = graph.getVertexCount();
		}else{
			lest = estimate(l.getConditions(), graph);
		}
		boolean rev=(lest<fest);
		return makeStm(q, rev);
	}

	public StateMachine makeStm(Expression q, boolean rev) throws HdglException {
		if(rev){
			q = q.accept(new ReverseCloneVisitor());
		}
		SimpleStateMachine stm = QueryToStateMachine.convert(q);
		StateMachine fstm = stm.buildStateMachine();
		if(rev){
			for(StateMachine.State s:fstm.getStates()){
				for(StateMachine.Condition c:s.getConditions()){
					for(StateMachine.Transition t:c.getTransitions()){
						if(t.getType()==TransitionType.In){
							throw new HdglException("unexpected state");
						}else if(t.getType()==TransitionType.Out){
							t.setType(TransitionType.In);
						}else if(t.getType()==TransitionType.OutSuccess){
							t.setType(TransitionType.InSuccess);
						}
					}
				}
			}
		}
		return fstm;
	}
}
