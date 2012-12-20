package gpath.query.convert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import gpath.query.expression.Entity;
import gpath.query.expression.Expression;
import gpath.query.expression.Vertex;
import gpath.query.parser.QueryLexer;
import gpath.query.parser.QueryParser;
import gpath.query.stm.SimpleStateMachine;
import gpath.query.stm.StateMachine;
import gpath.query.stm.StateMachine.Transition;
import gpath.query.stm.StateMachine.TransitionType;
import gpath.query.visitor.FindFirstVertexVisitor;
import gpath.query.visitor.FindLastVertexVisitor;
import gpath.query.visitor.ReverseCloneVisitor;
import gpath.query.visitor.TopLevelExpressionVisitor;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;

public class JumpQueryProcessor implements QueryProcessor {

	StatisticsGraphStore graph;
	
	ReverseQueryProcessor reverseQueryProcessor;
	
	public JumpQueryProcessor(StatisticsGraphStore graph){
		this.graph = graph;
		reverseQueryProcessor = new ReverseQueryProcessor(graph);
	}
	
	long estimate(Expression expression, StatisticsGraphStore graph){
		return estimate(expression.accept(new TopLevelExpressionVisitor()), graph);
	}
	
	long estimate(Iterable<Expression> expression, StatisticsGraphStore graph){
		long est;
		double frac=1;
		long total=graph.getVertexCount();
		if(expression==null){
			return total;
		}
		for(Expression e:expression){
			if(e instanceof Entity){
				frac *= estimate(((Entity) e).getConditions(), graph)/(double)total;
			}
		}
		est = (long) (total*frac);
		if(est<=0)est=1;
		if(est>graph.getVertexCount())est=graph.getVertexCount();
		return est;
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
			List<Expression> splits = q.accept(new TopLevelExpressionVisitor());
			long min_est = Long.MAX_VALUE;
			Vertex split = null;
			for(Expression v:splits){
				if(v instanceof Vertex){
					long e = (estimate(((Vertex)v).getConditions(), graph));
					if(e < min_est){
						min_est = e;
						split = ((Vertex)v);
					}
				}
			}
			if(split == null){
				return reverseQueryProcessor.processExpression(q);
			}
			int splitindex = splits.indexOf(split);
			if(splitindex==0){
				return reverseQueryProcessor.makeStm(q, false);
			}else if(splitindex==splits.size()-1){
				return reverseQueryProcessor.makeStm(q, true);
			}
			Expression left = null,right =null;
			for(int i=0;i<=splitindex;i++){
				if(left==null){
					left = splits.get(i);
				}else{
					left = Expression.buildConcat(left, splits.get(i));
				}
			}
			for(int i=splitindex;i<splits.size();i++){
				if(right==null){
					right = splits.get(i);
				}else{
					right = Expression.buildConcat(right, splits.get(i));
				}
			}
			long lest = estimate(splits.subList(0, splitindex), graph);
			long rest = estimate(splits.subList(splitindex + 1, splits.size()), graph);
			boolean rev=lest>rest;
			
			StateMachine lstm = reverseQueryProcessor.makeStm(Expression.buildQuery(left), true);
			StateMachine rstm = reverseQueryProcessor.makeStm(Expression.buildQuery(right), false);
			return rev?concat(rstm, lstm):concat(lstm, rstm);
		}catch (RecognitionException e) {
			throw new BadQueryException(query, e);
		}
	}
	
	public StateMachine concat(StateMachine m1, StateMachine m2){
		for(StateMachine.State s:m1.getStates()){
			for(gpath.query.stm.StateMachine.Condition c:s.getConditions()){
				for(Transition t:c.getTransitions()){
					if(t.getType()==TransitionType.InSuccess||t.getType()==TransitionType.OutSuccess){
						t.setType(TransitionType.Backtrack);
						//t.setToState(newid.get(m2.getStartState()));
					}
				}
			}
		}
		int m2id = 1;
		Map<Integer, Integer> newid=new HashMap<Integer, Integer>();
		for(StateMachine.State s:m2.getStates()){
			newid.put(m2id++, m1.addState(s));
		}
		for(StateMachine.State s:m1.getStates()){
			for(gpath.query.stm.StateMachine.Condition c:s.getConditions()){
				for(Transition t:c.getTransitions()){
					if(t.getType()==TransitionType.Backtrack){
						t.setToState(newid.get(m2.getStartState()));
					}
				}
			}
		}
		for(StateMachine.State s:m2.getStates()){
			for(gpath.query.stm.StateMachine.Condition c:s.getConditions()){
				for(Transition t:c.getTransitions()){
					for(Map.Entry<Integer, Integer> swapid:newid.entrySet()){
						if(t.getToState() == swapid.getKey()){
							t.setToState(swapid.getValue());
						}
					}
				}
			}
		}
		return m1;
	}
}
