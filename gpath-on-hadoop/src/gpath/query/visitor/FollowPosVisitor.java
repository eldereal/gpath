package gpath.query.visitor;

import gpath.query.expression.AsteriskQuantifier;
import gpath.query.expression.Concat;
import gpath.query.expression.Condition;
import gpath.query.expression.Edge;
import gpath.query.expression.Entity;
import gpath.query.expression.Expression;
import gpath.query.expression.Order;
import gpath.query.expression.Parallel;
import gpath.query.expression.PlusQuantifier;
import gpath.query.expression.Query;
import gpath.query.expression.QuestionQuantifier;
import gpath.query.expression.Vertex;
import gpath.query.visitor.Visitor._void;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FollowPosVisitor implements Visitor<_void, _void> {

	Map<Entity, Integer> idsMap;
	Map<Integer, Entity> idsRevMap;
	Map<Expression, Boolean> nullable;
	Map<Expression, Set<Integer>> firstpos;
	Map<Expression, Set<Integer>> lastpos;
	Map<Entity, Set<Integer>> followpos = new HashMap<Entity, Set<Integer>>();
	
	
	public FollowPosVisitor(Map<Entity, Integer> idsMap, Map<Integer, Entity> idsRevMap,
			Map<Expression, Boolean> nullable,
			Map<Expression, Set<Integer>> firstpos,
			Map<Expression, Set<Integer>> lastpos) {
		super();
		this.idsMap = idsMap;
		this.nullable = nullable;
		this.firstpos = firstpos;
		this.lastpos = lastpos;
		this.idsRevMap = idsRevMap;
	}

	public Map<Entity, Set<Integer>> getFollowPos(){
		return followpos;
	}
	
	@Override
	public _void visitQuery(Query query,
			_void... arguments) {
		query.getExpression().accept(this);
		Set<Integer> first = lastpos.get(query.getExpression());
		Set<Integer> c2 = firstpos.get(query.getEOF());
		for(Integer c1:first){
			Entity e = idsRevMap.get(c1);
			Set<Integer> follow;
			if(!followpos.containsKey(e)){
				follow = new HashSet<Integer>();
				followpos.put(e, follow);
			}else{
				follow = followpos.get(e);
			}
			follow.addAll(c2);
		}
		return null;
	}

	@Override
	public _void visitVertex(Vertex vertex,
			_void... arguments) {
		return null;
	}

	@Override
	public _void visitEdge(Edge edge,
			_void... arguments) {
		return null;
	}

	@Override
	public _void visitAsteriskQuantifier(
			AsteriskQuantifier quantifier,
			_void... arguments) {
		quantifier.getQuantified().accept(this);		
		Set<Integer> first = lastpos.get(quantifier.getQuantified());
		Set<Integer> c2 = firstpos.get(quantifier.getQuantified());
		for(Integer c1:first){
			Entity e = idsRevMap.get(c1);
			Set<Integer> follow;
			if(!followpos.containsKey(e)){
				follow = new HashSet<Integer>();
				followpos.put(e, follow);
			}else{
				follow = followpos.get(e);
			}
			follow.addAll(c2);
		}
		return null;
	}

	@Override
	public _void visitQuestionQuantifier(
			QuestionQuantifier quantifier,
			_void... arguments) {
		quantifier.getQuantified().accept(this);
		return null;
	}

	@Override
	public _void visitPlusQuantifier(
			PlusQuantifier quantifier,
			_void... arguments) {
		quantifier.getQuantified().accept(this);		
		Set<Integer> first = lastpos.get(quantifier.getQuantified());
		Set<Integer> c2 = firstpos.get(quantifier.getQuantified());
		for(Integer c1:first){
			Entity e = idsRevMap.get(c1);
			Set<Integer> follow;
			if(!followpos.containsKey(e)){
				follow = new HashSet<Integer>();
				followpos.put(e, follow);
			}else{
				follow = followpos.get(e);
			}
			follow.addAll(c2);
		}
		return null;
	}

	@Override
	public _void visitConcat(Concat concat,
			_void... arguments) {
		concat.getFirst().accept(this);		
		concat.getSecond().accept(this);
		Set<Integer> first = lastpos.get(concat.getFirst());
		Set<Integer> c2 = firstpos.get(concat.getSecond());
		for(Integer c1:first){
			Entity e = idsRevMap.get(c1);
			Set<Integer> follow;
			if(!followpos.containsKey(e)){
				follow = new HashSet<Integer>();
				followpos.put(e, follow);
			}else{
				follow = followpos.get(e);
			}
			follow.addAll(c2);
		}
		return null;
	}

	@Override
	public _void visitCondition(Condition cond,
			_void... arguments) {
		return null;
	}

	@Override
	public _void visitOrder(Order order,
			_void... arguments) {
		return null;
	}

	@Override
	public _void visitParallel(Parallel parallel,
			_void... arguments) {
		parallel.getFirst().accept(this);
		parallel.getSecond().accept(this);
		return null;
	}


}
