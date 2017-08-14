/*
 * dk.brics.automaton
 * 
 * Copyright (c) 2001-2011 Anders Moeller
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.indeed.imhotep.automaton;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Basic automata operations.
 */
public final class BasicOperations {
	
	private BasicOperations() {}

	/** 
	 * Returns an automaton that accepts the concatenation of the languages of 
	 * the given automata. 
	 * <p>
	 * Complexity: linear in number of states. 
	 */
	public static Automaton concatenate(Automaton a1, Automaton a2) {
		if (a1.isSingleton() && a2.isSingleton()) {
			return BasicAutomata.makeString(a1.singleton + a2.singleton);
		}
		if (isEmpty(a1) || isEmpty(a2)) {
			return BasicAutomata.makeEmpty();
		}
		final boolean deterministic = a1.isSingleton() && a2.isDeterministic();
		if (a1 == a2) {
			a1 = a1.cloneExpanded();
			a2 = a2.cloneExpanded();
		} else {
			a1 = a1.cloneExpandedIfRequired();
			a2 = a2.cloneExpandedIfRequired();
		}
		for (final State s : a1.getAcceptStates()) {
			s.accept = false;
			s.addEpsilon(a2.initial);
		}
		a1.deterministic = deterministic;
		a1.clearHashCode();
		a1.checkMinimizeAlways();
		return a1;
	}
	
	/**
	 * Returns an automaton that accepts the concatenation of the languages of
	 * the given automata.
	 * <p>
	 * Complexity: linear in total number of states.
	 */
	public static Automaton concatenate(final List<Automaton> l) {
		if (l.isEmpty()) {
			return BasicAutomata.makeEmptyString();
		}
		boolean all_singleton = true;
		for (final Automaton a : l) {
			if (!a.isSingleton()) {
				all_singleton = false;
				break;
			}
		}
		if (all_singleton) {
			final StringBuilder b = new StringBuilder();
			for (final Automaton a : l) {
				b.append(a.singleton);
			}
			return BasicAutomata.makeString(b.toString());
		} else {
			for (final Automaton a : l) {
				if (a.isEmpty()) {
					return BasicAutomata.makeEmpty();
				}
			}
			final Set<Integer> ids = new HashSet<>();
			for (final Automaton a : l) {
				ids.add(System.identityHashCode(a));
			}
			final boolean has_aliases = ids.size() != l.size();
			Automaton b = l.get(0);
			if (has_aliases) {
				b = b.cloneExpanded();
			} else {
				b = b.cloneExpandedIfRequired();
			}
			Set<State> ac = b.getAcceptStates();
			boolean first = true;
			for (final Automaton a : l) {
				if (first) {
					first = false;
				} else {
					if (a.isEmptyString()) {
						continue;
					}
					Automaton aa = a;
					if (has_aliases) {
						aa = aa.cloneExpanded();
					} else {
						aa = aa.cloneExpandedIfRequired();
					}
					final Set<State> ns = aa.getAcceptStates();
					for (final State s : ac) {
						s.accept = false;
						s.addEpsilon(aa.initial);
						if (s.accept) {
							ns.add(s);
						}
					}
					ac = ns;
				}
			}
			b.deterministic = false;
			b.clearHashCode();
			b.checkMinimizeAlways();
			return b;
		}
	}

	/**
	 * Returns an automaton that accepts the union of the empty string and the
	 * language of the given automaton.
	 * <p>
	 * Complexity: linear in number of states.
	 */
	public static Automaton optional(Automaton a) {
		a = a.cloneExpandedIfRequired();
		final State s = new State();
		s.addEpsilon(a.initial);
		s.accept = true;
		a.initial = s;
		a.deterministic = false;
		a.clearHashCode();
		a.checkMinimizeAlways();
		return a;
	}
	
	/**
	 * Returns an automaton that accepts the Kleene star (zero or more
	 * concatenated repetitions) of the language of the given automaton.
	 * Never modifies the input automaton language.
	 * <p>
	 * Complexity: linear in number of states.
	 */
	public static Automaton repeat(Automaton a) {
		a = a.cloneExpanded();
		final State s = new State();
		s.accept = true;
		s.addEpsilon(a.initial);
		for (final State p : a.getAcceptStates()) {
			p.addEpsilon(s);
		}
		a.initial = s;
		a.deterministic = false;
		a.clearHashCode();
		a.checkMinimizeAlways();
		return a;
	}

	/**
	 * Returns an automaton that accepts <code>min</code> or more
	 * concatenated repetitions of the language of the given automaton.
	 * <p>
	 * Complexity: linear in number of states and in <code>min</code>.
	 */
	public static Automaton repeat(final Automaton a, int min) {
		if (min == 0) {
			return repeat(a);
		}
		final List<Automaton> as = new ArrayList<>();
		while (min-- > 0) {
			as.add(a);
		}
		as.add(repeat(a));
		return concatenate(as);
	}
	
	/**
	 * Returns an automaton that accepts between <code>min</code> and
	 * <code>max</code> (including both) concatenated repetitions of the
	 * language of the given automaton.
	 * <p>
	 * Complexity: linear in number of states and in <code>min</code> and
	 * <code>max</code>.
	 */
	public static Automaton repeat(final Automaton a, int min, int max) {
		if (min > max) {
			return BasicAutomata.makeEmpty();
		}
		max -= min;
		a.expandSingleton();
		final Automaton b;
		if (min == 0) {
			b = BasicAutomata.makeEmptyString();
		} else if (min == 1) {
			b = a.clone();
		} else {
			final List<Automaton> as = new ArrayList<>();
			while (min-- > 0) {
				as.add(a);
			}
			b = concatenate(as);
		}
		if (max > 0) {
			Automaton d = a.clone();
			while (--max > 0) {
				final Automaton c = a.clone();
				for (final State p : c.getAcceptStates()) {
					p.addEpsilon(d.initial);
				}
				d = c;
			}
			for (final State p : b.getAcceptStates()) {
				p.addEpsilon(d.initial);
			}
			b.deterministic = false;
			b.clearHashCode();
			b.checkMinimizeAlways();
		}
		return b;
	}

	/**
	 * Returns a (deterministic) automaton that accepts the complement of the
	 * language of the given automaton.
	 * <p>
	 * Complexity: linear in number of states (if already deterministic).
	 */
	public static Automaton complement(Automaton a) {
		a = a.cloneExpandedIfRequired();
		a.determinize();
		a.totalize();
		for (final State p : a.getStates()) {
			p.accept = !p.accept;
		}
		a.removeDeadTransitions();
		return a;
	}

	/**
	 * Returns a (deterministic) automaton that accepts the intersection of
	 * the language of <code>a1</code> and the complement of the language of 
	 * <code>a2</code>. As a side-effect, the automata may be determinized, if not
	 * already deterministic.
	 * <p>
	 * Complexity: quadratic in number of states (if already deterministic).
	 */
	public static Automaton minus(final Automaton a1, final Automaton a2) {
		if (a1.isEmpty() || a1 == a2) {
			return BasicAutomata.makeEmpty();
		}
		if (a2.isEmpty()) {
			return a1.cloneIfRequired();
		}
		if (a1.isSingleton()) {
			if (a2.run(a1.singleton)) {
				return BasicAutomata.makeEmpty();
			} else {
				return a1.cloneIfRequired();
			}
		}
		return intersection(a1, a2.complement());
	}

	/**
	 * Returns an automaton that accepts the intersection of
	 * the languages of the given automata. 
	 * Never modifies the input automata languages.
	 * <p>
	 * Complexity: quadratic in number of states.
	 */
	public static Automaton intersection(final Automaton a1, final Automaton a2) {
		if (a1.isSingleton()) {
			if (a2.run(a1.singleton)) {
				return a1.cloneIfRequired();
			} else {
				return BasicAutomata.makeEmpty();
			}
		}
		if (a2.isSingleton()) {
			if (a1.run(a2.singleton)) {
				return a2.cloneIfRequired();
			} else {
				return BasicAutomata.makeEmpty();
			}
		}
		if (a1 == a2) {
			return a1.cloneIfRequired();
		}
		final Transition[][] transitions1 = Automaton.getSortedTransitions(a1.getStates());
		final Transition[][] transitions2 = Automaton.getSortedTransitions(a2.getStates());
		final Automaton c = new Automaton();
		final LinkedList<StatePair> worklist = new LinkedList<>();
		final HashMap<StatePair, StatePair> newstates = new HashMap<>();
		StatePair p = new StatePair(c.initial, a1.initial, a2.initial);
		worklist.add(p);
		newstates.put(p, p);
		while (!worklist.isEmpty()) {
			p = worklist.removeFirst();
			p.s.accept = p.s1.accept && p.s2.accept;
			final Transition[] t1 = transitions1[p.s1.number];
			final Transition[] t2 = transitions2[p.s2.number];
			for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
				while (b2 < t2.length && t2[b2].max < t1[n1].min) {
					b2++;
				}
				for (int n2 = b2; n2 < t2.length && t1[n1].max >= t2[n2].min; n2++) {
					if (t2[n2].max >= t1[n1].min) {
						final StatePair q = new StatePair(t1[n1].to, t2[n2].to);
						StatePair r = newstates.get(q);
						if (r == null) {
							q.s = new State();
							worklist.add(q);
							newstates.put(q, q);
							r = q;
						}
						final char min = t1[n1].min > t2[n2].min ? t1[n1].min : t2[n2].min;
						final char max = t1[n1].max < t2[n2].max ? t1[n1].max : t2[n2].max;
						p.s.transitions.add(new Transition(min, max, r.s));
					}
				}
			}
		}
		c.deterministic = a1.deterministic && a2.deterministic;
		c.removeDeadTransitions();
		c.checkMinimizeAlways();
		return c;
	}
		
	/**
	 * Returns true if the language of <code>a1</code> is a subset of the
	 * language of <code>a2</code>. 
	 * As a side-effect, <code>a2</code> is determinized if not already marked as
	 * deterministic.
	 * <p>
	 * Complexity: quadratic in number of states.
	 */
	public static boolean subsetOf(final Automaton a1, final Automaton a2) {
		if (a1 == a2) {
			return true;
		}
		if (a1.isSingleton()) {
			if (a2.isSingleton()) {
				return a1.singleton.equals(a2.singleton);
			}
			return a2.run(a1.singleton);
		}
		a2.determinize();
		final Transition[][] transitions1 = Automaton.getSortedTransitions(a1.getStates());
		final Transition[][] transitions2 = Automaton.getSortedTransitions(a2.getStates());
		final LinkedList<StatePair> worklist = new LinkedList<>();
		final HashSet<StatePair> visited = new HashSet<>();
		StatePair p = new StatePair(a1.initial, a2.initial);
		worklist.add(p);
		visited.add(p);
		while (!worklist.isEmpty()) {
			p = worklist.removeFirst();
			if (p.s1.accept && !p.s2.accept) {
				return false;
			}
			final Transition[] t1 = transitions1[p.s1.number];
			final Transition[] t2 = transitions2[p.s2.number];
			for (int n1 = 0, b2 = 0; n1 < t1.length; n1++) {
				while (b2 < t2.length && t2[b2].max < t1[n1].min) {
					b2++;
				}
				int min1 = t1[n1].min, max1 = t1[n1].max;
				for (int n2 = b2; n2 < t2.length && t1[n1].max >= t2[n2].min; n2++) {
					if (t2[n2].min > min1) {
						return false;
					}
					if (t2[n2].max < Character.MAX_VALUE) {
						min1 = t2[n2].max + 1;
					} else {
						min1 = Character.MAX_VALUE;
						max1 = Character.MIN_VALUE;
					}
					final StatePair q = new StatePair(t1[n1].to, t2[n2].to);
					if (!visited.contains(q)) {
						worklist.add(q);
						visited.add(q);
					}
				}
				if (min1 <= max1) {
					return false;
				}
			}		
		}
		return true;
	}
	
	/**
	 * Returns an automaton that accepts the union of the languages of the given automata.
	 * <p>
	 * Complexity: linear in number of states.
	 */
	public static Automaton union(Automaton a1, Automaton a2) {
		if ((a1.isSingleton() && a2.isSingleton() && a1.singleton.equals(a2.singleton)) || a1 == a2) {
			return a1.cloneIfRequired();
		}
		if (a1 == a2) {
			a1 = a1.cloneExpanded();
			a2 = a2.cloneExpanded();
		} else {
			a1 = a1.cloneExpandedIfRequired();
			a2 = a2.cloneExpandedIfRequired();
		}
		final State s = new State();
		s.addEpsilon(a1.initial);
		s.addEpsilon(a2.initial);
		a1.initial = s;
		a1.deterministic = false;
		a1.clearHashCode();
		a1.checkMinimizeAlways();
		return a1;
	}
	
	/**
	 * Returns an automaton that accepts the union of the languages of the given automata.
	 * <p>
	 * Complexity: linear in number of states.
	 */
	public static Automaton union(final Collection<Automaton> l) {
		final Set<Integer> ids = new HashSet<>();
		for (final Automaton a : l) {
			ids.add(System.identityHashCode(a));
		}
		final boolean has_aliases = ids.size() != l.size();
		final State s = new State();
		for (final Automaton b : l) {
			if (b.isEmpty()) {
				continue;
			}
			Automaton bb = b;
			if (has_aliases) {
				bb = bb.cloneExpanded();
			} else {
				bb = bb.cloneExpandedIfRequired();
			}
			s.addEpsilon(bb.initial);
		}
		final Automaton a = new Automaton();
		a.initial = s;
		a.deterministic = false;
		a.clearHashCode();
		a.checkMinimizeAlways();
		return a;
	}

	/**
	 * Determinizes the given automaton.
	 * <p>
	 * Complexity: exponential in number of states.
	 */
	public static void determinize(final Automaton a) {
		if (a.deterministic || a.isSingleton()) {
			return;
		}
		final Set<State> initialset = new HashSet<>();
		initialset.add(a.initial);
		determinize(a, initialset);
	}

	/** 
	 * Determinizes the given automaton using the given set of initial states. 
	 */
	static void determinize(final Automaton a, final Set<State> initialset) {
		final char[] points = a.getStartPoints();
		// subset construction
		final Map<Set<State>, Set<State>> sets = new HashMap<>();
		final LinkedList<Set<State>> worklist = new LinkedList<>();
		final Map<Set<State>, State> newstate = new HashMap<>();
		sets.put(initialset, initialset);
		worklist.add(initialset);
		a.initial = new State();
		newstate.put(initialset, a.initial);
		while (!worklist.isEmpty()) {
			final Set<State> s = worklist.removeFirst();
			final State r = newstate.get(s);
			for (final State q : s) {
				if (q.accept) {
					r.accept = true;
					break;
				}
			}
			for (int n = 0; n < points.length; n++) {
				final Set<State> p = new HashSet<>();
				for (final State q : s) {
					for (final Transition t : q.transitions) {
						if (t.min <= points[n] && points[n] <= t.max) {
							p.add(t.to);
						}
					}
				}
				if (!sets.containsKey(p)) {
					if (sets.size() > 50000) {
						throw new RegexTooComplexException("The provided regex is too complex. " +
								"Please replace expressions like '.*A.*|.*B.*|.*C.*' with '.*(A|B|C).*'");
					}
					sets.put(p, p);
					worklist.add(p);
					newstate.put(p, new State());
				}
				final State q = newstate.get(p);
				final char min = points[n];
				final char max;
				if (n + 1 < points.length) {
					max = (char) (points[n + 1] - 1);
				} else {
					max = Character.MAX_VALUE;
				}
				r.transitions.add(new Transition(min, max, q));
			}
		}
		a.deterministic = true;
		a.removeDeadTransitions();
	}

	/** 
	 * Adds epsilon transitions to the given automaton.
	 * This method adds extra character interval transitions that are equivalent to the given
	 * set of epsilon transitions. 
	 * @param pairs collection of {@link StatePair} objects representing pairs of source/destination states 
	 *        where epsilon transitions should be added
	 */
	public static void addEpsilons(final Automaton a, final Collection<StatePair> pairs) {
		a.expandSingleton();
		final HashMap<State, HashSet<State>> forward = new HashMap<>();
		final HashMap<State, HashSet<State>> back = new HashMap<>();
		for (final StatePair p : pairs) {
			HashSet<State> to = forward.get(p.s1);
			if (to == null) {
				to = new HashSet<>();
				forward.put(p.s1, to);
			}
			to.add(p.s2);
			HashSet<State> from = back.get(p.s2);
			if (from == null) {
				from = new HashSet<>();
				back.put(p.s2, from);
			}
			from.add(p.s1);
		}
		// calculate epsilon closure
		final LinkedList<StatePair> worklist = new LinkedList<>(pairs);
		final HashSet<StatePair> workset = new HashSet<>(pairs);
		while (!worklist.isEmpty()) {
			final StatePair p = worklist.removeFirst();
			workset.remove(p);
			final HashSet<State> to = forward.get(p.s2);
			final HashSet<State> from = back.get(p.s1);
			if (to != null) {
				for (final State s : to) {
					final StatePair pp = new StatePair(p.s1, s);
					if (!pairs.contains(pp)) {
						pairs.add(pp);
						forward.get(p.s1).add(s);
						back.get(s).add(p.s1);
						worklist.add(pp);
						workset.add(pp);
						if (from != null) {
							for (final State q : from) {
								final StatePair qq = new StatePair(q, p.s1);
								if (!workset.contains(qq)) {
									worklist.add(qq);
									workset.add(qq);
								}
							}
						}
					}
				}
			}
		}
		// add transitions
		for (final StatePair p : pairs) {
			p.s1.addEpsilon(p.s2);
		}
		a.deterministic = false;
		a.clearHashCode();
		a.checkMinimizeAlways();
	}
	
	/**
	 * Returns true if the given automaton accepts the empty string and nothing else.
	 */
	public static boolean isEmptyString(final Automaton a) {
		if (a.isSingleton()) {
			return a.singleton.isEmpty();
		} else {
			return a.initial.accept && a.initial.transitions.isEmpty();
		}
	}

	/**
	 * Returns true if the given automaton accepts no strings.
	 */
	public static boolean isEmpty(final Automaton a) {
		return !a.isSingleton() && !a.initial.accept && a.initial.transitions.isEmpty();
	}
	
	/**
	 * Returns true if the given automaton accepts all strings.
	 */
	public static boolean isTotal(final Automaton a) {
		if (a.isSingleton()) {
			return false;
		}
		if (a.initial.accept && a.initial.transitions.size() == 1) {
			final Transition t = a.initial.transitions.iterator().next();
			return t.to == a.initial && t.min == Character.MIN_VALUE && t.max == Character.MAX_VALUE;
		}
		return false;
	}
	
	/**
	 * Returns a shortest accepted/rejected string. 
	 * If more than one shortest string is found, the lexicographically first of the shortest strings is returned.
	 * @param accepted if true, look for accepted strings; otherwise, look for rejected strings
	 * @return the string, null if none found
	 */
	public static String getShortestExample(final Automaton a, final boolean accepted) {
		if (a.isSingleton()) {
			if (accepted) {
				return a.singleton;
			} else if (!a.singleton.isEmpty()) {
				return "";
			} else {
				return "\u0000";
			}

		}
		return getShortestExample(a.getInitialState(), accepted);
	}

	static String getShortestExample(final State s, final boolean accepted) {
		final Map<State,String> path = new HashMap<>();
		final LinkedList<State> queue = new LinkedList<>();
		path.put(s, "");
		queue.add(s);
		String best = null;
		while (!queue.isEmpty()) {
			final State q = queue.removeFirst();
			final String p = path.get(q);
			if (q.accept == accepted) {
				if (best == null || p.length() < best.length() || (p.length() == best.length() && p.compareTo(best) < 0)) {
					best = p;
				}
			} else {
				for (final Transition t : q.getTransitions()) {
					final String tp = path.get(t.to);
					final String np = p + t.min;
					if (tp == null || (tp.length() == np.length() && np.compareTo(tp) < 0)) {
						if (tp == null) {
							queue.addLast(t.to);
						}
						path.put(t.to, np);
					}
				}
			}
		}
		return best;
	}
	
	/**
	 * Returns true if the given string is accepted by the automaton. 
	 * <p>
	 * Complexity: linear in the length of the string.
	 * <p>
	 * <b>Note:</b> for full performance, use the {@link RunAutomaton} class.
	 */
	public static boolean run(final Automaton a, final String s) {
		if (a.isSingleton()) {
			return s.equals(a.singleton);
		}
		if (a.deterministic) {
			State p = a.initial;
			for (int i = 0; i < s.length(); i++) {
				final State q = p.step(s.charAt(i));
				if (q == null) {
					return false;
				}
				p = q;
			}
			return p.accept;
		} else {
			final Set<State> states = a.getStates();
			Automaton.setStateNumbers(states);
			LinkedList<State> pp = new LinkedList<>();
			LinkedList<State> pp_other = new LinkedList<>();
			BitSet bb = new BitSet(states.size());
			BitSet bb_other = new BitSet(states.size());
			pp.add(a.initial);
			final ArrayList<State> dest = new ArrayList<>();
			boolean accept = a.initial.accept;
			for (int i = 0; i < s.length(); i++) {
				final char c = s.charAt(i);
				accept = false;
				pp_other.clear();
				bb_other.clear();
				for (final State p : pp) {
					dest.clear();
					p.step(c, dest);
					for (final State q : dest) {
						if (q.accept) {
							accept = true;
						}
						if (!bb_other.get(q.number)) {
							bb_other.set(q.number);
							pp_other.add(q);
						}
					}
				}
				final LinkedList<State> tp = pp;
				pp = pp_other;
				pp_other = tp;
				final BitSet tb = bb;
				bb = bb_other;
				bb_other = tb;
			}
			return accept;
		}
	}
}
