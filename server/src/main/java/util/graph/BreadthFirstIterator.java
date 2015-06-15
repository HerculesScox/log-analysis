
package util.graph;

import java.util.*;

/**
 * Iterates over the vertices in a directed graph in breadth-first order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class BreadthFirstIterator<V, E extends DefaultEdge>
    implements Iterator<V> {
  private final DirectedGraph<V, E> graph;
  private final Deque<V> deque = new ArrayDeque<V>();
  private final Set<V> set = new HashSet<V>();

  public BreadthFirstIterator(DirectedGraph<V, E> graph, V root) {
    this.graph = graph;
    this.deque.add(root);
  }

  public static <V, E extends DefaultEdge> Iterable<V> of(
      final DirectedGraph<V, E> graph, final V root) {
    return new Iterable<V>() {
      public Iterator<V> iterator() {
        return new BreadthFirstIterator<V, E>(graph, root);
      }
    };
  }

  /** Populates a set with the nodes reachable from a given node. */
  public static <V, E extends DefaultEdge> void reachable(Set<V> set,
      final DirectedGraph<V, E> graph, final V root) {
    final Deque<V> deque = new ArrayDeque<V>();
    deque.add(root);
    set.add(root);
    while (!deque.isEmpty()) {
      V v = deque.removeFirst();
      for (E e : graph.getOutwardEdges(v)) {
        @SuppressWarnings("unchecked") V target = (V) e.target;
        if (set.add(target)) {
          deque.addLast(target);
        }
      }
    }
  }

  public boolean hasNext() {
    return !deque.isEmpty();
  }

  public V next() {
    V v = deque.removeFirst();
    for (E e : graph.getOutwardEdges(v)) {
      @SuppressWarnings("unchecked") V target = (V) e.target;
      if (set.add(target)) {
        deque.addLast(target);
      }
    }
    return v;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}

// End BreadthFirstIterator.java
