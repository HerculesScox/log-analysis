
package util.graph;

import java.util.Set;

/**
 * Detects cycles in directed graphs.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class CycleDetector<V, E extends DefaultEdge> {
  private final DirectedGraph<V, E> graph;

  public CycleDetector(DirectedGraph<V, E> graph) {
    this.graph = graph;
  }

  public Set<V> findCycles() {
    return new TopologicalOrderIterator<V, E>(graph).findCycles();
  }
}

// End CycleDetector.java
