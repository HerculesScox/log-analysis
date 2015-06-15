
package util.graph;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Directed graph.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public interface DirectedGraph<V, E> {
  /** Adds a vertex to this graph.
   *
   * @param vertex Vertex
   * @return Whether vertex was added
   */
  boolean addVertex(V vertex);

  /** Adds an edge to this graph.
   *
   * @param vertex Source vertex
   * @param targetVertex Target vertex
   * @return New edge, if added, otherwise null
   * @throws IllegalArgumentException if either vertex is not already in graph
   */
  E addEdge(V vertex, V targetVertex);

  E getEdge(V source, V target);

  boolean removeEdge(V vertex, V targetVertex);

  Set<V> vertexSet();

  void removeAllVertices(Collection<V> collection);

  List<E> getOutwardEdges(V source);

  List<E> getInwardEdges(V vertex);

  Set<E> edgeSet();

  /** Factory for edges.
   *
   * @param <V> Vertex type
   * @param <E> Edge type
   */
  interface EdgeFactory<V, E> {
    E createEdge(V v0, V v1);
  }
}

// End DirectedGraph.java
