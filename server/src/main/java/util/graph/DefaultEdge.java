
package util.graph;

/**
 * Default implementation of Edge.
 */
public class DefaultEdge {
  public final Object source;
  public final Object target;

  public DefaultEdge(Object source, Object target) {
    this.source = source;
    this.target = target;
  }

  @Override public int hashCode() {
    return source.hashCode() * 31 + target.hashCode();
  }

  @Override public boolean equals(Object obj) {
    return this == obj
        || obj instanceof DefaultEdge
        && ((DefaultEdge) obj).source.equals(source)
        && ((DefaultEdge) obj).target.equals(target);
  }

  public static <V> DirectedGraph.EdgeFactory<V, DefaultEdge> factory() {
    return new DirectedGraph.EdgeFactory<V, DefaultEdge>() {
      public DefaultEdge createEdge(V v0, V v1) {
        return new DefaultEdge(v0, v1);
      }
    };
  }
}

// End DefaultEdge.java
