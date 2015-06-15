
package util;

import java.util.*;

/**
 * Miscellaneous utility functions.
 */
public class Util {
  private Util() {}
  /**
   * Combines two integers into a hash code.
   */
  public static int hash(
      int i,
      int j) {
    return (i << 4) ^ j;
  }

  /**
   * Computes a hash code from an existing hash code and an object (which may
   * be null).
   */
  public static int hash(
      int h,
      Object o) {
    int k = (o == null) ? 0 : o.hashCode();
    return ((h << 4) | h) ^ k;
  }

  /**
   * Computes a hash code from an existing hash code and an array of objects
   * (which may be null).
   */
  public static int hashArray(
      int h,
      Object[] a) {
    // The hashcode for a null array and an empty array should be different
    // than h, so use magic numbers.
    if (a == null) {
      return hash(h, 19690429);
    }
    if (a.length == 0) {
      return hash(h, 19690721);
    }
    for (int i = 0; i < a.length; i++) {
      h = hash(h, a[i]);
    }
    return h;
  }

  /** Computes the hash code of a {@code double} value. Equivalent to
   * {@link Double}{@code .hashCode(double)}, but that method was only
   * introduced in JDK 1.8.
   *
   * @param v Value
   * @return Hash code
   */
  public static int hashCode(double v) {
    long bits = Double.doubleToLongBits(v);
    return (int) (bits ^ (bits >>> 32));
  }

  /**
   * Returns a set of the elements which are in <code>set1</code> but not in
   * <code>set2</code>, without modifying either.
   */
  public static <T> Set<T> minus(Set<T> set1, Set<T> set2) {
    if (set1.isEmpty()) {
      return set1;
    } else if (set2.isEmpty()) {
      return set1;
    } else {
      Set<T> set = new HashSet<T>(set1);
      set.removeAll(set2);
      return set;
    }
  }

  /**
   * Computes <code>nlogn(n)</code> using the natural logarithm (or <code>
   * n</code> if <code>n &lt; {@link Math#E}</code>, so the result is never
   * negative.
   */
  public static double nLogN(double d) {
    return (d < Math.E) ? d : (d * Math.log(d));
  }


}

// End Util.java
