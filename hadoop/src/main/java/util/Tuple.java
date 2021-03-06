package util;

/**
 * Created by zhangyun on 4/23/15.
 */
public class Tuple<T,V> {
  private T key;
  private V value;

  public Tuple() {

  }

  public Tuple( T key , V value ){
    this.key = key;
    this.value = value;
  }

  public T getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public void setKey(T key) {
    this.key = key;
  }

  public void setValue(V value) {
    this.value = value;
  }
}
