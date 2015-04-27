package base;

/**
 * Created by zhangyun on 4/21/15.
 */
public class Counter{
  protected String name;
  protected String displayName;
  private Counts counts;

  /**
   * The Counts is a part of Counter, it defined
   * more detailed information
   */
  private static class Counts{
    protected String name;
    protected String displayName;
    private long value;

    private Counts(String displayName, String name, long value) {
      this.displayName = displayName;
      this.name = name;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public String getDisplayName() {
      return displayName;
    }

    public long getValue() {
      return value;
    }
  }

  public Counter(String displayName, String name, Counts counts) {
    this.displayName = displayName;
    this.name = name;
    this.counts = counts;
  }

  public static Counts createCounts(String displayName, String name, long value){
      return new Counts( displayName,  name,  value);
  }

}
