package base;

/**
 * Created by zhangyun on 4/29/15.
 */
public class MapTask extends Task {
  /**The MapTask processing split, input file paths are split file paths*/
  private String inputPath;
  private int numReduceTasks;
  private int TaskSortMB;
  private long softLimit;
  private long bufStart;
  private long bufVoid;
  private long kvStart;
  private long kvLength;
  private String  outCollector;


}
