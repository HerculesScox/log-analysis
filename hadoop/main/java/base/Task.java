package base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangyun on 4/21/15.
 */
public abstract class Task {
  private static Log LOG = LogFactory.getLog(Task.class);
  private String taskID;
  private String ContainerID:


  /**
   * To generate digest of entity related content
   */

  public class Node{
    protected List<Node> childNode;
    protected List<Node> parentNode;
    protected String nodeId;
    protected String nodeName;

    public Node(){
      ArrayList<Node> childNode = new ArrayList<Node>();
      ArrayList<Node> parentNode = new ArrayList<Node>();
      nodeId = "";
    }
  }


}
