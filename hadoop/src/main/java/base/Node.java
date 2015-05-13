package base;

import java.util.*;

/**
 * Created by zhangyun on 5/4/15.
 */
public class Node{
  protected List<Node> childNode;
  protected List<Node> parentNode;
  protected String nodeId;
  protected String nodeName;
  //Processing lines numbers;
  private  long rows;
  //Taken time of processing data
  private long procTime;
  //Remark information of node
  private String remark;

  public Node(String nodeName, String ID){
    this.childNode = new ArrayList<Node>();
    this.parentNode = new ArrayList<Node>();
    this.nodeId = ID;
    this.nodeName = nodeName;
    rows = 0;
    this.procTime = 0L;
  }

  public Node(String nodeName, String ID,
              List<Node> childNode, List<Node> parentNode ){
    this.childNode = childNode;
    this.parentNode = parentNode;
    this.nodeId = ID;
    this.nodeName = nodeName;
    rows = 0;
  }

  public String toString(){
    return this.nodeName+ "[" +
            this.nodeId + "] <processing:" + rows +
            "/taken Time:" + procTime + ">";
  }

  public String toString(Collection<Node> top) {
    StringBuilder builder = new StringBuilder();
    Set<String> visited = new HashSet<String>();
    for (Node op : top) {
      if (builder.length() > 0) {
        builder.append('\n');
      }
      toString(builder, visited, op, 0);
    }
    return builder.toString();
  }

  static boolean toString(StringBuilder builder, Set<String> visited, Node op, int start) {
    String name = op.toString();
    boolean added = visited.add(name);
    if (start > 0) {
      builder.append("-");
      start++;
    }
    builder.append(name);
    start += name.length();
    if (added) {
      if (op.getNumChild() > 0) {
        List<Node> children = op.getChildNode();
        for (int i = 0; i < children.size(); i++) {
          if (i > 0) {
            builder.append('\n');
            for (int j = 0; j < start; j++) {
              builder.append(' ');
            }
          }
          toString(builder, visited, children.get(i), start);
        }
      }
      return true;
    }
    return false;
  }

  public int getNumChild(){
    return childNode.size();
  }

  public List<Node> getChildNode() {
    return childNode;
  }

  public List<Node> getParentNode() {
    return parentNode;
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getNodeName() {
    return nodeName;
  }

  public long getRows() {
    return rows;
  }

  public void setRows(long rows) {
    this.rows += rows;
  }

  public void setParentNode(List<Node> parentNode) {
    this.parentNode = parentNode;
  }

  public void setChildNode(List<Node> childNode) {
    this.childNode = childNode;
  }

  public String getRemark() {
    return remark;
  }

  public void setRemark(String remark) {
    this.remark = remark;
  }

  public long getProcTime() {
    return procTime;
  }

  public void setProcTime(long procTime) {
    this.procTime = procTime;
  }
}