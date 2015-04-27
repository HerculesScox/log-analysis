import java.util.HashSet;
import java.util.Observable;

/**
 * Created by zhangyun on 4/24/15.
 */
public class QueryLog extends Observable{
  //store all workflowID after analyzing
  public static HashSet<String> allWorkFlowID;


  public QueryLog() {
    allWorkFlowID = new HashSet<String>();
  }

  /**
   * When All information of one query was collected, QueryLog class
   * should notify Register(Observer) to sore them into DB.
   */
  public void gatherSucc(){
    setChanged();
    notifyObservers();
  }
}
