import java.util.Observable;
import java.util.Observer;

/**
 * Created by zhangyun on 4/27/15.
 * The Register is used to store statistic information,when QueryLog
 * accept all log of one query.
 */
public class Register implements Observer{

  /**
   * This method is called whenever the observed object is changed. An
   * application calls an <tt>Observable</tt> object's
   * <code>notifyObservers</code> method to have all the object's
   * observers notified of the change.
   *
   * @param o   the observable object.
   * @param arg an argument passed to the <code>notifyObservers</code>
   */
  @Override
  public void update(Observable o, Object arg) {

  }
}
