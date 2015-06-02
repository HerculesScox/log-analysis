/**
* Created by zhangyun on 5/15/15.
*/
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Paths;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import spark.ModelAndView;
import spark.TemplateEngine;


public class FreeMarkerEngine extends TemplateEngine {

  /**
   * The FreeMarker configuration
   */
  private Configuration configuration;

  /**
   * Creates a FreeMarkerEngine
   */
  public FreeMarkerEngine() {
    this.configuration = createDefaultConfiguration();
  }

  /**
   * Creates a FreeMarkerEngine with a configuration
   *
   * @param configuration The Freemarker configuration
   */
  public FreeMarkerEngine(Configuration configuration) {
    this.configuration = configuration;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public String render(ModelAndView modelAndView) {
    try {
      StringWriter stringWriter = new StringWriter();

      Template template = configuration.getTemplate(modelAndView.getViewName());
      template.process(modelAndView.getModel(), stringWriter);

      return stringWriter.toString();
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    } catch (TemplateException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Sets FreeMarker configuration.
   * Note: If configuration is not set the default configuration
   * will be used.
   *
   * @param configuration the configuration to set
   */
  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  private Configuration createDefaultConfiguration() {
    Configuration configuration = new Configuration();
   // configuration.setClassForTemplateLoading(FreeMarkerEngine.class, "/");
   try {
     String basePath = Paths.get("").toAbsolutePath().toString();
     String path = basePath;
     if( basePath.contains("server")){
       path += "/src/main/java/view";
     }else{
       path += "/server/src/main/java/view";
     }
     configuration.setDirectoryForTemplateLoading(new File(path));
   }catch ( IOException e){
      e.printStackTrace();
   }
   return configuration;
  }

}
