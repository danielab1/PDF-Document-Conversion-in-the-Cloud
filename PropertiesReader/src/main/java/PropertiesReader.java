
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

/**
 * @author Crunchify.com
 *
 */

public class PropertiesReader {
    private String path;

    public PropertiesReader(String path){
        this.path = path;
    }
    public String getProperty(String key) {
        InputStream inputStream = null;
        try {
            Properties prop = new Properties();
            inputStream = new FileInputStream(path);

            prop.load(inputStream);


            // get the property value and print it out
            return prop.getProperty(key);

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }  finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}