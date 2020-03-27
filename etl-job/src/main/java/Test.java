import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    public static void main(String[] args) {
        Long time = System.currentTimeMillis();
        DateFormat dateFormat =  new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Date date = new Date(time);
        String jsonSchemaDate = dateFormat.format(date);
        System.out.println(jsonSchemaDate);
    }
}
