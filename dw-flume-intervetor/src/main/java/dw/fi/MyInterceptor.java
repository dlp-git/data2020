package dw.fi;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String eventBody = new String(event.getBody());
//        JSONObject json = JSONObject.parseObject(eventBody);
//        String type = (String) json.get("type");
        boolean b = Pattern.compile(".*?\"type\":\"startup\".*?").matcher(eventBody).find();
        if (b){
            event.getHeaders().put("type","startup");
        }else{
            event.getHeaders().put("type","event");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event:list){
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }

    public static void main(String[] args) {
        String s = "{\"area\":\"shan3xi\",\"uid\":\"9638\",\"os\":\"ios\",\"ch\":\"appstore\",\"appid\":\"data2020\",\"mid\":\"mid_12964\",\"type\":\"startup\",\"vs\":\"1.2.0\"}";
        Matcher matcher = Pattern.compile(".*?\"type\":\"startup\".*?").matcher(s);
        if (matcher.find()){
            String group = matcher.group(0);
            String[] split = group.split(",");
            String s1 = split[split.length - 1];
            System.out.println(group);
        }
    }
}
