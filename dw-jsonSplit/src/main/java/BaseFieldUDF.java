import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
   public String evaluate (String line,String keyStr){
       String[] keysArr = keyStr.split(",");

       //原始日志格式:时间|json日志
       String[] logContent = line.split("\\|");
       //校验一个String类型的变量是否为空时，通常存在3中情况:
       //是否为 null
       //是否为 “”
       //是否为空字符串(引号中间有空格) 如： " "。
       //制表符、换行符、换页符和回车
       //StringUtils的isBlank()方法可以一次性校验这三种情况，返回值都是true,否则为false
       if (logContent.length != 2 || StringUtils.isBlank(logContent[1])){
           return "";
       }

       StringBuffer sb = new StringBuffer();
       try {
           //拼接公共字段
           JSONObject jsonObject = new JSONObject(logContent[1]);
           JSONObject cm = jsonObject.getJSONObject("cm");
           for (int i = 0; i < cm.length(); i++) {
               String key = keysArr[i].trim();
               if (cm.has(key)){
                   sb.append(cm.getString(key)).append("\t");
               }
           }

           //拼接时间字段
           sb.append(jsonObject.getString("et")).append("\t");

           //拼接服务器时间
           sb.append(logContent[0]).append("\t");
       } catch (JSONException e){
           e.printStackTrace();
       }

       return sb.toString();
   }
}
