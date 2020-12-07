import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldTypes = new ArrayList<>();
//        自定义输出数据的列名1和列类型：evevt_name String
        fieldNames.add("evevt_name");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//        自定义输出数据的列名2和列类型：event_json String
        fieldNames.add("event_json");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldTypes);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //获取输入数据
        String input = args[0].toString();
        if (StringUtils.isBlank(input)){
            return;
        }else {
            try {
                JSONArray jsonArray = new JSONArray(input);
                String[] result = new String[2];

                //事件字段格式：[json,json,...] 即JSONArray
                for (int i = 0; i < jsonArray.length(); i++) {
                    try {
                        //获取事件
                        result[0] = jsonArray.getJSONObject(i).getString("en");
                        //事件数组：事件详细信息
                        result[1] = jsonArray.getString(i);
                        //返回2列数据
                        forward(result);
                    }catch (JSONException e){
                        //防止因为某个数据的错误结束整个循环
                        continue;
                    }
                }

            } catch (JSONException e){
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException { }

    public static void main(String[] args) {
        EventJsonUDTF e = new EventJsonUDTF();

    }
}
