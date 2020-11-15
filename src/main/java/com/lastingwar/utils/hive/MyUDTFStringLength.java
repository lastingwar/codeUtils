package com.lastingwar.utils.hive;

import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yhm
 * @create 2020-11-15 20:24
 */
public class MyUDTFStringLength extends GenericUDTF {

    private transient ObjectInspector inputOI = null;
    @Override
    public void close() throws HiveException {
    }

    /**
     * 确定输出的列名和类型
     * @param args 传入的参数
     * @return 鉴别器(列名数组和 列类型数组)
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("explode() takes only one argument");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        switch (args[0].getCategory()) {
            case LIST:
                inputOI = args[0];
                fieldNames.add("col");
                fieldOIs.add(((ListObjectInspector)inputOI).getListElementObjectInspector());
                break;
            case MAP:
                inputOI = args[0];
                fieldNames.add("key");
                fieldNames.add("value");
                fieldOIs.add(((MapObjectInspector)inputOI).getMapKeyObjectInspector());
                fieldOIs.add(((MapObjectInspector)inputOI).getMapValueObjectInspector());
                break;
            default:
                throw new UDFArgumentException("explode() takes an array or a map as a parameter");
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    private transient final Object[] forwardListObj = new Object[1];
    private transient final Object[] forwardMapObj = new Object[2];

    /**
     * 函数逻辑 结果使用forward(forwardListObj)循环传出
     * @param o 传入参数以长度为1数组的形式
     * @throws HiveException
     */
    @Override
    public void process(Object[] o) throws HiveException {
        switch (inputOI.getCategory()) {
            case LIST:
                ListObjectInspector listOI = (ListObjectInspector)inputOI;
                List<?> list = listOI.getList(o[0]);
                if (list == null) {
                    return;
                }
                for (Object r : list) {
                    forwardListObj[0] = r;
                    forward(forwardListObj);
                }
                break;
            case MAP:
                MapObjectInspector mapOI = (MapObjectInspector)inputOI;
                Map<?,?> map = mapOI.getMap(o[0]);
                if (map == null) {
                    return;
                }
                for (Map.Entry<?,?> r : map.entrySet()) {
                    forwardMapObj[0] = r.getKey();
                    forwardMapObj[1] = r.getValue();
                    forward(forwardMapObj);
                }
                break;
            default:
                throw new TaskExecutionException("explode() can only operate on an array or a map");
        }
    }
}
