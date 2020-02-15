/*
package io.transwarp.geo.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

*/
/**
 * created by hanchun on 2020-02-14
 *//*

public class ShowByRegionUDF2 extends GenericUDTF {

  private final int argsCount = 8;
  private final int retCount = 8;
  private ShowByRegionApi showByRegionApi = new ShowByRegionApi();
  private Object forwardColObj[] = new Object[retCount];

  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length != argsCount) {
      throw new UDFArgumentException(new IllegalArgumentException("there must be "
        + argsCount + " parameter for ShowByRegionUDF"));
    }
    //暂时不加type检查

    List<String> outFieldNames = new ArrayList<String>();
    List<ObjectInspector> outFieldOIs = new ArrayList<ObjectInspector>();
    outFieldNames.add("latest_sh_time");
    outFieldNames.add("pass_city");
    outFieldNames.add("pass_time");
    outFieldNames.add("earliest_city_time");
    outFieldNames.add("latest_city_time");
    outFieldNames.add("duration_time");
    outFieldNames.add("speed");
    outFieldNames.add("transportation");
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector));
    outFieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    return ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs);
  }

  public void process(Object[] args) throws HiveException {

    List<Long> timeList = (List<Long>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaLongObjectInspector).getList(args[0]);
    List<Float> lonList = (List<Float>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector).getList(args[1]);
    List<Float> latList = (List<Float>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector).getList(args[2]);
    List<Text> provIdList = (List<Text>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector).getList(args[3]);
    List<Text> areaIdList = (List<Text>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(args[4]);
    List<Text> districtIdList = (List<Text>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(args[5]);
    List<Text> lacList = (List<Text>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(args[6]);
    List<Text> ciList = (List<Text>) ObjectInspectorFactory.getStandardListObjectInspector(
      PrimitiveObjectInspectorFactory.javaStringObjectInspector).getList(args[7]);

    ShowByRegionApi.ListResult result = showByRegionApi.showByRegion(timeList, lonList, latList, provIdList, areaIdList, districtIdList, lacList, ciList);
    forwardColObj[0] = result.getLatest_sh_time();
    forwardColObj[1] = result.getPass_city();
    forwardColObj[2] = result.getPass_time();
    forwardColObj[3] = result.getEarliest_city_time();
    forwardColObj[4] = result.getLatest_city_time();
    forwardColObj[5] = result.getDuration_time();
    forwardColObj[6] = result.getSpeed();
    forwardColObj[7] = result.getTransportation();
    forward(forwardColObj);
  }

  public void close() throws HiveException {
    System.out.println("Close ShowByRegionUDF");
  }

}
*/
