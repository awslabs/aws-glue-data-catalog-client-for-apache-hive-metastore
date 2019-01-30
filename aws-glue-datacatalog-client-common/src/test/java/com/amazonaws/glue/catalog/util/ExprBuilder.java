package com.amazonaws.glue.catalog.util;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Utility to craft mock expression trees. Inspired by org.apache.hadoop.hive.metastore.TestMetastoreExpr
 */
public class ExprBuilder {

  private final String tblName;
  private final Stack<ExprNodeDesc> stack = new Stack<>();

  public ExprBuilder(String tblName) {
    this.tblName = tblName;
  }

  public ExprNodeGenericFuncDesc build() throws Exception {
    if (stack.size() != 1) {
      throw new Exception("Bad test: " + stack.size());
    }
    return (ExprNodeGenericFuncDesc) stack.pop();
  }

  public ExprBuilder pred(String name, int args) throws Exception {
    return fn(name, TypeInfoFactory.booleanTypeInfo, args);
  }

  private ExprBuilder fn(String name, TypeInfo ti, int args) throws Exception {
    List<ExprNodeDesc> children = new ArrayList<>();
    for (int i = 0; i < args; ++i) {
      children.add(stack.pop());
    }
    stack.push(new ExprNodeGenericFuncDesc(ti, FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
    return this;
  }

  public ExprBuilder strCol(String col) {
    return colInternal(TypeInfoFactory.stringTypeInfo, col, true);
  }

  public ExprBuilder timestampCol(String col) {
    return colInternal(TypeInfoFactory.timestampTypeInfo, col, true);
  }

  public ExprBuilder booleanCol(String col) {
    return colInternal(TypeInfoFactory.booleanTypeInfo, col, true);
  }

  public ExprBuilder charCol(String col) {
    return colInternal(TypeInfoFactory.charTypeInfo, col, true);
  }

  public ExprBuilder dateCol(String col) {
    return colInternal(TypeInfoFactory.dateTypeInfo, col, true);
  }

  private ExprBuilder colInternal(TypeInfo ti, String col, boolean part) {
    stack.push(new ExprNodeColumnDesc(ti, col, tblName, part));
    return this;
  }

  public ExprBuilder val(boolean val) {
    return valInternal(TypeInfoFactory.booleanTypeInfo, val);
  }

  public ExprBuilder val(String val) {
    return valInternal(TypeInfoFactory.stringTypeInfo, val);
  }

  public ExprBuilder vals(List<String> vals) {
    for (String s : vals) {
      val(s);
    }
    return this;
  }

  public ExprBuilder val(Timestamp val) {
    return valInternal(TypeInfoFactory.timestampTypeInfo, val);
  }

  public ExprBuilder val(Character val) {
    return valInternal(TypeInfoFactory.charTypeInfo, val);
  }

  public ExprBuilder val(Date val) {
    return valInternal(TypeInfoFactory.timestampTypeInfo, val);
  }

  private ExprBuilder valInternal(TypeInfo ti, Object val) {
    stack.push(new ExprNodeConstantDesc(ti, val));
    return this;
  }

}
