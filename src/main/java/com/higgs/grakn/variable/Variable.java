package com.higgs.grakn.variable;

import com.higgs.grakn.client.schema.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * User: JerryYou
 *
 * Date: 2019-08-21
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class Variable {

  public  static String GRAKN_ADDRESS = "172.20.0.8:48555";

  public  static String GRAKN_ADDRESS_KB = "172.20.0.9:48555";

  public  static String LOCAL_GRAKN_ADDRESS_KB = "127.0.0.1:48555";

  public  static String PHONE_CALL_KEY_SPACE = "phone_calls";

  public  static String KEY_SPACE = "kb1";

  public static  String stringFormatSql(String var) {
    return "\"" + var + "\"";
  }

  public static List<String> entityTypeList = Arrays.asList(
      Schema.Entity.KEYWORD.getName(),
      Schema.Entity.JOB_FUNCTION.getName(),
      Schema.Entity.DIRECTION.getName(),
      Schema.Entity.INDUSTRY.getName(),
      Schema.Entity.SKILL.getName(),
      Schema.Entity.TOPIC.getName(),
      Schema.Entity.CERTIFICATE.getName(),
      Schema.Entity.MAJOR.getName(),
      Schema.Entity.SCHOOL.getName(),
      Schema.Entity.COMPANY.getName(),
      Schema.Entity.MAJOR_CATEGORY.getName(),
      Schema.Entity.MAJOR_DISCIPLINE.getName(),
      Schema.Entity.LOCATION.getName(),
      Schema.Entity.KNOW_NOT_RECOGNIZE.getName(),
      Schema.Entity.CONSENSUS.getName(),
      Schema.Entity.JOB_TITLE.getName(),
      Schema.Entity.DEPARTMENT.getName(),
      Schema.Entity.JOB_RANK.getName(),
      Schema.Entity.ATTRIBUTE.getName(),
      Schema.Entity.IT_ORANGE_INDUSTRY.getName()
  );


  public static String dirFormat(String dataDir, boolean isDir) {
    if (!dataDir.endsWith("/") && isDir) {
      dataDir = dataDir + "/";
    }
    return dataDir;
  }

  public static String dirFormat(String dataDir) {
    if (!dataDir.endsWith("/")) {
      dataDir = dataDir + "/";
    }
    return dataDir;
  }

  public static String getVarValue(String type ,String key)  {
    MessageDigest md = null;
    String var = type + "-" + getMD5(key);
    return var;
  }

  public static String getRelVarValue(String relType ,String... key)  {
    MessageDigest md = null;
    String var = relType;
    List<String> keys = Arrays.asList(key);
    for (int i = 0; i < keys.size();i ++) {
      var = var + "-" + getMD5(keys.get(i));
    }
    return var;
  }

  public static String getMD5(String key)  {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    try {
      md.update(key.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    String md5 = new BigInteger(1, md.digest()).toString(16);
    return md5;
  }


  /**
   * 深拷贝， 引用对象独立，互不影响
   * @param src 源数组
   * @param <T> 支持实体泛型，继承自EntityNode
   * @return 目标数组
   */
  public static <T> List<T> deepCopy(List<T> src) {

    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(byteOut);
      out.writeObject(src);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
    ObjectInputStream in = null;
    List<T> dest = new ArrayList<>();
    try {
      in = new ObjectInputStream(byteIn);
      dest = (List<T>) in.readObject();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return dest;
  }


}
