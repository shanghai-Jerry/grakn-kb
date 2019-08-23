package com.higgs.grakn;

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
import java.util.List;
import java.util.Map;

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

  public  static String PHONE_CALL_KEY_SPACE = "phone_calls";

  public  static String KEY_SPACE = "kb";

  public static Map<Integer, String> relsId2nameMap;

  public static  String stringFormatSql(String var) {
    return "\"" + var + "\"";
  }

  public static String dirFormat(String dataDir) {
    if (!dataDir.endsWith("/")) {
      dataDir = dataDir + "/";
    }
    return dataDir;
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
