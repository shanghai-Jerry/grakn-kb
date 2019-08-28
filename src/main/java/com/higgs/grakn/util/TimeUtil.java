package com.higgs.grakn.util;

/**
 * User: JerryYou
 *
 * Date: 2019-08-27
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class TimeUtil {

  public static String costTime(long cost) {
    long hour = cost / 3600;
    long min = (cost - hour * 3600) / 60;
    long second = (cost - hour * 3600 - min * 60);
    return  hour + "时" + min + "分" + second + "秒";
  }
}
