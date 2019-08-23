package com.higgs.grakn;

import java.util.Collection;

/**
 * User: JerryYou
 *
 * Date: 2019-08-23
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class Demo {

  public static void main(String[] args) {
    DataMigration dataMigration = new DataMigration();
    Collection<Input> inputs = dataMigration.initialiseInputs
        ("/Users/devops/workspace/kb/phone_calls");
    dataMigration.defaultConnectAndMigrate(inputs);
  }
}
