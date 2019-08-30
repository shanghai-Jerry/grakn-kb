package com.higgs.grakn.task;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.migration.DataMigration;
import com.higgs.grakn.client.migration.EntityInput;
import com.higgs.grakn.client.migration.Input;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * User: JerryYou
 *
 * Date: 2019-08-27
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class KbDataMigrationLocal extends DataMigration {
  static Logger logger = LoggerFactory.getLogger(KbDataMigrationLocal.class);

  public static void main(String[] args) {

    String dir = Variable.dirFormat("/Users/devops/workspace/kb/kb_system", true);
    String KEY_SPACE = "kb";
    String graknServer = "172.20.0.9:48555";
    // init variable
    KbDataMigrationLocal kbDataMigration = new KbDataMigrationLocal();
    HgraknClient hgraknClient = new HgraknClient(graknServer, KEY_SPACE);
    kbDataMigration.setHgraknClient(hgraknClient);
    kbDataMigration.setBatchSize(10000);
    Collection<Input> inputs = new ArrayList<>();
    List<EntityInput> entityInputs = Arrays.asList(
      new EntityInput(dir + "kb_entity_entity_type.csv", Schema.Entity.ENTITY_TYPE_ENTITY.getName()),
        new EntityInput(dir + "kb_entity_school_type.csv", Schema.Entity.SCHOOL_TYPE_ENTITY.getName())
    );
    inputs.addAll(entityInputs);
    kbDataMigration.connectAndMigrate(inputs);

  }

}
