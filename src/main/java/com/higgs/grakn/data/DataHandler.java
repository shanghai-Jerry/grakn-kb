package com.higgs.grakn.data;

import com.higgs.grakn.Input;

import java.util.List;
import java.util.concurrent.Callable;

import grakn.client.GraknClient;
import graql.lang.query.GraqlQuery;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * User: JerryYou
 *
 * Date: 2019-08-23
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class DataHandler implements Callable<Long> {

  static Logger logger = LoggerFactory.getLogger(DataHandler.class);

  Input input;
  GraknClient.Session session;
  private List<JsonObject> data;
  long process() {
    int totalLength = this.data.size();
    int count = 0;
    GraknClient.Transaction transaction = session.transaction().write();
    for (JsonObject item : this.data) {
      count++;
      GraqlQuery graqlInsertQuery = input.template(item);
      //
      transaction.execute(graqlInsertQuery);
      if (count % 1000 == 0) {
        logger.info(Thread.currentThread().getName()+" - Executing Graql Insert: " + count + "/" +
            this.data.size());
      }
    }
    // commit when last item finished
    transaction.commit();
    this.data.clear();
    return totalLength;
  }

  public DataHandler(Input input, GraknClient.Session session, List<JsonObject> items) {
      this.data = items;
      this.input = input;
      this.session = session;
  }

  @Override
  public Long call() throws Exception {
    return process();
  }
}
