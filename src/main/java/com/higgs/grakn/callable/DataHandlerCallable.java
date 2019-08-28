package com.higgs.grakn.callable;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.migration.Input;

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
public class DataHandlerCallable implements Callable<Long> {

  static Logger logger = LoggerFactory.getLogger(DataHandlerCallable.class);

  Input input;
  HgraknClient hgraknClient;
  private List<JsonObject> data;
  long process() {

    int totalLength = this.data.size();
    int count = 0;
    int finished = 0;
    GraknClient.Session session = hgraknClient.NewSession(hgraknClient.getKeySpace());
    GraknClient.Transaction transaction = session.transaction().write();
    try {
      for (JsonObject item : this.data) {
        count++;
        finished++;
        GraqlQuery graqlInsertQuery = input.template(item);
        transaction.execute(graqlInsertQuery);
        if (count % 10000 == 0) {
          logger.info(Thread.currentThread().getName()+" - Executing Graql Insert: " +
              finished + "/" + this.data.size());
          transaction.commit();
          transaction = session.transaction().write();
          count = 0;
        }
      }
      if (count > 0) {
          transaction.commit();
      }
    } catch (Exception e) {

      logger.info("[commit error] => " + e.getMessage());
    } finally {
      session.close();
    }
    // commit when last item finished
    this.data.clear();
    return totalLength;
  }

  public DataHandlerCallable(Input input, HgraknClient hgraknClient, List<JsonObject> items) {
      this.data = items;
      this.input = input;
      this.hgraknClient = hgraknClient;
  }

  @Override
  public Long call() throws Exception {
    return process();
  }
}
