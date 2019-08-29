package com.higgs.grakn.query;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.hadoop.CompanyEntity4GraknMapred;
import com.higgs.grakn.util.TimeUtil;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.List;

import grakn.client.GraknClient;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlQuery;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static graql.lang.Graql.var;

/**
 * User: JerryYou
 *
 * Date: 2019-08-21
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class Kbquery {

  HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS_KB, "kb");
  GraknClient.Session session =  hgraknClient.getClient().session(hgraknClient.getKeySpace());
  private static Logger logger = LoggerFactory.getLogger(CompanyEntity4GraknMapred.class);


  void query() {
    long st = System.currentTimeMillis();
    // Query
    GraqlGet.Unfiltered unfiltered = Graql.match(
            var("c")
                .isa(Schema.Entity.ENTITY_TYPE.getName())
                .has(Schema.Attribute.NAME.getName(), "沃尔玛")
                .has(Schema.Attribute.CORP_TYPE.getName(), var("1"))
    ).get("1");

    int page = 1;
    int pageSize = 100;
    List<String> names = new ArrayList<>();
    GraknClient.Transaction readTransaction = session.transaction().read();
    while (true) {
      int offset = (page - 1) * pageSize;
      GraqlQuery query = unfiltered.offset(offset).limit(pageSize);
      List<ConceptMap> answers = (List<ConceptMap>) readTransaction.execute(query);
      List<String> tmps = new ArrayList<>();
      answers.forEach(
          answer -> tmps.add(answer.get("1").asAttribute().value().toString())
      );
      if (tmps.size() == 0 || tmps.size() != pageSize) {
        break;
      } else {
        names.addAll(tmps);
      }
      page++;
      System.out.println("page:" + page);
    }

    long et = System.currentTimeMillis();

    System.out.println("total:"+ names.size() + ",cost: "+ TimeUtil.costTime((et - st)/1000) +
        ",st:" + st + ",et:" + et);
    readTransaction.close();
    for (String item : names) {
      logger.info("item:" + item);
    }
  }

  void compute() {

  }

  public static void main( String[] args ) {

    Kbquery query = new Kbquery();

    query.query();
    // transactions, sessions and clients must always be closed
    query.session.close();
    query.hgraknClient.getClient().close();
  }
}
