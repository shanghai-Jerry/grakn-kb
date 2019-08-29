package com.higgs.grakn.query;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.hadoop.CompanyEntity4GraknMapred;
import com.higgs.grakn.util.TimeUtil;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.List;

import grakn.client.GraknClient;
import grakn.core.concept.answer.Numeric;
import graql.lang.Graql;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

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
    String query = "match\n" + "  $1 isa entitytype-entity, has name \"世界五百强\";\n" + "  $2 isa " +
        "entitytype-entity, has name \"沃尔玛\";\n" + "  $4 (company-corptype:$1," +
        "corptype-company:$2)" +
        " isa company-corp-type;\n" + " get ;count;";
    int page = 1;
    int pageSize = 100;
    GraknClient.Transaction readTransaction = session.transaction().read();
      int offset = (page - 1) * pageSize;
      String finalQuery = query + " offset " + offset + "; limit " + pageSize + ";";
      List<Numeric> answers = readTransaction.execute(Graql.parse(query).asGetAggregate());
      List<String> tmps = new ArrayList<>();

      answers.forEach(
          answer -> logger.info("count:" + answer.number().intValue())
      );
    long et = System.currentTimeMillis();

    System.out.println(",cost: "+ TimeUtil.costTime(st,et) +",st:" + st + ",et:" + et);
    readTransaction.close();
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
