package com.higgs.grakn.query;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.util.TimeUtil;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.List;

import grakn.client.GraknClient;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;

/**
 * User: JerryYou
 *
 * Date: 2019-08-21
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class Query {

  HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS, "kb1");
  GraknClient.Session session =  hgraknClient.getClient().session(hgraknClient.getKeySpace());

  void query() {
    long st = System.currentTimeMillis();
    String query = "match\n" + "\n" + " $1 isa entitytype-entity, has name \"世界五百强\";\n" + " $2 " +
        "isa entitytype-entity, has name $3;\n" + " $4 (company-corptype:$1,corptype-company:$2) isa company-corp-type;\n" + " get $3;";
    // Query
    int page = 1;
    int pageSize = 100;
    List<String> names = new ArrayList<>();
    GraknClient.Transaction readTransaction = session.transaction().read();
    while (true) {
      int offset = (page - 1) * pageSize;
      query += "offset " + offset + "; limit " + pageSize + ";";
      List<ConceptMap> answers = readTransaction.execute((GraqlGet) Graql.parse(query));
      List<String> tmps = new ArrayList<>();
      answers.forEach(
          answer -> tmps.add(answer.get("n").asAttribute().value().toString())
      );
      if (tmps.size() == 0) {
        break;
      } else {
        names.addAll(tmps);
      }
      page++;
      System.out.println("page:" + page);
    }

    long et = System.currentTimeMillis();

    System.out.println("total:"+ names.size() + ",cost: "+ TimeUtil.costTime(st, et) +
        ",st:" + st + ",et:" + et);
    readTransaction.close();
  }

  void compute() {

  }

  public static void main( String[] args ) {

    Query query = new Query();

    query.query();
    // transactions, sessions and clients must always be closed
    //  transactions, sessions and clients must always be closed
    query.session.close();
    query.hgraknClient.getClient().close();
  }
}
