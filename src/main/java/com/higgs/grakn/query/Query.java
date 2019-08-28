package com.higgs.grakn.query;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.util.TimeUtil;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import grakn.client.GraknClient;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlQuery;

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
public class Query {

  HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS, "kb1");
  GraknClient.Session session =  hgraknClient.getClient().session(hgraknClient.getKeySpace());


  void count() {
    long st = System.currentTimeMillis();
    // Query
    GraqlQuery query = Graql.match(
        var("x").isa(Schema.Entity.ENTITY_TYPE.getName())
            .has(Schema.Attribute.NAME.getName(), "java"),
        var("y").isa(Schema.Entity.ENTITY_TYPE.getName())
            .has(Schema.Attribute.NAME.getName(), var("n")),
        var("rel").isa(Schema.RelType.COMPANY_DEPARTMENT.getName())
            .rel(Schema.Relations.DEPARTMENT_IN.getName(),var("x")).rel(
            Schema.Relations.HAS_DEPARTMENT.getName(),var("y"))

    ).get("n").offset(0).limit(100);
    GraknClient.Transaction readTransaction = session.transaction().read();
    Stream<ConceptMap> answers = (Stream<ConceptMap>) readTransaction.stream(query);
    List<String> depts = new ArrayList<>();
    answers.forEach(
        answer ->
            depts.add(answer.get("n").asAttribute().value().toString())
    );

    long et = System.currentTimeMillis();

    System.out.println("total:"+ depts.size() + ",cost: "+ TimeUtil.costTime((et - st)/1000));
    readTransaction.close();

  }

  void query() {
    long st = System.currentTimeMillis();
    // Query
    GraqlGet.Unfiltered unfiltered = Graql.match(
            var("x")
            .isa("entitytype-entity").has("name", var("n"))
    ).get("n");

    int page = 1;
    int pageSize = 100;
    List<String> names = new ArrayList<>();
    GraknClient.Transaction readTransaction = session.transaction().read();
    while (true) {
      int offset = (page - 1) * pageSize;
      GraqlQuery query = unfiltered.offset(offset).limit(pageSize);
      List<ConceptMap> answers = (List<ConceptMap>) readTransaction.execute(query, false);
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

    System.out.println("total:"+ names.size() + ",cost: "+ TimeUtil.costTime((et - st)/1000) +
        ",st:" + st + ",et:" + et);
    readTransaction.close();
  }

  void compute() {

  }

  public static void main( String[] args ) {

    Query query = new Query();

    query.count();
    // transactions, sessions and clients must always be closed
    //  transactions, sessions and clients must always be closed
    query.session.close();
    query.hgraknClient.getClient().close();
  }
}
