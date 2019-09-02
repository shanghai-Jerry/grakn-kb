package com.higgs.grakn.query;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.hadoop.CompanyEntity4GraknMapred;
import com.higgs.grakn.util.TimeUtil;
import com.higgs.grakn.variable.Variable;

import java.util.ArrayList;
import java.util.List;

import grakn.client.GraknClient;
import grakn.core.concept.ConceptId;
import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import graql.lang.query.GraqlQuery;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static graql.lang.Graql.match;
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

  String getEId(GraknClient.Transaction readTransaction,String name, String target) {
    long st = System.currentTimeMillis();
    String ret = "";
    String nameVar = Variable.getVarValue("1", name);
    String targetVar = Variable.getVarValue("1", target);
    GraqlQuery query = match(
        var(nameVar).isa(Schema.Entity.ENTITY.getName())
        .has(Schema.Attribute.NAME.getName(), name),
        var(targetVar).isa(Schema.Entity.ENTITY.getName())
            .has(Schema.Attribute.NAME.getName(), target)
    ).get();
    List<ConceptMap> answers = (List<ConceptMap>) readTransaction.execute(query);
    String id2Name;
    String id2Traget;
    logger.info("answer size:" + answers.size());
    if (answers.size() < 0) {
      return  "";
    }
    id2Name = answers.get(0).get(nameVar).id().toString();
    id2Traget = answers.get(0).get(targetVar).id().toString();
    long et = System.currentTimeMillis();
    System.out.println("get id cost: "+ TimeUtil.costTime(st,et) +",st:" + st + ",et:" + et);
    String retVar = id2Name + "," + id2Traget;
    logger.info("get ids:" + retVar);
    return  retVar;
  }

  void queryCompute(GraknClient.Transaction readTransaction, String name, String target) {

    long st = System.currentTimeMillis();
    String id = getEId(readTransaction, name, target);
    String []ids = id.split(",");
    if (ids.length != 2) {
      logger.info("not path");
      return;
    }
    List<ConceptList> answers = readTransaction.execute(Graql.compute().path().from(ids[0]).to(ids[1]));
    List<ConceptId> conceptIds = answers.get(0).list();
    logger.info("path is:");
    for (ConceptId conceptId : conceptIds) {
      logger.info("id:" + conceptId);
    }
    long et = System.currentTimeMillis();

    System.out.println("cost: "+ TimeUtil.costTime(st,et) +",st:" + st + ",et:" + et);
    readTransaction.close();
  }

  void queryAggregate() {
    long st = System.currentTimeMillis();
    String query = "compute count in entity-type-entity;";
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

    System.out.println("cost: "+ TimeUtil.costTime(st,et) +",st:" + st + ",et:" + et);
    readTransaction.close();
  }

  void query() {
    long st = System.currentTimeMillis();
    String query = "match $1 isa entitytype-entity, has name \"幼儿园教师资格\", has cert-code $2; get;\n";
    int page = 1;
    int pageSize = 100;
    GraknClient.Transaction readTransaction = session.transaction().read();
    int offset = (page - 1) * pageSize;
    String finalQuery = query + " offset " + offset + "; limit " + pageSize + ";";
    List<ConceptMap> answers = readTransaction.execute((GraqlGet) Graql.parse(query));

    answers.forEach(
        answer -> logger.info("item:" + answer.get("2").asAttribute().value().toString())
    );
    long et = System.currentTimeMillis();

    System.out.println("cost: "+ TimeUtil.costTime(st,et) +",st:" + st + ",et:" + et);
    readTransaction.close();
  }

  void match_query_insert() {
    long st = System.currentTimeMillis();
    int page = 1;
    int pageSize = 100;
    GraknClient.Transaction writeTransaction = session.transaction().write();
    try {
      GraqlQuery graqlQuery = Graql.match(
          var("c").isa(Schema.Entity.ENTITY.getName())
          .has("name", "沃尔玛2")
      ).insert(
          var("c").has("code", 100)
      );
      writeTransaction.execute(graqlQuery);
    } catch (Exception e) {
      logger.info("[Insert commit error] =>" + e.getMessage());
    } finally {
      writeTransaction.close();
    }

    long et = System.currentTimeMillis();

    System.out.println("cost: "+ TimeUtil.costTime(st,et) +",st:" + st + ",et:" + et);

  }

  public static void main( String[] args ) {

    Kbquery query = new Kbquery();
    GraknClient.Transaction writeTransaction = query.session.transaction().write();
    GraknClient.Transaction readTransaction = query.session.transaction().read();
    query.queryCompute(readTransaction,"速卖通", "bat");
    // transactions, sessions and clients must always be closed
    query.session.close();
    query.hgraknClient.getClient().close();
  }
}
