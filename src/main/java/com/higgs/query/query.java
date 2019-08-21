package com.higgs.query;

import com.higgs.HgraknClient;
import com.higgs.Schema;
import com.higgs.Variable;

import java.util.stream.Stream;

import grakn.client.GraknClient;
import grakn.core.concept.answer.ConceptMap;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;

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
public class query {

  public static void main( String[] args ) {
    HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS);
    GraknClient.Session session =  hgraknClient.getClient().session(Variable.KEY_SPACE);
    GraknClient.Transaction readTransaction = session.transaction().read();
    GraqlGet getQuery = Graql.match(
        var("p").isa(Schema.Entity.ENTITY.getName())
            .has(Schema.Attribute.ENTITY_TYPE.getName(), var("pt")),
            var("pt").contains("方向"),
        var("p").has(Schema.Attribute.SCHOOL_CODE.getName(), var("en"))
        ).get().limit(10);
    Stream<ConceptMap> answers = readTransaction.stream(getQuery);
    answers.forEach(
        answer -> System.out.println(answer.get("en").asAttribute().value()));

    // transactions, sessions and clients must always be closed
    readTransaction.close();

    //  transactions, sessions and clients must always be closed
    session.close();
    hgraknClient.getClient().close();
  }
}
