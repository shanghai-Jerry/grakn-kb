package com.higgs.query;

import com.higgs.HgraknClient;
import com.higgs.grakn.Variable;

import java.util.stream.Stream;

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
public class query {

  public static void main( String[] args ) {
    HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS);
    GraknClient.Session session =  hgraknClient.getClient().session(Variable.PHONE_CALL_KEY_SPACE);
    GraknClient.Transaction readTransaction = session.transaction().read();
    String sql = "match\n" + "  $customer isa person, has first-name $na;\n" + "  $company isa " +
        "company, has name " +
        "\"Telecom\";\n" + "  $contract (customer: $customer, provider: $company) isa contract;\n" +
        "  $target isa person, has phone-number \"+86 921 547 9004\", has first-name $tna;\n" +
        "  $call (caller: $customer, callee: $target) isa call, has started-at $started-at;\n" +
        "  $min-date == 2018-09-14T17:18:49; $started-at > $min-date;\n" +
        "get $customer, $company, $contract,$call, $target,$na, $tna;";
    GraqlGet getQuery = Graql.parse(sql).asGet();
    Stream<ConceptMap> answers = readTransaction.stream(getQuery);
    answers.forEach(
        answer -> System.out.println("name:"+answer.get("tna").asAttribute().value().toString() +
            ",customer name:"+ answer.get("na").asAttribute().value().toString()));

    // transactions, sessions and clients must always be closed
    readTransaction.close();

    //  transactions, sessions and clients must always be closed
    session.close();
    hgraknClient.getClient().close();
  }
}
