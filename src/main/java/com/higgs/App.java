package com.higgs;

import com.higgs.grakn.client.HgraknClient;
import com.higgs.grakn.client.schema.Schema;
import com.higgs.grakn.variable.Variable;

import grakn.client.GraknClient;
import graql.lang.Graql;
import graql.lang.query.GraqlInsert;

import static graql.lang.Graql.var;

/**
 * Hello world!
 *
 */
public class App {
    // insert all new data
    public void insert(GraknClient.Session session) {
        // Insert a person using a WRITE transaction
        GraknClient.Transaction writeTransaction = session.transaction().write();
        // 如何封装Statement？
        GraqlInsert query = Graql.insert(
            var("e").isa(Schema.Entity.ENTITY.getName()).has(Schema.Attribute.NAME.getName(),
                "c++").has(Schema.Attribute.ENTITY_TYPE.getName(),var("x")),
            var("x").isa(Schema.Attribute.ENTITY_TYPE.getName()).val("技能,方向"),
            var("e1").isa(Schema.Entity.ENTITY.getName()).has(Schema.Attribute.NAME.getName(), "服务端语言"),
            var("rel").isa(Schema.RelType.ENTITY_REL.getName()).rel(Schema.Relations
                .SKILL_KEYWORD.getName(), "e1").rel(Schema.Relations.KEYWORD_SKILL.getName(), "e")
        );
        writeTransaction.execute(query);
        // to persist changes, a write transaction must always be committed (closed)
        writeTransaction.commit();
    }

    public void match_insert(GraknClient.Session session) {
        // Insert a person using a WRITE transaction
        GraknClient.Transaction writeTransaction = session.transaction().write();
        GraqlInsert graqlInsert = Graql.match(
            var("p").isa(Schema.Entity.ENTITY.getName())
                .has(Schema.Attribute.ENTITY_TYPE.getName(), var("pt"))
                .has(Schema.Attribute.NAME.getName(), var("en")),
            var("pt").contains("方向")
        ).insert(
          var("p").has(Schema.Attribute.SCHOOL_CODE.getName(), "1000001")
        );
        writeTransaction.execute(graqlInsert);
        // to persist changes, a write transaction must always be committed (closed)
        writeTransaction.commit();

    }

    public static void main( String[] args ) {
        App app = new App();
        HgraknClient hgraknClient = new HgraknClient(Variable.GRAKN_ADDRESS,  Variable.KEY_SPACE);
        GraknClient.Session session =  hgraknClient.getClient().session(Variable.KEY_SPACE);
        //  transactions, sessions and clients must always be closed
        app.match_insert(session);
        session.close();
        hgraknClient.getClient().close();
    }

}
