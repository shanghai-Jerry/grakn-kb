package com.higgs.grakn.task;

import com.csvreader.CsvReader;
import com.higgs.grakn.util.FileUtils;
import com.higgs.grakn.variable.Variable;

import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User: JerryYou
 *
 * Date: 2019-08-30
 *
 * Copyright (c) 2018 devops
 *
 * <<licensetext>>
 */
public class EntityTypeAttributeFormat {

  static Logger logger = LoggerFactory.getLogger(EntityTypeAttributeFormat.class);

  public String valuesFormat(String [] values) {
    return  "\"" + values[0] +"\","+ "\"" + values[1] +"\"," + "\"" + values[2] +"\","+"\"" +
        values[3] +"\"";
  }

  public void convertRelation(String path, String file) {
    // 关系input
    List<List<String>> relations = new ArrayList<>();
    Map<String, Boolean> distinct = new HashedMap();

    try {
      CsvReader csvReader = new CsvReader(KbParseData.getReader(path + "/" + file) );
      for (int i = 0; i  <= 42; i++) {
        relations.add(new ArrayList<>());
      }
      while(csvReader.readRecord()) {
        String [] values = csvReader.getValues();
        if (values.length != 4) {
          continue;
        }
        int type = Integer.parseInt(values[2]);
        List<String> relationsIndex = relations.get(type);
        String key = Variable.getRelVarValue(values[2], values[0], values[1]);
        String key2 = Variable.getRelVarValue(values[2], values[1], values[0]);
        if (!distinct.containsKey(key)) {
          relationsIndex.add(valuesFormat(values));
          relations.set(type,relationsIndex);
          distinct.put(key, true);
          distinct.put(key2, true);
        }
      }
    } catch (FileNotFoundException e) {
      logger.info("[NoFile] =>" + e.getMessage());
    } catch (IOException e) {
      logger.info("[IOExp] =>" + e.getMessage());
    }
    for (int i = 0; i  <= 42; i++) {
      FileUtils.saveFileToCsv(path + "/relation_" + String.valueOf(i) + ".csv", relations.get(i),
          false);
    }
  }

  public void convert(String path, String outPut) {
    Map<String, List<String>> typeMap = new HashMap<>();
    try {
      CsvReader csvReader = new CsvReader(KbParseData.getReader(path));
      while(csvReader.readRecord()) {
        String [] values = csvReader.getValues();
        if (values.length != 2) {
          continue;
        }
        String name = values[0];
        String type = values[1];
        List<String> types =  typeMap.getOrDefault(name, new ArrayList<>());
        types.add(Variable.entityTypeList.get(Integer.parseInt(type)));
        typeMap.put("\"" + name + "\"", types);
      }
    } catch (FileNotFoundException e) {
      logger.info("[NoFile] =>" + e.getMessage());
    } catch (IOException e) {
      logger.info("[IOExp] =>" + e.getMessage());
    }
    Set<Map.Entry<String, List<String>>> entrySet = typeMap.entrySet();
    Iterator<Map.Entry<String, List<String>>> iterator = entrySet.iterator();
    List<String> typesList = new ArrayList<>();
    while (iterator.hasNext()) {
      Map.Entry<String, List<String>> entry = iterator.next();
      String key = entry.getKey();
      List<String> values = entry.getValue();
      int i = 0;
      StringBuilder stringBuilder = new StringBuilder();
      for (String value : values) {
        if (i == 0) {
          stringBuilder.append(value);
        } else {
          stringBuilder.append("," + value);
        }
        i++;
      }
      typesList.add("\"" + key + "\",\"" + stringBuilder.toString() +"\",\"1\"");
    }
    FileUtils.saveFileToCsv(outPut, typesList, false);
  }

  public static void main(String[] args) {
    EntityTypeAttributeFormat entityTypeAttributeFormat = new EntityTypeAttributeFormat();
    /*entityTypeAttributeFormat.convert("/Users/devops/workspace/kb/kb_system/entity-type.csv",
        "/Users/devops/workspace/kb/kb_system/entity-type-format.csv");
    */
    entityTypeAttributeFormat.convertRelation("/Users/devops/workspace/kb/kb_system", "kb_relation_1567074526188.txt");
  }
}
