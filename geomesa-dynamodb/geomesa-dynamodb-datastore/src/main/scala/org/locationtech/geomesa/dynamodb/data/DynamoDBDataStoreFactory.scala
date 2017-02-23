/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.dynamodb.data

import java.io.Serializable

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditReader, AuditWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties

import scala.collection.JavaConversions._
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model._
import org.locationtech.geomesa.dynamodb.data.DynamoDBDataStoreFactory.DynamoDBDataStoreConfig
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient

class DynamoDBDataStoreFactory extends DataStoreFactorySpi {

  import DynamoDBDataStoreFactory.Params._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]) = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): DynamoDBDataStore = {
    import GeoMesaDataStoreFactory.RichParam

    def getOrCreateCatalogTable(dynamoDB: DynamoDB, table: String): String = {
      val tables = dynamoDB.listTables().iterator().toList
      val ret = tables
        .find(_.getTableName == table)
        .getOrElse(
          dynamoDB.createTable(
            table,
            java.util.Arrays.asList(new KeySchemaElement("feature", KeyType.HASH)),
            java.util.Arrays.asList(new AttributeDefinition("feature", ScalarAttributeType.S)),
            new ProvisionedThroughput()))//TODO
      ret.waitForActive
      ret.getTableName
    }

    val docClient = new DynamoDB(new AmazonDynamoDBClient())
    val catalog = getOrCreateCatalogTable(docClient, TableNameParam.lookup[String](params))
    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](params)
    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](params)
    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](params)
    val queryTimeout = QueryTimeoutParam.lookupWithDefault[Option[Long]](params)
    val caching = CachingParam.lookupWithDefault[Boolean](params)

    val config = DynamoDBDataStoreConfig(catalog, looseBBox, queryThreads, generateStats, queryTimeout, caching)

    new DynamoDBDataStore(docClient, config)
  }

  override def getDisplayName = DynamoDBDataStoreFactory.DISPLAY_NAME

  override def getDescription = DynamoDBDataStoreFactory.DESCRIPTION

  override def getParametersInfo: Array[Param] =
    Array(TableNameParam, LooseBBoxParam, QueryThreadsParam, GenerateStatsParam, QueryTimeoutParam, CachingParam)

  def canProcess(params: java.util.Map[String,Serializable]) = params.containsKey(TableNameParam.key)

  override def isAvailable = true

  override def getImplementationHints = null
}

object DynamoDBDataStoreFactory {

  val DISPLAY_NAME = "DynamoDB (GeoMesa)"
  val DESCRIPTION = "Amazon Web Service DynamoDB\u2122 fully managed NoSQL database"

  object Params {
    val TableNameParam         = new Param("tableName", classOf[String], "DynamoDB catalog table name", true)
    val LooseBBoxParam         = GeoMesaDataStoreFactory.LooseBBoxParam
    val QueryThreadsParam      = GeoMesaDataStoreFactory.QueryThreadsParam
    val GenerateStatsParam     = GeoMesaDataStoreFactory.GenerateStatsParam
    val QueryTimeoutParam      = GeoMesaDataStoreFactory.QueryTimeoutParam
    val CachingParam           = GeoMesaDataStoreFactory.CachingParam
  }

  case class DynamoDBDataStoreConfig(catalog: String,
    looseBBox: Boolean,
    queryThreads: Int,
    generateStats: Boolean,
    queryTimeout: Option[Long],
    cachingParam: Boolean) //TODO extends GeoMesaDataStoreConfig
}