/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.nio.charset.StandardCharsets
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.document.utils._
import org.locationtech.geomesa.index.metadata.{CachedLazyMetadata, GeoMesaMetadata, MetadataSerializer, MetadataAdapter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.{CloseQuietly, CloseWithLogging}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import org.locationtech.geomesa.index.index.IndexAdapter

class DynamoDBBackedMetadata[T](val client: DynamoDB, val catalog: String, val serializer: MetadataSerializer[T])
    extends GeoMesaMetadata[T] with CachedLazyMetadata[T] with DynamoDBMetadataAdapter

trait DynamoDBMetadataAdapter extends MetadataAdapter {

	def client: DynamoDB
  def catalog: String

  override protected def checkIfTableExists: Boolean = {
	  try {
	    client.getTable(catalog).describe
	    true
	  } catch {
	    case e:ResourceNotFoundException =>
	      false
	  }
  }

  override protected def createTable(): Unit = {
    if(!checkIfTableExists) {
      client.createTable(
          catalog,
          java.util.Arrays.asList(new KeySchemaElement("feature", KeyType.HASH)),
          java.util.Arrays.asList(new AttributeDefinition("feature", ScalarAttributeType.S)),
          new ProvisionedThroughput())
    }
  }

  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    val items = new TableWriteItems(catalog)
    rows.foreach { case (key, value) =>
      items.addItemToPut(new Item().withPrimaryKey(wrap(key), wrap(value)))
    }
    client.batchWriteItem(items)
  }

  override protected def delete(row: Array[Byte]): Unit = delete(Seq(row))

  override protected def delete(rows: Seq[Array[Byte]]): Unit = {
    val items = new TableWriteItems(catalog)
    rows.foreach { case (key) =>
      items.withHashOnlyKeysToDelete(wrap(key))
    }
    client.batchWriteItem(items)
  }

  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    val spec = new ScanSpec().
      withFilterExpression("#k == :k").
      withNameMap(new NameMap().`with`("#k", "key")).
      withValueMap(new ValueMap().withString(":k", wrap(row)))

    val rows = client.getTable(catalog).scan(spec)
    if(rows.size < 1) {
      None
    } else {
      Some(rows.head.getString("value").getBytes(StandardCharsets.UTF_8))
    }
  }

  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[Array[Byte]] = {
    val spec = new ScanSpec
    val rows = prefix match {
      case None =>
        client.getTable(catalog).scan(spec)
      case Some(p) =>
        val start = wrap(p)
        val end = wrap(IndexAdapter.rowFollowingPrefix(p))
        spec.
          withFilterExpression("#k >= :start and #k < :end").
          withNameMap(new NameMap().`with`("#k", "key")).
          withValueMap(new ValueMap().withString(":start", start).withString(":end", end))
        client.getTable(catalog).scan(spec)
    }

    CloseableIterator(rows.map(_.getString("key").getBytes(StandardCharsets.UTF_8)).iterator)
  }

  override def close(): Unit = None

  private def wrap(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
}
