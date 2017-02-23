/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.dynamodb.data

import java.math.BigInteger
import java.net.URI
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.model._
import com.google.common.collect.HashBiMap
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.data.store._
import org.geotools.feature.NameImpl
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.curve.{TimePeriod, Z3SFC}
import org.locationtech.geomesa.index.metadata.{HasGeoMesaMetadata, MetadataStringSerializer}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.dynamodb.data.DynamoDBDataStoreFactory.DynamoDBDataStoreConfig

object DynamoDBDataStore {
  sealed trait FieldSerializer {
    def serialize(o: java.lang.Object): java.lang.Object
    def deserialize(o: java.lang.Object): java.lang.Object
  }
  case object GeomSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = {
      val geom = o.asInstanceOf[Point]
      ByteBuffer.wrap(WKBUtils.write(geom))
    }

    override def deserialize(o: Object): AnyRef = WKBUtils.read(o.asInstanceOf[ByteBuffer].array())
  }

  case object DefaultSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = o
    override def deserialize(o: Object): AnyRef = o
  }

  object FieldSerializer {
    def apply(attrDescriptor: AttributeDescriptor): FieldSerializer = {
      if(classOf[Geometry].isAssignableFrom(attrDescriptor.getType.getBinding)) GeomSerializer
      else DefaultSerializer
    }
  }
}

class DynamoDBDataStore(val client: DynamoDB, val config: DynamoDBDataStoreConfig)
    extends ContentDataStore with HasGeoMesaMetadata[String] {

  override val metadata = new DynamoDBBackedMetadata(client, config.catalog, MetadataStringSerializer)

  override def createFeatureSource(contentEntry: ContentEntry): ContentFeatureSource = null//TODO

  override def getSchema(name: Name): SimpleFeatureType = {
    val sftOpt = metadata.read(name.getLocalPart, "attributes").map(SimpleFeatureTypes.createType(name.getLocalPart, _))
    sftOpt.orNull
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    //TODO
  }

  override def removeSchema(typeName: String): Unit = {
    //TODO
    //session.execute(s"drop table $typeName")
    metadata.delete(typeName)
  }

  override def createContentState(entry: ContentEntry): ContentState = null//TODO

  override def createTypeNames(): util.List[Name] = null//TODO

  override def dispose(): Unit = {
    super.dispose()
  }
}
