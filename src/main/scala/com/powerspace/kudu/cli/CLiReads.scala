package com.powerspace.kudu.cli

import java.text.NumberFormat
import java.util.Locale

import com.powerspace.kudu.HashedKey

import scala.util.Try

/**
  * Simple cli reads for kudu managment
  */
object CLiReads {
  implicit val listRead: scopt.Read[List[String]] =
    scopt.Read.reads(x => x.split(',').toList)

  implicit val anyRead: scopt.Read[Either[Number, String]] =
    scopt.Read.reads(x => {
      Try(NumberFormat.getNumberInstance(Locale.ENGLISH).parse(x)).map(Left(_)).
        getOrElse(Right(x))
    })

  implicit val keysRead: scopt.Read[List[HashedKey]] =
    scopt.Read.reads(x => x.split(',').toList
      .map(arr => arr.split(':').toList match {
        case List(key) => HashedKey(key)
        case List(key, bucket) => HashedKey(key, bucket.toInt)
      }))

}
