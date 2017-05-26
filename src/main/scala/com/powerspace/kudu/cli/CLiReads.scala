package com.powerspace.kudu.cli

import com.powerspace.kudu.HashedKey

/**
  * Simple cli reads for kudu managment
  */
object CLiReads {
  implicit val listRead: scopt.Read[List[String]] =
    scopt.Read.reads(x => x.split(',').toList)

  implicit val keysRead: scopt.Read[List[HashedKey]] =
    scopt.Read.reads(x => x.split(',').toList
      .map(arr => arr.split(':').toList match {
        case List(key) => HashedKey(key)
        case List(key, bucket) => HashedKey(key, bucket.toInt)
      }))

}
