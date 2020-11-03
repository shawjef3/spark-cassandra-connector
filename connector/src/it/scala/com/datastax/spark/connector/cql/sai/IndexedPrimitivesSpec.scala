package com.datastax.spark.connector.cql.sai

import com.datastax.spark.connector.SparkCassandraITWordSpecBase
import com.datastax.spark.connector.ccm.CcmConfig.V6_8_3
import com.datastax.spark.connector.cluster.DefaultCluster
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._


class IndexedPrimitivesSpec extends SparkCassandraITWordSpecBase with DefaultCluster with SaiBaseSpec {

  override def beforeClass {
    dseFrom(V6_8_3) {
      conn.withSessionDo { session =>
        createKeyspace(session)
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS $ks.primitives_test (
             |  pk int,
             |  text_col text,
             |  int_col int,
             |  bigint_col bigint,
             |
             |  PRIMARY KEY (pk));""".stripMargin)

        session.execute(
          s"CREATE CUSTOM INDEX int_sai_idx ON $ks.primitives_test (int_col) USING 'StorageAttachedIndex';")
        session.execute(
          s"CREATE CUSTOM INDEX bigint_sai_idx ON $ks.primitives_test (bigint_col) USING 'StorageAttachedIndex';")
        session.execute(
          s"""CREATE CUSTOM INDEX text_sai_idx ON $ks.primitives_test (text_col)
             |USING 'StorageAttachedIndex'
             |WITH OPTIONS = {'case_sensitive': false, 'normalize': true };""".stripMargin)

        for (i <- 0 to 9) {
          session.execute(s"insert into $ks.primitives_test (pk, text_col, int_col, bigint_col) values ($i, 'text$i', $i, $i)")
        }
      }
    }
  }

  "Index on a numeric column" should {

    "allow for equality predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") === 7)
      assertPushedPredicate(data, pushedPredicate = EqualTo("int_col", 7))
      data.count() shouldBe 1
    }

    "allow for gte predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") >= 7)
      assertPushedPredicate(data, pushedPredicate = GreaterThanOrEqual("int_col", 7))
      data.count() shouldBe 3
    }

    "allow for gt predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") > 7)
      assertPushedPredicate(data, pushedPredicate = GreaterThan("int_col", 7))
      data.count() shouldBe 2
    }

    "allow for lt predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") < 7)
      assertPushedPredicate(data, pushedPredicate = LessThan("int_col", 7))
      data.count() shouldBe 7
    }

    "allow for lte predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") <= 7)
      assertPushedPredicate(data, pushedPredicate = LessThanOrEqual("int_col", 7))
      data.count() shouldBe 8
    }

    "allow for more than one range predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") >= 7 and col("int_col") < 9)
      assertPushedPredicate(
        data,
        pushedPredicate = GreaterThanOrEqual("int_col", 7), LessThan("int_col", 9))
      data.count() shouldBe 2
    }

    "allow only for equality push down if range and equality predicates are defined" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") === 2 and col("int_col") < 4)

      assertPushedPredicate(data, pushedPredicate = EqualTo("int_col", 2))
      data.count() shouldBe 1
    }

    "allow only for a single equality push down if there are more than one" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("int_col") === 5 and col("int_col") === 8)
      assertPushedPredicate(data, pushedPredicate = EqualTo("int_col", 5))
      data.count() shouldBe 0
    }

    "not allow for a predicate push down if OR clause is used" in dseFrom(V6_8_3) {
      assertNoPushDown(df("primitives_test").filter(col("int_col") >= 7 or col("text_col") === 4))
    }

    "allow for gt predicate push down on bigints" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("bigint_col")  < 8L)
      assertPushedPredicate(data, pushedPredicate = LessThan("bigint_col", 8L))
      data.count() shouldBe 8
    }
  }

  "Index on a text column" should {

    "allow for equality predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("text_col") === "text3")
      assertPushedPredicate(data, pushedPredicate = EqualTo("text_col", "text3"))
      data.count() shouldBe 1
    }

    "not allow for contains predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("text_col") contains "text5")
      assertNoPushDown(data)
      data.count() shouldBe 1
    }

    "not allow for range predicate push down" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("text_col") < "text4")
      assertNoPushDown(data)
      data.count() shouldBe 4
    }

    "allow only for a single equality push down if there are more than one" in dseFrom(V6_8_3) {
      val data = df("primitives_test").filter(col("text_col") === "text1" and col("text_col") === "text2")
      assertPushedPredicate(data, pushedPredicate = EqualTo("text_col", "text1"))
      data.count() shouldBe 0
    }
  }
}