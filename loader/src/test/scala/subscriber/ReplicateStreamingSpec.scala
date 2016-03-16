package subscriber

import org.scalatest.{FlatSpec, Matchers}

class ReplicateStreamingSpec extends FlatSpec with Matchers {
  "ReplicateStreaming" should "parse log" in {
    val log = """1457927050825	insert	e	kshp1wKyK25A_160209155438392	96	toros_1boon_article_personal	{"score":0.0,"meta_key":"56c2e02ca2b8815f0863903d"}"""
    val replLog = ReplicateFunctions.parseReplicationLog(log)
    replLog should not be empty

    replLog.get.contains("_rep_") should be(true)
  }

  it should "parse log2" in {
    val log = "1458108396781\tinsertBulk\te\t0.d.1458054000000.age_band.sex.14.F\t20160316135309281\tmediadaum_v2_user_doc_read_topK\t{\"age_band\":\"14\",\"sex\":\"F\",\"time_unit\":\"d\",\"time_value\":1458054000000,\"date_time\":201603160000,\"dimension\":\"age_band.sex.14.F\",\"score\":38.0}"
    val replLog = ReplicateFunctions.parseReplicationLog(log)
    replLog should not be empty

    replLog.get.contains("_rep_") should be(true)
  }
}
