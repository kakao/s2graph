package subscriber

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by hsleep(honeysleep@gmail.com) on 2016. 3. 14..
  */
class ReplicateStreamingSpec extends FlatSpec with Matchers {
  "ReplicateStreaming" should "parse log" in {
    val log = """1457927050825	insert	e	kshp1wKyK25A_160209155438392	96	toros_1boon_article_personal	{"score":0.0,"meta_key":"56c2e02ca2b8815f0863903d"}"""
    val replLog = ReplicateStreaming.parseReplicationLog(log)
    replLog should not be empty

    replLog.get should contain ("_rep_")
  }
}
