/*
 * Copyright 2012 Twitter, Inc.
 * Copyright 2012 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag.kestrel

import com.twitter.common.zookeeper.{Group, ServerSet, ZooKeeperClient}
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.conversions.time._
import com.twitter.logging.TestLogging
import com.twitter.thrift.{Status => TStatus}
import com.twitter.util.{MockTimer, TempFolder, Time, TimeControl}
import java.io._
import java.net.{InetAddress, InetSocketAddress, UnknownHostException}
import org.specs.SpecificationWithJUnit
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.JavaConversions
import scala.collection.mutable.Queue
import config._

class ZooKeeperServerStatusSpec extends SpecificationWithJUnit with JMocker with ClassMocker with TempFolder
with TestLogging {
  val mockZKClient = mock[ZooKeeperClient]
  val mockZKServerSets = Map(
    "/kestrel/read"  -> Queue[ServerSet](),
    "/kestrel/write" -> Queue[ServerSet]()
  )
  var mockTimer: MockTimer = null

  val memcacheAddr = new InetSocketAddress("10.1.2.3", 22133)
  val thriftAddr = new InetSocketAddress("10.1.2.3", 2229)

  def statusFile = canonicalFolderName + "/state"

  def nextMockZKServerSet(nodeType: String): ServerSet = {
    val mockServerSet = mock[ServerSet]
    mockServerSet
  }

  def makeZooKeeperServerStatus(): ZooKeeperServerStatus = {
    val zkConfig = new ZooKeeperBuilder {
      host = "localhost"
      pathPrefix = "/kestrel"
      useTwitterServerSet = false
    }

    mockTimer = new MockTimer
    val serverStatus = new ZooKeeperServerStatus(zkConfig(), statusFile, mockTimer) {
      override protected val zkClient = mockZKClient

      override protected def createServerSet(nodeType: String) = {
        val ss = nextMockZKServerSet(nodeType)
        ignoring(ss)
        ss
      }
    }
    serverStatus.start()
    serverStatus
  }

  def endpoints(memcache: Option[InetSocketAddress],
                thrift: Option[InetSocketAddress],
                text: Option[InetSocketAddress]) = {
    val sockets = memcache.map { a => "memcache" -> a } ++
                    thrift.map { a => "thrift" -> a } ++
                    text.map { a => "text" -> a }
    (sockets.head._2, sockets.toMap)
  }


  def withZooKeeperServerStatus(f: (ZooKeeperServerStatus, TimeControl) => Unit) {
    withTempFolder {
      Time.withCurrentTimeFrozen { mutator =>
        val serverStatus = makeZooKeeperServerStatus()
        f(serverStatus, mutator)
      }
    }
  }

  def storedStatus(): String = {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(statusFile), "UTF-8"))
    try {
      reader.readLine
    } finally {
      reader.close()
    }
  }

  def writeStatus(status: Status) {
    new File(statusFile).getParentFile.mkdirs()
    val writer = new OutputStreamWriter(new FileOutputStream(statusFile), "UTF-8")
    try {
      writer.write(status.toString)
    } finally {
      writer.close()
    }
  }

  "ZooKeeperServerStatus" should {
    "status" in {
      "close zookeeper client on shutdown" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          serverStatus.markUp()
          serverStatus.status mustEqual Up
          expect {
            one(mockZKClient).close()
          }
          serverStatus.shutdown()
          serverStatus.status mustEqual Down
        }
      }
    }
    "server set memberships" in {
      "Quiescent -> Up joins both serversets" in {
        withZooKeeperServerStatus {(serverStatus, _) =>
          serverStatus.markQuiescent()
          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustBe None
          oldWriteEndpoint mustBe None
          serverStatus.markUp()
          val (newReadEndpoint, newWriteEndpoint) = serverStatus.endpoints()
          newReadEndpoint mustNotBe None
          newWriteEndpoint mustNotBe None
        }
      }

      "Up -> Down should leave both serversets" in {
        withZooKeeperServerStatus {(serverStatus, _) =>
          serverStatus.markUp()
          // Adding endpoints will automatically also join the necessary serversets
          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustNotBe None
          oldWriteEndpoint mustNotBe None
          expect {
            one(mockZKClient).close()
          }
          serverStatus.shutdown()
          val (newReadEndpoint, newWriteEndpoint) = serverStatus.endpoints()
          newReadEndpoint mustBe None
          newWriteEndpoint mustBe None
        }
      }

      "Up -> Readonly should leave write serverset and remain in read" in {
        withZooKeeperServerStatus {(serverStatus, _) =>
          serverStatus.markUp()
          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustNotBe None
          oldWriteEndpoint mustNotBe None
          serverStatus.markReadOnly()
          val (newReadEndpoint, newWriteEndpoint) = serverStatus.endpoints()
          newReadEndpoint mustEqual oldReadEndpoint
          newWriteEndpoint mustBe None
        }
      }

      "Readonly -> Up should join both serversets" in {
        withZooKeeperServerStatus {(serverStatus, _) =>
          serverStatus.markReadOnly()
          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustNotBe None
          oldWriteEndpoint mustBe None
          serverStatus.markUp()
          val (newReadEndpoint, newWriteEndpoint) = serverStatus.endpoints()
          newReadEndpoint mustEqual oldReadEndpoint
          newWriteEndpoint mustNotBe None
        }
      }

      "Up -> Up should not update endpoints" in {
        withZooKeeperServerStatus {(serverStatus, _) =>
          serverStatus.markUp()
          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustNotBe None
          oldWriteEndpoint mustNotBe None
          serverStatus.markUp()
          val (newReadEndpoint, newWriteEndpoint) = serverStatus.endpoints()
          newReadEndpoint mustEqual oldReadEndpoint
          newWriteEndpoint mustEqual oldWriteEndpoint
        }
      }

      "Should not join serversets without endpoints" in {
        withZooKeeperServerStatus {(serverStatus, _) =>
          serverStatus.markUp()
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustBe None
          oldWriteEndpoint mustBe None
        }
      }
    }

    "adding endpoints" in {
      val hostAddr = ZooKeeperIP.toExternalAddress(InetAddress.getByAddress(Array[Byte](0, 0, 0, 0)))
      val expectedAddr = new InetSocketAddress(hostAddr, 22133)
      "should be able to register with a wildcard address" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          serverStatus.markUp()
          val wildcardAddr = new InetSocketAddress(22133)
          serverStatus.addEndpoints("memcache", Map("memcache" -> wildcardAddr))
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustNotBe None
          oldWriteEndpoint mustNotBe None
          serverStatus.mainAddress mustEqual Some(expectedAddr)
        }
      }

      "should explode if given the loopback address" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val localhostAddr = new InetSocketAddress("localhost", 22133)
          serverStatus.addEndpoints("memcache", Map("memcache" -> localhostAddr)) must throwA[UnknownHostException]
        }
      }

      "should leave non-wildcard, non-loopback addresses alone" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          serverStatus.markUp()
          val addr = new InetSocketAddress("1.2.3.4", 22133)
          serverStatus.addEndpoints("memcache", Map("memcache" -> addr))
          val (oldReadEndpoint, oldWriteEndpoint) = serverStatus.endpoints()
          oldReadEndpoint mustNotBe None
          oldWriteEndpoint mustNotBe None
          serverStatus.mainAddress mustEqual Some(addr)
        }
      }
    }
  }
}
