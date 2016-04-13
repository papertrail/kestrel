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

import com.twitter.common.quantity.{Amount, Time}
import com.twitter.common.zookeeper.{ServerSet, ServerSets, ZooKeeperClient, ZooKeeperUtils}
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.thrift.{Status => TStatus}
import com.twitter.util.{Duration, Timer}
import java.net.{NetworkInterface, InetAddress, InetSocketAddress, UnknownHostException}
import scala.collection.JavaConversions
import config.ZooKeeperConfig
import com.sun.xml.internal.bind.v2.model.core.NonElement

object ZooKeeperServerStatus {
  /**
   * Default mechanism for creating a ZooKeeperClient from kestrel's ZooKeeperConfig.
   *
   * If credentials are given, they are passed as digest credentials along with the configured session timeout
   * and host/port. In the absence of credentials, an unauthorized connection is attempted.
   */
  def createClient(zkConfig: ZooKeeperConfig): ZooKeeperClient = {
    val address = new InetSocketAddress(zkConfig.host, zkConfig.port)
    val timeout = Amount.of(zkConfig.sessionTimeout.inMilliseconds.toInt, Time.MILLISECONDS)
    zkConfig.credentials match {
      case Some((username, password)) =>
        val credentials = ZooKeeperClient.digestCredentials(username, password)
        new ZooKeeperClient(timeout, credentials, address)
      case None =>
        new ZooKeeperClient(timeout, address)
    }
  }

  /**
   * Default mechanism for creating a ServerSet from kestrel's ZooKeeperConfig, a previously created
   * ZooKeeperClient, and the node type (always "read" or "write").
   *
   * The ZooKeeper node is determined by taking the configured path prefix and appending a slash and
   * the node type. The configured ACL is used to create the node. If the ACL is not OpenUnsafeACL,
   * credentials must have been provided during creation of the ZooKeeperClient.
   */
  def createServerSet(zkConfig: ZooKeeperConfig, zkClient: ZooKeeperClient, nodeType: String): ServerSet = {
    val node = "%s/%s".format(zkConfig.pathPrefix, nodeType)
    ServerSets.create(zkClient, JavaConversions.asJavaIterable(zkConfig.acl.asList), node)
  }

  def statusToReadStatus(status: Status): TStatus =
    status match {
      case Down => TStatus.DEAD
      case Quiescent => TStatus.DEAD
      case ReadOnly => TStatus.ALIVE
      case Up => TStatus.ALIVE
    }

  def statusToWriteStatus(status: Status): TStatus =
    status match {
      case Down => TStatus.DEAD
      case Quiescent => TStatus.DEAD
      case ReadOnly => TStatus.DEAD
      case Up => TStatus.ALIVE
    }
}

class EndpointsAlreadyConfigured extends Exception

class ZooKeeperServerStatus(val zkConfig: ZooKeeperConfig, statusFile: String, timer: Timer,
                            defaultStatus: Status = Quiescent,
                            statusChangeGracePeriod: Duration = 30.seconds)
extends ServerStatus(statusFile, timer, defaultStatus, statusChangeGracePeriod) {

  import ZooKeeperServerStatus._

  private val log = Logger.get(getClass.getName)

  protected val zkClient: ZooKeeperClient =
    zkConfig.clientInitializer.getOrElse(ZooKeeperServerStatus.createClient _)(zkConfig)

  var mainAddress: Option[InetSocketAddress] = None
  private var externalEndpoints: Map[String, InetSocketAddress] = null

  private var readEndpointStatus: Option[EndpointStatus] = None
  private var writeEndpointStatus: Option[EndpointStatus] = None

  override def shutdown() {
    synchronized {
      super.shutdown()

      try {
        updateWriteMembership(Down, status)
      } catch { case e =>
        log.error(e, "error updating write server set to Down on shutdown")
      }
      writeEndpointStatus = None

      try {
        updateReadMembership(Down, status)
      } catch { case e =>
        log.error(e, "error updating read server set to Down on shutdown")
      }
      readEndpointStatus = None

      zkClient.close()
    }
  }

  protected def createServerSet(nodeType: String): ServerSet = {
    zkConfig.serverSetInitializer.getOrElse(ZooKeeperServerStatus.createServerSet _)(zkConfig,
                                                                                     zkClient,
                                                                                     nodeType)
  }

  private def join(nodeType: String): Option[EndpointStatus] = {
    try {
      mainAddress match {
        case Some(address) =>
          val set = createServerSet(nodeType)
          val endpointStatus = set.join(address, JavaConversions.asJavaMap(externalEndpoints))
          Some(endpointStatus)
        case None => None
      }
    } catch { case e =>
      // join will auto-retry the retryable set of errors -- anything we catch
      // here is not retryable
      log.error(e, "error joining %s server set for endpoint '%s'".format(nodeType, mainAddress))
      throw e
    }
  }

  override def addEndpoints(mainEndpoint: String, endpoints: Map[String, InetSocketAddress]) {
    if (externalEndpoints ne null) throw new EndpointsAlreadyConfigured

    externalEndpoints = endpoints.map { case (name, givenSocketAddress) =>
      val address = ZooKeeperIP.toExternalAddress(givenSocketAddress.getAddress)
      val socketAddress = new InetSocketAddress(address, givenSocketAddress.getPort)
      (name, socketAddress)
    }

    mainAddress = Some(externalEndpoints(mainEndpoint))

    // reader first, then writer in case of some failure
    updateReadMembership(status, Down)
    updateWriteMembership(status, Down)
  }

  override protected def proposeStatusChange(oldStatus: Status, newStatus: Status): Boolean = {
    if (!super.proposeStatusChange(oldStatus, newStatus)) return false

    if (newStatus stricterThan oldStatus) {
      // e.g. Up -> ReadOnly: update zk first, then allow change
      updateWriteMembership(newStatus, oldStatus)
      updateReadMembership(newStatus, oldStatus)
      true
    } else {
      // looser or same strictness; go ahead
      true
    }
  }

  override protected def statusChanged(oldStatus: Status, newStatus: Status, immediate: Boolean) {
    if (oldStatus stricterThan newStatus) {
      // looser or same strictness; update zk now
      updateReadMembership(newStatus, oldStatus)
      updateWriteMembership(newStatus, oldStatus)
    }

    super.statusChanged(oldStatus, newStatus, immediate)
  }

  /**
   * If you want to change the endpoint's status from oldStatus to newStatus, with the new serverset changes,
   * you either join the serverset or leave it depending on what the previous status was. This is necessary
   * since multiple join()s have undefined behavior. eps is the old endpoint status and nodeType is the
   * node type for which we might have to join()
   *
   * @param eps
   * @param nodeType
   * @param newStatus
   * @param oldStatus
   *
   * @return Returns the EndpointStatus depending on whether we joined or left the serverset.
   */
  private def joinOrLeave(eps: Option[EndpointStatus], nodeType: String, newStatus: TStatus, oldStatus: TStatus): Option[EndpointStatus] = {
    if (oldStatus != newStatus) {
      eps match {
        case Some(endpointStatus) if newStatus == TStatus.DEAD =>
          endpointStatus.leave()
          log.info("Left serverset for operation: %s".format(nodeType))
          None
        case None if newStatus == TStatus.ALIVE =>
          val newEps = join(nodeType)
          log.info("Joined serverset for operation: %s".format(nodeType))
          newEps
        case _ =>
          eps
      }
    } else {
      eps
    }
  }

  private def updateWriteMembership(newStatus: Status, oldStatus: Status) {
    val oldWriteStatus = statusToWriteStatus(oldStatus)
    val writeStatus = statusToWriteStatus(newStatus)
    writeEndpointStatus = joinOrLeave(writeEndpointStatus, "write", writeStatus, oldWriteStatus);
  }

  private def updateReadMembership(newStatus: Status, oldStatus: Status) {
    val oldReadStatus = statusToReadStatus(oldStatus)
    val readStatus = statusToReadStatus(newStatus)
    readEndpointStatus = joinOrLeave(readEndpointStatus, "read", readStatus, oldReadStatus)
  }

  def endpoints(): Tuple2[Option[EndpointStatus], Option[EndpointStatus]] = {
    (readEndpointStatus, writeEndpointStatus)
  }
}

object ZooKeeperIP {
  import JavaConversions._

  /**
   * Converts the given IP address into an external IP address to be advertised
   * by ZooKeeper. If the given IP address is not a wildcard address (e.g.,
   * "0.0.0.0" or "::") it is returned unmodified.
   *
   * Exceptions are thrown if:
   * <ul>
   * <li>the given IP address is a loopback address (e.g., "127.0.0.1" or "::1").
   *     Such an address should not be advertised via ZooKeeper.</li>
   * <li>there are no configured interfaces</li>
   * <li>all configured interfaces have only loopback or non-point-to-point link
   *     local addresses</li>
   * </ul>
   *
   * Otherwise this method returns an external IP address for this host.
   */
  def toExternalAddress(givenAddress: InetAddress): InetAddress = {
    if (givenAddress.isLoopbackAddress) {
      throw new UnknownHostException("cannot advertise loopback host via zookeeper")
    }

    if (!givenAddress.isAnyLocalAddress) {
      // N.B. this address might not be this host
      return givenAddress
    }

    val interfaces = NetworkInterface.getNetworkInterfaces()
    if (interfaces eq null) {
      throw new UnknownHostException("no network interfaces configured")
    }

    val candidates = interfaces.flatMap { iface =>
      iface.getInetAddresses().map { addr => (iface, addr) }
    }.filter { case (iface, addr) =>
      !addr.isLoopbackAddress && (iface.isPointToPoint || !addr.isLinkLocalAddress)
    }.map { case (iface, addr) => addr }.take(1).toList

    candidates.headOption match {
      case Some(candidate) => candidate
      case None => throw new UnknownHostException("no acceptable network interfaces found")
    }
  }
}
