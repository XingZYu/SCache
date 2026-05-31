/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.scache.storage

import java.io.File

/**
 * A CXL memory domain groups hosts that share a common CXL-backed memory pool.
 * Each domain has its own pool file and allocator, managed by the SCache master.
 *
 * @param domainId      Unique domain identifier (e.g., "cxl-domain-0")
 * @param poolPath      Filesystem path to the shared CXL pool file (fsdax/CXL.mem)
 * @param poolSizeBytes Total size of the pool in bytes
 * @param poolAlignBytes Allocation alignment in bytes (typically 4096)
 * @param memberHosts   Set of hostnames that share this CXL domain
 */
case class CxlMemoryDomain(
    domainId: String,
    poolPath: String,
    poolSizeBytes: Long,
    poolAlignBytes: Int,
    memberHosts: Set[String])

/**
 * Runtime state for an active CXL domain, including its allocator and pool file handle.
 */
private[scache] case class CxlDomainState(
    domain: CxlMemoryDomain,
    allocator: CxlPoolAllocator,
    poolFile: File)

/**
 * Statistics for a CXL pool, reported via RPC for monitoring.
 */
case class CxlPoolStats(
    domainId: String,
    freeBytes: Long,
    totalBytes: Long,
    allocationCount: Long,
    freeSegmentCount: Int,
    fragmentationRatio: Double)
