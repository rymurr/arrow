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

package org.apache.arrow.memory.rounding;

import org.apache.arrow.memory.util.CommonUtil;

/**
 * The default rounding policy. That is, if the requested size is within the chunk size,
 * the rounded size will be the next power of two. Otherwise, the rounded size
 * will be identical to the requested size.
 */
public class DefaultRoundingPolicy implements RoundingPolicy {

  public final long chunkSize;

  /**
   * default chunk size from Netty.
   */
  private static final long DEFAULT_CHUNK_SIZE = 16777216L;

  /**
   * The singleton instance.
   */
  public static final DefaultRoundingPolicy DEFAULT_ROUNDING_POLICY = new DefaultRoundingPolicy(DEFAULT_CHUNK_SIZE);

  public DefaultRoundingPolicy(long chunkSize) {
    this.chunkSize = chunkSize;
  }

  @Override
  public long getRoundedSize(long requestSize) {
    return requestSize < chunkSize ?
            CommonUtil.nextPowerOfTwo(requestSize) : requestSize;
  }
}
