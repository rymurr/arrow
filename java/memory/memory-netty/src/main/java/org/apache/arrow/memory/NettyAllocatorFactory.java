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

package org.apache.arrow.memory;

import java.lang.reflect.Field;

import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.util.VisibleForTesting;

/**
 * A root allocator for using direct memory for Arrow Vectors/Arrays. Supports creating a
 * tree of descendant child allocators to facilitate better instrumentation of memory
 * allocations.
 */
public final class NettyAllocatorFactory implements BaseAllocator.Factory {

  public static final NettyAllocatorFactory FACTORY = new NettyAllocatorFactory();

  private NettyAllocatorFactory() {
  }

  @Override
  public BufferAllocator create(BaseAllocator.Config config) {
    return new RootAllocator(config);
  }

  /**
   * root allocator for Netty.
   */
  static final class RootAllocator extends BaseAllocator {

    private final ArrowBuf empty = new ArrowBuf(ReferenceManager.NO_OP,
        null,
        0,
        NettyAllocationManager.EMPTY.memoryAddress(), true);

    /**
     * Initialize an allocator.
     *
     * @see Config
     */
    private RootAllocator(Config config) throws OutOfMemoryException {
      super(null, "ROOT", config);
    }

    /**
     * Verify the accounting state of the allocation system.
     */
    @VisibleForTesting
    public void verify() {
      verifyAllocator();
    }

    @Override
    public ArrowBuf getEmpty() {
      return empty;
    }
  }

  public static BufferAllocator create() {
    return create(AllocationListener.NOOP, Long.MAX_VALUE);
  }

  public static BufferAllocator create(final long limit) {
    return create(AllocationListener.NOOP, limit);
  }

  public static BufferAllocator create(final AllocationListener listener, final long limit) {
    return create(listener, limit, getRoundingPolicy());
  }

  private static RoundingPolicy getRoundingPolicy() {
    RoundingPolicy roundingPolicy = DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY;
    try {
      Field field = NettyAllocationManager.class.getDeclaredField("CHUNK_SIZE");
      field.setAccessible(true);
      long chunkSize = (Long) field.get(null);
      roundingPolicy = new DefaultRoundingPolicy(chunkSize);
    } catch (Exception e) {
      //pass use default
    }
    return roundingPolicy;
  }

  /**
   * Constructor.
   *
   * @param listener       the allocation listener
   * @param limit          max allocation size in bytes
   * @param roundingPolicy the policy for rounding the buffer size
   */
  public static BufferAllocator create(final AllocationListener listener,
                                       final long limit,
                                       RoundingPolicy roundingPolicy) {
    return FACTORY.create(BaseAllocator.configBuilder()
        .allocationManagerFactory(NettyAllocationManager::new)
        .listener(listener)
        .maxAllocation(limit)
        .roundingPolicy(roundingPolicy)
        .build()
    );
  }

}
