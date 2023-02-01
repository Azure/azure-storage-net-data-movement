//------------------------------------------------------------------------------
// <copyright file="MemoryManager.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Class for maintaining a pool of memory buffer objects.
    /// </summary>
    internal class MemoryManager
    {
        private MemoryPool memoryPool;

        private long currentCapacity;

        private readonly int BufferSize;

        private readonly object memoryCapacityLockObject = new object();

        public MemoryManager(
            long capacity, int bufferSize)
        {
            BufferSize = bufferSize;
            this.currentCapacity = capacity;
            long availableCells = capacity / bufferSize;

            int cellNumber = (int)Math.Min((long)Constants.MemoryManagerCellsMaximum, availableCells);

            this.memoryPool = new MemoryPool(cellNumber, bufferSize);
        }

        public byte[] RequireBuffer(string clientRequestId)
        {
            return this.memoryPool.GetBuffer(clientRequestId ?? string.Empty);
        }

        public byte[][] RequireBuffers(string clientRequestId, int count)
        {
            return this.memoryPool.GetBuffers(clientRequestId ?? string.Empty, count);
        }

        public void ReleaseBuffer(byte[] buffer)
        {
            this.memoryPool.AddBuffer(buffer);
        }

        public void ReleaseBuffers(byte[][] buffer)
        {
            this.memoryPool.AddBuffers(buffer);
        }


        public void ReleaseBuffers(string clientRequestId)
        {
	        this.memoryPool.AddBuffers(clientRequestId ?? string.Empty);
        }

        internal void SetMemoryLimitation(long memoryLimitation)
        {
            lock (this.memoryCapacityLockObject)
            {
                if (memoryLimitation > this.currentCapacity)
                {
                    this.currentCapacity = memoryLimitation;

                    long availableCells = this.currentCapacity/BufferSize;
                    int cellNumber = (int) Math.Min((long) Constants.MemoryManagerCellsMaximum, availableCells);
                    this.memoryPool.SetCapacity(cellNumber);
                }
            }
        }

        private class MemoryPool
        {
            public readonly int BufferSize;

            private int maxCellCount;
            private int availableCells;
            private int allocatedCells;
            private object cellsListLock;
            private MemoryCell cellsListHeadCell;
            private ConcurrentDictionary<string, ConcurrentDictionary<byte[], MemoryCell>> transfers;

            public MemoryPool(int cellsCount, int bufferSize)
            {
                this.BufferSize = bufferSize;

                this.maxCellCount = cellsCount;
                this.availableCells = cellsCount;
                this.allocatedCells = 0;
                this.cellsListLock = new object();
                this.cellsListHeadCell = null;
                this.transfers = new ConcurrentDictionary<string, ConcurrentDictionary<byte[], MemoryCell>>();
            }

            public void SetCapacity(int cellsCount)
            {
                if (cellsCount > this.maxCellCount)
                {
                    lock (this.cellsListLock)
                    {
                        if (cellsCount > this.maxCellCount)
                        {
                            this.availableCells += (cellsCount - this.maxCellCount);
                            this.maxCellCount = cellsCount;
                        }
                    }
                }
            }

            public byte[] GetBuffer(string clientRequestId)
            {
                if (this.availableCells > 0)
                {
                    MemoryCell retCell = null;

                    lock (this.cellsListLock)
                    {
                        if (this.availableCells > 0)
                        {
                            if (null != this.cellsListHeadCell)
                            {
                                retCell = this.cellsListHeadCell;
                                this.cellsListHeadCell = retCell.NextCell;
                                retCell.NextCell = null;
                            }
                            else
                            {
                                retCell = new MemoryCell(this.BufferSize);
                                ++this.allocatedCells;
                            }

                            --this.availableCells;
                        }
                    }

                    if (null != retCell)
                    {
	                    TryAddCellsInUse(clientRequestId, retCell);
                        return retCell.Buffer;
                    }
                }

                return null;
            }

            public byte[][] GetBuffers(string clientRequestId, int count)
            {
                if (this.availableCells >= count)
                {
                    List<MemoryCell> retCells = null;

                    lock (this.cellsListLock)
                    {
                        if (this.availableCells >= count)
                        {
                            retCells = new List<MemoryCell>();

                            for (int i = 0; i < count; i++)
                            {
                                MemoryCell retCell;
                                if (null != this.cellsListHeadCell)
                                {
                                    retCell = this.cellsListHeadCell;
                                    this.cellsListHeadCell = retCell.NextCell;
                                    retCell.NextCell = null;
                                }
                                else
                                {
                                    retCell = new MemoryCell(this.BufferSize);
                                    ++this.allocatedCells;
                                }

                                --this.availableCells;

                                TryAddCellsInUse(clientRequestId, retCell);
                                retCells.Add(retCell);
                            }
                        }
                    }

                    if (null != retCells)
                    {
                        return retCells.Select(c => c.Buffer).ToArray();
                    }
                }

                return null;
            }

            public void AddBuffer(byte[] buffer)
            {
                if (null == buffer)
                {
                    throw new ArgumentNullException("buffer");
                }

                foreach (var transferCells in this.transfers.Values)
                {
	                RemoveCell(buffer, transferCells);
                }
                
            }

            public void AddBuffers(byte[][] buffers)
            {
                if (null == buffers)
                {
                    throw new ArgumentNullException("buffers");
                }

                if (buffers.Length == 1)
                {
                    this.AddBuffer(buffers[0]);
                    return;
                }

                foreach (var buffer in buffers)
                {
	                foreach (var transferCells in this.transfers.Values)
	                {
		                RemoveCell(buffer, transferCells);
                    }
                }
            }

            public void AddBuffers(string clientRequestId)
            {
	            this.transfers.TryGetValue(clientRequestId, out var transferCells);

	            if (transferCells == null)
	            {
		            return;
	            }

                foreach (var buffer in transferCells.Keys)
                {
	                RemoveCell(buffer, transferCells);
                }

                this.transfers.TryRemove(clientRequestId, out var _);
            }

            private void RemoveCell(byte[] buffer, ConcurrentDictionary<byte[], MemoryCell> cells)
            {
	            MemoryCell cell;
	            if (cells.TryRemove(buffer, out cell))
	            {
		            lock (this.cellsListLock)
		            {
			            cell.NextCell = this.cellsListHeadCell;
			            this.cellsListHeadCell = cell;
			            ++this.availableCells;
		            }
	            }
            }

            private void TryAddCellsInUse(string clientRequestId, MemoryCell retCell)
            {
	            this.transfers.AddOrUpdate(
		            clientRequestId,
		            ctx =>
		            {
			            var newDict = new ConcurrentDictionary<byte[], MemoryCell>();
			            newDict.TryAdd(retCell.Buffer, retCell);
			            return newDict;
		            },
		            (ctx, dict) =>
		            {
			            dict.TryAdd(retCell.Buffer, retCell);
			            return dict;
		            });
            }
        }

        private class MemoryCell
        {
            private byte[] buffer;

            public MemoryCell(int size)
            {
                this.buffer = new byte[size];
            }

            public MemoryCell NextCell
            {
                get;
                set;
            }

            public byte[] Buffer
            {
                get
                {
                    return this.buffer;
                }
            }
        }
    }
}
