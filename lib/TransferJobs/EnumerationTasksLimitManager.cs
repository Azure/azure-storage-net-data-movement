namespace Microsoft.Azure.Storage.DataMovement
{
	using System;
	using System.Threading;

	internal class EnumerationTasksLimitManager
	{
		private readonly int _maxTransferConcurrency;
		private readonly WaitHandle _enumerationResetEvent;
		private readonly TimeSpan _waitTimeout;
		private readonly TimeSpan? _transferStuckTimeout;
		private DateTime _lastUpdate;

		public EnumerationTasksLimitManager(int maxTransferConcurrency, WaitHandle enumerationResetEvent, TimeSpan waitTimeout, TimeSpan? transferStuckTimeout)
		{
			_maxTransferConcurrency = maxTransferConcurrency;
			_enumerationResetEvent = enumerationResetEvent;
			_waitTimeout = waitTimeout;
			_transferStuckTimeout = transferStuckTimeout;
		}

		public void ProgressMade()
		{
			_lastUpdate = DateTime.Now;
		}

		public void CheckAndPauseEnumeration(long outstandingTasks, MemoryManager memoryManager, CancellationToken cancellationToken)
		{
			if (outstandingTasks > _maxTransferConcurrency)
			{
				_lastUpdate = DateTime.Now;
				while (!_enumerationResetEvent.WaitOne(_waitTimeout)
				       && !cancellationToken.IsCancellationRequested)
				{
					if (IsTransferStuck(_lastUpdate))
					{
						throw new TransferStuckException(string.Format(Resources.TransferStuckException, outstandingTasks, memoryManager.CellsStatistics));
					}
				}
			}
		}

		private bool IsTransferStuck(DateTime waitingLoopStartTime)
		{
			if (!_transferStuckTimeout.HasValue)
			{
				return false;
			}

			TimeSpan waitingTime = DateTime.Now - waitingLoopStartTime;

			return waitingTime > _transferStuckTimeout;
		}
	}
}