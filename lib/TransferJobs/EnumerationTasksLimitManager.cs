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

		public EnumerationTasksLimitManager(int maxTransferConcurrency, WaitHandle enumerationResetEvent, TimeSpan waitTimeout, TimeSpan? transferStuckTimeout)
		{
			_maxTransferConcurrency = maxTransferConcurrency;
			_enumerationResetEvent = enumerationResetEvent;
			_waitTimeout = waitTimeout;
			_transferStuckTimeout = transferStuckTimeout;
		}

		public void CheckAndPauseEnumeration(long outstandingTasks, CancellationToken cancellationToken)
		{
			if (outstandingTasks > _maxTransferConcurrency)
			{
				DateTime start = DateTime.Now;
				while (!_enumerationResetEvent.WaitOne(_waitTimeout)
				       && !cancellationToken.IsCancellationRequested)
				{
					if (IsTransferStuck(start))
					{
						throw new TransferStuckException(Resources.TransferStuckException);
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