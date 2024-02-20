using System;

namespace Microsoft.Azure.Storage.DataMovement
{
    /// <summary>
    /// Enumeration event args.
    /// </summary>
    public sealed class EnumerationEventArgs : EventArgs
    {
        /// <summary>
        /// Amount if items enumerated from source enumerator
        /// </summary>
        public long ItemsEnumerated { get; set; }
        
        /// <summary>
        /// Amount if items enumerated from journal
        /// </summary>
        public long ItemsEnumeratedFromJournal { get; set; }
        
        /// <summary>
        /// Amount if items enumerated from sub transfers
        /// </summary>
        public long ItemsEnumeratedFromSubTransfers { get; set; }
    }
}