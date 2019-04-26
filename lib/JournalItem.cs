//------------------------------------------------------------------------------
// <copyright file="JournalItem.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{

#if !BINARY_SERIALIZATION
    using System.Runtime.Serialization;
    [DataContract]
#endif // BINARY_SERIALIZATION
    internal abstract class JournalItem
    {
        public StreamJournal Journal
        {
            get;
            set;
        }

        public long StreamJournalOffset { get; set; }
    }
}
