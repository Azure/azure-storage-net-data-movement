//------------------------------------------------------------------------------
// <copyright file="TransferLocation.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System.Runtime.Serialization;

#if !BINARY_SERIALIZATION
    [DataContract]
#endif
    internal abstract class TransferLocation
    {
        /// <summary>
        /// Gets transfer location type.
        /// </summary>
        public abstract TransferLocationType Type
        {
            get;
        }

        /// <summary>
        /// Gets source/destination instance in transfer.
        /// </summary>
        public abstract object Instance
        {
            get;
        }

        /// <summary>
        /// Gets or sets a value indicating whether detailed information(ETag, LMT, Length, Properties and metadata) 
        /// of the instance contained in the current transfer location has been fetched.
        /// </summary>
        public bool? IsInstanceInfoFetched
        {
            get;
            set;
        }

        /// <summary>
        /// Validates the transfer location.
        /// </summary>
        public abstract void Validate();

        // Summary:
        //     Determines whether the specified transfer location is equal to the current transfer location.
        //
        // Parameters:
        //   obj:
        //     The transfer location to compare with the current transfer location.
        //
        // Returns:
        //     true if the specified transfer location is equal to the current transfer location; otherwise, false.
        public override bool Equals(object obj)
        {
            TransferLocation location = obj as TransferLocation;
            if (location == null || this.Type != location.Type)
                return false;

            return this.ToString() == location.ToString();
        }

        //
        // Summary:
        //     Returns the hash code for the transfer location.
        //
        // Returns:
        //     A 32-bit signed integer hash code.
        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }
    }
}
