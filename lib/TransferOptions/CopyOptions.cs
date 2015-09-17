//------------------------------------------------------------------------------
// <copyright file="CopyOptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    /// <summary>
    /// Represents a set of options that may be specified for copy operation
    /// </summary>
    public sealed class CopyOptions
    {
        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the source object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition SourceAccessCondition { get; set; }

        /// <summary>
        /// Gets or sets an <see cref="AccessCondition"/> object that represents the access conditions for the destination object. If <c>null</c>, no condition is used.
        /// </summary>
        public AccessCondition DestinationAccessCondition { get; set; }
    }
}
