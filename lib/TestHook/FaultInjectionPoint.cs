
//------------------------------------------------------------------------------
// <copyright file="FaultInjectionPoint.cs" company="Microsoft">
//      Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>
// <summary>
//      Fault injection point definition
// </summary>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Storage.DataMovement
{
    using System;

#if DEBUG
    /// <summary>
    /// Static fault injection point for testing. Each fault injection point
    /// consists of a name and optionally a value.
    /// To enable a fault injection point, add a process-wide environment
    /// variable, with variable value = #faultValue.
    /// </summary>
    internal class FaultInjectionPoint
    {
        public static string FIP_ThrowExceptionAfterEnumerated = "ThrowExceptionAfterEnumerated";
        public static string FIP_ThrowExceptionOnDirectory = "ThrowExceptionOnDirectory";

        /// <summary>
        /// Initializes a new instance of the <see cref="FaultInjectionPoint" /> class
        /// </summary>
        /// <param name="name">fault injection point name</param>
        public FaultInjectionPoint(string name)
        {
            this.Name = name;
        }

        /// <summary>
        /// Gets name of the fault injection point
        /// </summary>
        public string Name
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets value of the fault injection point
        /// </summary>
        /// <param name="value">value of the fault injection point</param>
        /// <returns>true the fault injection point is enable, false otherwise</returns>
        public bool TryGetValue(out string value)
        {
            value = Environment.GetEnvironmentVariable(this.Name);
            return (null != value);
        }
    }
#endif
}
