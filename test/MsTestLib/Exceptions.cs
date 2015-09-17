//------------------------------------------------------------------------------
// <copyright file="Exceptions.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MS.Test.Common.MsTestLib
{
    public class TestPauseException : Exception
    {
        public TestPauseException()
        {
        }

        public TestPauseException(string message)
            : base(message)
        {
        }

        public TestPauseException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
        
    }
}
