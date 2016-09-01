//------------------------------------------------------------------------------
// <copyright file="TestResult.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace DMLibTest
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestResult<TDataInfo> where TDataInfo : IDataInfo
    {
        private List<Exception> exceptions;
        
        public TestResult()
        {
            this.exceptions = new List<Exception>();
            this.DataInfo = default(TDataInfo);
        }

        public TDataInfo DataInfo
        {
            set;
            get;
        }

        public List<TransferItem> TransferItems
        {
            set;
            get;
        }

        public List<Exception> Exceptions
        {
            get
            {
                return this.exceptions;
            }
        }

        public void AddException(Exception e)
        {
            this.exceptions.Add(e);
        }
    }
}
