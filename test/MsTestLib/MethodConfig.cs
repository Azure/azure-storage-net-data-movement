//------------------------------------------------------------------------------
// <copyright file="MethodConfig.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MS.Test.Common.MsTestLib
{

    public class MethodConfig
    {
        public MethodConfig()
        {
            methodParams = new Dictionary<string, string>();
        }

        private Dictionary<string, string> methodParams;

        public Dictionary<string, string> MethodParams
        {
            get { return methodParams; }
            set { methodParams = value; }
        }

        public string this[string key]
        {
            get
            {
                return methodParams[key];
            }

            set
            {
                methodParams[key] = value;
            }
        }
    }


}
