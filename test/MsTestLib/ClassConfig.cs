//------------------------------------------------------------------------------
// <copyright file="ClassConfig.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MS.Test.Common.MsTestLib
{

    public class ClassConfig
    {
        public ClassConfig()
        {
            classParams = new Dictionary<string, string>();
            classMethods = new Dictionary<string, MethodConfig>();
        }

        private Dictionary<string, string> classParams;

        public Dictionary<string, string> ClassParams
        {
            get { return classParams; }
            set { classParams = value; }
        }

        private Dictionary<string, MethodConfig> classMethods;

        public MethodConfig this[string methodName]
        {
            get
            {
                if (classMethods.ContainsKey(methodName))
                {
                    return classMethods[methodName];
                }
                else
                {
                    return null;
                }
            }

            set
            {
                classMethods[methodName] = value;
            }

        }

    }

}
