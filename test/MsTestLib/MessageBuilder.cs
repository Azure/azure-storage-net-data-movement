//------------------------------------------------------------------------------
// <copyright file="MessageBuilder.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MS.Test.Common.MsTestLib
{
    public class MessageBuilder
    {
        ///
        /// <summary>
        /// Uses String.Format method for formatting. Incase of any Exceptions due
        /// to null arguments or incorrect message format, it formats the message in
        /// an internal standard format. For example:
        /// MSG: my-message
        /// Obj-1: objToLog[1]
        /// Obj-2: objToLog[2]
        /// ...
        /// </summary>
        /// <param name="msgFormat"></param>
        /// <param name="objToLog">Objects that need to be serialized in the message</param>
        /// <returns></returns>
        ///
        public static string FormatString(string msgFormat, params object[] objToLog)
        {
            if ((string.IsNullOrEmpty(msgFormat) == false)
                && (msgFormat.IndexOf('{') != -1)
                && (msgFormat.IndexOf('}') != -1))
            {
                try
                {
                    return String.Format(msgFormat, objToLog);
                }
                catch
                {
                    //ignore exception
                }
            }

            string prefix = string.Empty;
            if (objToLog != null && objToLog.Length > 1)
            {
                prefix = " ";
            }

            StringBuilder sBuilder = new StringBuilder(prefix);
            sBuilder.Append(msgFormat);
            sBuilder.Append(SerializeObjects(objToLog));
            return sBuilder.ToString();
        }

        private static string SerializeObjects(object[] objToLog)
        {
            StringBuilder sBuilder = new StringBuilder();
            if (objToLog != null)
            {
                for (int i = 0; i < objToLog.Length; i++)
                {
                    if (objToLog != null)
                    {
                        try
                        {
                            sBuilder.Append("\n");
                            sBuilder.Append(" Obj-");
                            sBuilder.Append(i);
                            sBuilder.Append(" : ");
                            sBuilder.Append(objToLog[i]);
                        }
                        catch 
                        {
                            //Ignore any serialization exceptions
                            
                        }
                    }
                }
            }
            return sBuilder.ToString();
        }
    }
}
