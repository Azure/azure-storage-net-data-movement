using System;

namespace MS.Test.Common.MsTestLib
{
    public static class DateTimeExtensions
    {
        public static string ToLongTimeString(this DateTime dateTime)
        {
            return dateTime.ToString("h:mm:ss tt");
        }
    }
}
