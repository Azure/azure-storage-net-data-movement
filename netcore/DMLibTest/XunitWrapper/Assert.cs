using System;
using System.Globalization;

namespace Microsoft.VisualStudio.TestTools.UnitTesting
{
    public static class Assert
    {
        public static void AreEqual(object expected, object actual)
        {
            Type type = expected.GetType();
            Type type2 = actual.GetType();
            bool flag = type != type2;
            if (flag)
            {
                bool flag2 = expected.ToString() == actual.ToString();
                if (flag2)
                {
                    return;
                }
                bool flag3 = type == typeof(Uri) && type2 == typeof(string);
                if (flag3)
                {
                    Xunit.Assert.Equal<Uri>((Uri)expected, new Uri((string)actual));
                    return;
                }
                bool flag4 = type == typeof(string) && type2 == typeof(Uri);
                if (flag4)
                {
                    Xunit.Assert.Equal<Uri>(new Uri((string)expected), (Uri)actual);
                    return;
                }
            }
            Xunit.Assert.Equal<object>(expected, actual);
        }

        public static void AreEqual(string expected, string actual, bool ignoreCase)
        {
            Xunit.Assert.Equal(expected, actual, ignoreCase, false, false);
        }

        public static void AreEqual(double expected, double actual, double delta)
        {
            Xunit.Assert.True(actual >= expected - delta && actual <= expected + delta);
        }

        public static void AreEqual(float expected, float actual, float delta)
        {
            Xunit.Assert.True(actual >= expected - delta && actual <= expected + delta);
        }

        public static void AreEqual(object expected, object actual, string message)
        {
            Xunit.Assert.Equal<object>(expected, actual);
        }

        public static void AreEqual(float expected, float actual, float delta, string message)
        {
            AreEqual(expected, actual, delta);
        }

        public static void AreEqual(double expected, double actual, double delta, string message)
        {
            AreEqual(expected, actual, delta);
        }

        public static void AreEqual(string expected, string actual, bool ignoreCase, CultureInfo culture)
        {
            try
            {
                AreEqual(expected, actual, ignoreCase);
            }
            finally
            {
            }
        }

        public static void AreEqual(string expected, string actual, bool ignoreCase, string message)
        {
            Xunit.Assert.Equal(expected, actual, ignoreCase, false, false);
        }

        public static void AreEqual(object expected, object actual, string message, params object[] parameters)
        {
            Xunit.Assert.Equal<object>(expected, actual);
        }

        public static void AreEqual(string expected, string actual, bool ignoreCase, CultureInfo culture, string message)
        {
            AreEqual(expected, actual, ignoreCase, culture);
        }

        public static void AreEqual(double expected, double actual, double delta, string message, params object[] parameters)
        {
            AreEqual(expected, actual, delta);
        }

        public static void AreEqual(string expected, string actual, bool ignoreCase, string message, params object[] parameters)
        {
            Xunit.Assert.Equal(expected, actual, ignoreCase, false, false);
        }

        public static void AreEqual(float expected, float actual, float delta, string message, params object[] parameters)
        {
            AreEqual(expected, actual, delta);
        }

        public static void AreEqual(string expected, string actual, bool ignoreCase, CultureInfo culture, string message, params object[] parameters)
        {
            AreEqual(expected, actual, ignoreCase, culture);
        }

        public static void AreEqual<T>(T expected, T actual)
        {
            Xunit.Assert.Equal<T>(expected, actual);
        }

        public static void AreEqual<T>(T expected, T actual, string message)
        {
            Xunit.Assert.Equal<T>(expected, actual);
        }

        public static void AreEqual<T>(T expected, T actual, string message, params object[] parameters)
        {
            Xunit.Assert.Equal<T>(expected, actual);
        }

        public static void AreNotEqual(object notExpected, object actual)
        {
            Xunit.Assert.NotEqual<object>(notExpected, actual);
        }

        public static void AreNotEqual(float notExpected, float actual, float delta)
        {
            Xunit.Assert.False(actual >= notExpected - delta && actual <= notExpected + delta);
        }

        public static void AreNotEqual(string notExpected, string actual, bool ignoreCase)
        {
            if (ignoreCase)
            {
                Xunit.Assert.NotEqual<string>(notExpected.ToLowerInvariant(), actual.ToLowerInvariant());
            }
            else
            {
                Xunit.Assert.NotEqual<string>(notExpected, actual);
            }
        }

        public static void AreNotEqual(double notExpected, double actual, double delta)
        {
            Xunit.Assert.False(actual >= notExpected - delta && actual <= notExpected + delta);
        }

        public static void AreNotEqual(object notExpected, object actual, string message)
        {
            Xunit.Assert.NotEqual<object>(notExpected, actual);
        }

        public static void AreNotEqual(double notExpected, double actual, double delta, string message)
        {
            Xunit.Assert.NotEqual<double>(notExpected, actual);
        }

        public static void AreNotEqual(string notExpected, string actual, bool ignoreCase, string message)
        {
            if (ignoreCase)
            {
                Xunit.Assert.NotEqual<string>(notExpected.ToLowerInvariant(), actual.ToLowerInvariant());
            }
            else
            {
                Xunit.Assert.NotEqual<string>(notExpected, actual);
            }
        }

        public static void AreNotEqual(string notExpected, string actual, bool ignoreCase, CultureInfo culture)
        {
            try
            {
                AreNotEqual(notExpected, actual, ignoreCase);
            }
            finally
            {
            }
        }

        public static void AreNotEqual(float notExpected, float actual, float delta, string message)
        {
            Xunit.Assert.False(actual >= notExpected - delta && actual <= notExpected + delta);
        }

        public static void AreNotEqual(object notExpected, object actual, string message, params object[] parameters)
        {
            Xunit.Assert.NotEqual<object>(notExpected, actual);
        }

        public static void AreNotEqual(string notExpected, string actual, bool ignoreCase, CultureInfo culture, string message)
        {
            AreNotEqual(notExpected, actual, ignoreCase, culture);
        }

        public static void AreNotEqual(string notExpected, string actual, bool ignoreCase, string message, params object[] parameters)
        {
            AreNotEqual(notExpected, actual, ignoreCase);
        }

        public static void AreNotEqual(double notExpected, double actual, double delta, string message, params object[] parameters)
        {
            AreNotEqual(notExpected, actual, delta);
        }

        public static void AreNotEqual(float notExpected, float actual, float delta, string message, params object[] parameters)
        {
            AreNotEqual(notExpected, actual, delta);
        }

        public static void AreNotEqual(string notExpected, string actual, bool ignoreCase, CultureInfo culture, string message, params object[] parameters)
        {
            AreNotEqual(notExpected, actual, ignoreCase, culture);
        }

        public static void AreNotEqual<T>(T notExpected, T actual)
        {
            Xunit.Assert.NotEqual<T>(notExpected, actual);
        }

        public static void AreNotEqual<T>(T notExpected, T actual, string message)
        {
            Xunit.Assert.NotEqual<T>(notExpected, actual);
        }

        public static void AreNotEqual<T>(T notExpected, T actual, string message, params object[] parameters)
        {
            Xunit.Assert.NotEqual<T>(notExpected, actual);
        }

        public static void AreNotSame(object notExpected, object actual)
        {
            Xunit.Assert.NotSame(notExpected, actual);
        }

        public static void AreNotSame(object notExpected, object actual, string message)
        {
            Xunit.Assert.NotSame(notExpected, actual);
        }

        public static void AreNotSame(object notExpected, object actual, string message, params object[] parameters)
        {
            Xunit.Assert.NotSame(notExpected, actual);
        }

        public static void AreSame(object expected, object actual)
        {
            Xunit.Assert.Same(expected, actual);
        }

        public static void AreSame(object expected, object actual, string message)
        {
            Xunit.Assert.Same(expected, actual);
        }

        public static void AreSame(object expected, object actual, string message, params object[] parameters)
        {
            Xunit.Assert.Same(expected, actual);
        }

        public static void Fail()
        {
            Xunit.Assert.True(false);
        }

        public static void Fail(string message)
        {
            Xunit.Assert.True(false, message);
        }

        public static void Fail(string message, params object[] parameters)
        {
            Xunit.Assert.True(false, string.Format(message, parameters));
        }

        public static void Inconclusive()
        {
            throw new NotSupportedException();
        }

        public static void Inconclusive(string message)
        {
            throw new NotSupportedException(message);
        }

        public static void Inconclusive(string message, params object[] parameters)
        {
            throw new NotSupportedException(string.Format(message, parameters));
        }

        public static void IsFalse(bool condition)
        {
            Xunit.Assert.False(condition);
        }

        public static void IsFalse(bool condition, string message)
        {
            Xunit.Assert.False(condition, message);
        }

        public static void IsFalse(bool condition, string message, params object[] parameters)
        {
            Xunit.Assert.False(condition, string.Format(message, parameters));
        }

        public static void IsInstanceOfType(object value, Type expectedType)
        {
            Xunit.Assert.IsAssignableFrom(expectedType, value);
        }

        public static void IsInstanceOfType(object value, Type expectedType, string message)
        {
            Xunit.Assert.IsAssignableFrom(expectedType, value);
        }

        public static void IsInstanceOfType(object value, Type expectedType, string message, params object[] parameters)
        {
            Xunit.Assert.IsAssignableFrom(expectedType, value);
        }

        public static void IsNotInstanceOfType(object value, Type wrongType)
        {
            try
            {
                Xunit.Assert.IsAssignableFrom(wrongType, value);
                Xunit.Assert.True(false);
            }
            catch
            {
            }
        }

        public static void IsNotInstanceOfType(object value, Type wrongType, string message)
        {
            IsNotInstanceOfType(value, wrongType);
        }

        public static void IsNotInstanceOfType(object value, Type wrongType, string message, params object[] parameters)
        {
            IsNotInstanceOfType(value, wrongType);
        }

        public static void IsNotNull(object value)
        {
            Xunit.Assert.NotNull(value);
        }

        public static void IsNotNull(object value, string message)
        {
            Xunit.Assert.NotNull(value);
        }

        public static void IsNotNull(object value, string message, params object[] parameters)
        {
            Xunit.Assert.NotNull(value);
        }

        public static void IsNull(object value)
        {
            Xunit.Assert.Null(value);
        }

        public static void IsNull(object value, string message)
        {
            Xunit.Assert.Null(value);
        }

        public static void IsNull(object value, string message, params object[] parameters)
        {
            Xunit.Assert.Null(value);
        }

        public static void IsTrue(bool condition)
        {
            Xunit.Assert.True(condition);
        }

        public static void IsTrue(bool condition, string message)
        {
            Xunit.Assert.True(condition, message);
        }

        public static void IsTrue(bool condition, string message, params object[] parameters)
        {
            Xunit.Assert.True(condition, string.Format(message, parameters));
        }

        public static void ThrowsException<TException>(Action action, string message, bool allowDerivedTypes = true)
        {
            try
            {
                action.Invoke();
                Fail(message);
            }
            catch (Exception ex)
            {
                bool flag = allowDerivedTypes && !(ex is TException);
                if (flag)
                {
                    Assert.Fail(string.Concat(new string[]
                    {
                        "Delegate throws exception of type ",
                        ex.GetType().Name,
                        ", but ",
                        typeof(TException).Name,
                        " or a derived type was expected."
                    }));
                }
                bool flag2 = !allowDerivedTypes && ex.GetType() != typeof(TException);
                if (flag2)
                {
                    Assert.Fail(string.Concat(new string[]
                    {
                        "Delegate throws exception of type ",
                        ex.GetType().Name,
                        ", but ",
                        typeof(TException).Name,
                        " was expected."
                    }));
                }
            }
        }
    }
}
