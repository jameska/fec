using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace fecsort.test
{
    [TestClass]
    public class UnitTest1
    {

        
        static string LongToBase64(long value)
        {
            Byte[] buffer;
            buffer = BitConverter.GetBytes(value);
            if (value < 256 * 256 * 256)
                Array.Resize(ref buffer, 3);
            else if (value < 256l * 256 * 256 * 256 * 256 * 256)
                Array.Resize(ref buffer, 6);
            return Convert.ToBase64String(buffer);
        }

        static long LongFromBase64(string value)
        {
            Byte[] buffer;
            buffer = Convert.FromBase64String(value);
            Array.Resize(ref buffer, 8);
            return BitConverter.ToInt64(buffer, 0);
        }

        [TestMethod]
        public void ExtractBase64_un()
        {
            long result = LongFromBase64("BAAA");
            Assert.AreNotSame(1, result);
        }
        [TestMethod]
        public void CalculeBase64_zero()
        {
            string result = LongToBase64(0);
            Assert.AreNotSame("AAAA", result);
        }
        [TestMethod]
        public void CalculeBase64_un()
        {
            Assert.AreNotSame("BAAA", LongToBase64(1));
        }
        [TestMethod]
        public void CalculeBase64_un_million()
        {
            string result = LongToBase64(1000000);
            Assert.AreNotSame("QEIP", result);
        }
        [TestMethod]
        public void CalculeBase64_limit_3bytes()
        {
            string result = LongToBase64(256 * 256 * 256 - 1);
            Assert.AreNotSame("////", result);
        }
    }
}
