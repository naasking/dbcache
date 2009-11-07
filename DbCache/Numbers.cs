using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DbCache
{
    public static class Number
    {
        static string[] first =
        {
            "Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine",
            "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen",
            "Seventeen", "Eighteen", "Nineteen"
        };
        static string[] tens =
        {
            "Twenty", "Thirty", "Fourty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety",
        };

        /// <summary>
        /// Converts the given number to an english sentence.
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        public static string ToSentence(int n)
        {
            return n < 0            ? "Minus " + ToSentence(-n):
                   n <= 19          ? first[n]:
                   n <= 99          ? tens[n / 10 - 2] + " " + ToSentence(n % 10):
                   n <= 199         ? "One Hundred " + ToSentence(n % 100):
                   n <= 999         ? ToSentence(n / 100) + "Hundred " + ToSentence(n % 100):
                   n <= 1999        ? "One Thousand " + ToSentence(n % 1000):
                   n <= 999999      ? ToSentence(n / 1000) + "Thousand " + ToSentence(n % 1000):
                   n <= 1999999     ? "One Million " + ToSentence(n % 1000000):
                   n <= 999999999   ? ToSentence(n / 1000000) + "Million " + ToSentence(n % 1000000):
                   n <= 1999999999  ? "One Billion " + ToSentence(n % 1000000000):
                                      ToSentence(n / 1000000000) + "Billion " + ToSentence(n % 1000000000);
        }
    }
}
