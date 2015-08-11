using System.Security.Cryptography;
using System.Text;

namespace HA.Core.Toolkit
{
    public class EncryptHelper
    {
        public static string Md5Encrypt(string content)
        {
            var md5 = new MD5CryptoServiceProvider();
            var sb = new StringBuilder(32);
            var targetData = md5.ComputeHash(Encoding.Default.GetBytes(content));
            foreach (var item in targetData)
            {
                sb.Append(item.ToString("x").PadLeft(2, '0'));
            }
            return sb.ToString();
        }
    }
}
