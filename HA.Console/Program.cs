using HA.Core;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HA.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var db = new Database("HA");
            db.Execute(@"DECLARE @T NVARCHAR(50)='1234'

INSERT dbo.SL_OrderLog
        ( olOrderSerialId ,
          olOperation 
        )
VALUES  ( @T,@0 )", "测试");
        }
    }
}
