using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using HA.Core;
using System.Linq.Expressions;

namespace HA.Persistence
{
   

    public class BasePersistence
    {
        public Database Database { get; private set; }

        public BasePersistence()
        {
            Database = new Database("HA");
        }

        public List<T> Fetch<T>(Expression<Func<T, bool>> predicate)
        {
            var sql = new Sql<T>();
            sql.Where(predicate);
            return Database.Fetch<T>(sql);
        }
    }
}
