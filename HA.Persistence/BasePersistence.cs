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

        private IEnumerable<T> Query<T>(Expression<Func<T, bool>> predicate)
        {
            var sql = new Sql<T>();
            sql.Where(predicate);
            return Database.Query<T>(sql);
        }

        public List<T> Fetch<T>(Expression<Func<T, bool>> predicate)
        {
            return Query(predicate).ToList();
        }

        public T FirstOrDefault<T>(Expression<Func<T, bool>> predicate)
        {
            return Query(predicate).FirstOrDefault();
        }

        public T Insert<T>(T model)
        {
            return (T)Database.Insert(model);
        }

        public void BulkCopy<T>(List<T> collection)
        {
            Database.BulkCopy(collection);
        }

        public int Insert<T>(List<T> collection,Func<string, T, string> sqlRebuild=null)
        {
           return Database.Insert(collection,sqlRebuild);
        }

        public int Insert<T>(List<T> collection,string whereSql)
        {
            return Database.Insert(collection, whereSql);
        }
    }
}
