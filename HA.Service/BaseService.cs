using HA.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace HA.Service
{
    public class BaseService<T> where T : BasePersistence
    {
        public T Persistence { get; private set; }

        public BaseService()
        {
            Persistence = Activator.CreateInstance<T>();
        }

        public List<T> Fetch<T>(Expression<Func<T, bool>> predicate)
        {
            return Persistence.Fetch<T>(predicate);
        }

        public T FirstOrDefault<T>(Expression<Func<T, bool>> predicate)
        {
            return Persistence.FirstOrDefault(predicate);
        }

        public T Insert<T>(T model)
        {
            return Persistence.Insert(model);
        }

        public void BulkCopy<T>(List<T> collection)
        {
            Persistence.BulkCopy(collection);
        }

        public int Insert<T>(List<T> collection)
        {
            return Persistence.Insert(collection);
        }

        public int Insert<T>(List<T> collection, Func<string, T, string> sqlRebuild)
        {
           return Persistence.Insert(collection, sqlRebuild);
        }

        public int Insert<T>(List<T> collection, string whereSql)
        {
            return Persistence.Insert(collection, whereSql);
        }

        public int Update<T>(T poco, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(poco, expressions);
        }

        public int Update<T>(IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(collection, expressions);
        }

        public int Update<T>(Expression<Func<T, object>>[] primaryKeyExpressions, IList<T> collection, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(primaryKeyExpressions, collection, expressions);
        }

        public int Update<T>(Expression<Func<T, object>>[] primaryKeyExpressions, IList<T> collection, Func<string, T, string> sqlRebuild, params Expression<Func<T, object>>[] expressions)
        {
            return Persistence.Update(primaryKeyExpressions, collection, sqlRebuild, expressions);
        }
    }
}
