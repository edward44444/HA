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

        public void Insert<T>(List<T> collection)
        {
            Persistence.Insert(collection);
        }
    }
}
