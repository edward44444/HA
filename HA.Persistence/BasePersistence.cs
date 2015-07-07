using PetaPoco;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

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
            return null;
        }
    }
}
