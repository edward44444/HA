using PetaPoco;
using System;
using System.Collections.Generic;
using System.Linq;
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
    }
}
