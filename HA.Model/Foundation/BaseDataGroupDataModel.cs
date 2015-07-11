using HA.Core;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HA.Model.Foundation
{
    [TableName("FD_BaseDataGroup")]
    [PrimaryKey("BDGID")]
    public class BaseDataGroupDataModel
    {
        /// <summary>
        /// 主键
        /// </summary>
        [Column("BDGID")]
        [DisplayName("主键")]
        public int Id { get; set; }

        /// <summary>
        /// 分组编码
        /// </summary>
        [Column("BDGCode")]
        [DisplayName("分组编码")]
        public string Code { get; set; }

        /// <summary>
        /// 分组名称
        /// </summary>
        [Column("BDGName")]
        [DisplayName("分组名称")]
        public string Name { get; set; }

        /// <summary>
        /// 创建时间
        /// </summary>
        [Column("CreatedOn")]
        [DisplayName("创建时间")]
        public DateTime CreatedOn { get; set; }

        /// <summary>
        /// 创建人
        /// </summary>
        [Column("CreatedBy")]
        [DisplayName("创建人")]
        public string CreatedBy { get; set; }

        [ChildColumn]
        public List<BaseDataDataModel> BaseDataList { get; set; }
    }
}
