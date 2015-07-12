using System;
using System.ComponentModel;
using HA.Core;

namespace HA.Model.Foundation
{
    [TableName("FD_BaseData")]
    [PrimaryKey("BDID")]
    [ForeighKey("BDGroupCode")]
    public class BaseDataDataModel
    {
        /// <summary>
        /// 主键
        /// </summary>
        [Column("BDID")]
        [DisplayName("主键")]
        public int Id { get; set; }

        /// <summary>
        /// 分组编码
        /// </summary>
        [Column("BDGroupCode")]
        [DisplayName("分组编码")]
        public string GroupCode { get; set; }

        /// <summary>
        /// 编码
        /// </summary>
        [Column("BDCode")]
        [DisplayName("编码")]
        public string Code { get; set; }

        /// <summary>
        /// 名称
        /// </summary>
        [Column("BDName")]
        [DisplayName("名称")]
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

        /// <summary>
        /// 行状态
        /// </summary>
        [Column("RowStatus")]
        [DisplayName("行状态")]
        public int RowStatus { get; set; }
    }
}
