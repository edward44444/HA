﻿using HA.Core;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HA.Model.Foundation
{
    [TableName("FD_BaseData")]
    [PrimaryKey("BDID")]
    public class BaseData
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
        [Column("CreateOn")]
        [DisplayName("创建时间")]
        public DateTime CreateOn { get; set; }

        /// <summary>
        /// 创建人
        /// </summary>
        [Column("CreateBy")]
        [DisplayName("创建人")]
        public string CreateBy { get; set; }
    }
}