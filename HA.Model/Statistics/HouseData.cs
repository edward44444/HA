using System.ComponentModel;
using HA.Core;

namespace HA.Model.Statistics
{
    [TableName("ST_HouseData")]
    [PrimaryKey("HDID")]
    public class HouseData
    {
        /// <summary>
        /// 主键
        /// </summary>
        [Column("HDID")]
        [DisplayName("主键")]
        public int Id { get; set; }

        /// <summary>
        /// 地区
        /// </summary>
        [Column("HDRegion")]
        [DisplayName("地区")]
        public string Region { get; set; }

        /// <summary>
        /// 地区名称
        /// </summary>
        [BaseDataConverter("BD_Region")]
        [DisplayName("地区名称")]
        [ResultColumn]
        public string RegionName { get; set; }
    }
}
