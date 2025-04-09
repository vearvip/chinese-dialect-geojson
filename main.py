import geopandas as gpd  # type: ignore
import pandas as pd  # type: ignore
from typing import Iterable
import os
import utils.db as db
from shapely.geometry import Point  # type: ignore
from utils.constant import (
    JIAN_CHENG,
    JING_WEI_DU,
    YIN_DIAN_FEN_QV,
    YIN_DIAN_YAN_SE,
)
from utils.index import transform_dialect_infos_to_tree

## 获取当前脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def get_dialect_rows():
    rows = db.read(f"SELECT * FROM info")
    # 过滤掉没有音典颜色和经纬度坐标的点
    rows = [item for item in rows if (item[YIN_DIAN_YAN_SE] and item[JING_WEI_DU])]

    dialect_rows = []
    dialect_columns = [JIAN_CHENG, JING_WEI_DU, YIN_DIAN_FEN_QV]
    for d in rows:  # 遍历列表中的每个字典
        item = {}
        for key, value in d.items():  # 遍历单个字典的键值对
            if key in dialect_columns:
                item[key] = value
        dialect_rows.append(item)
    return dialect_rows


def judge_point_in_area(
    latitude: float,
    longitude: float,
    geometry: gpd.GeoDataFrame.geometry,
) -> bool:
    # 创建点对象（注意坐标顺序：经度在前，纬度在后）
    point = Point(longitude, latitude)

    # 空间关系判断（使用射线法原理）
    return geometry.contains(point)


def combin_regions(
    gdf: gpd.GeoDataFrame,
    regions_to_merge: Iterable[str],
    regions_to_remove: Iterable[str],
    new_region_name: str,
):
    # 检查必要的字段是否存在
    if "name" not in gdf.columns:
        raise KeyError(
            "输入文件缺少必需的字段 'name'。请确保数据包含此字段用于匹配区域名称。"
        )

    # 区域筛选与验证
    # 筛选出目标区域的数据
    merged_regions = gdf[gdf["name"].isin(regions_to_merge)]

    # 验证是否有有效的区域被筛选出来
    if merged_regions.empty:
        print(
            f"未找到指定的区域：{regions_to_merge}。请确认区域名称拼写无误且存在于输入数据中。"
        )
        return gdf

    # 区域几何合并
    # 使用unary_union将多个区域的几何形状合并为一个新的整体
    new_region_geom = merged_regions.union_all()

    # 创建新的区域记录（仅包含名称和几何信息）
    new_region = gpd.GeoDataFrame(
        {"name": [new_region_name], "geometry": [new_region_geom]}, crs=gdf.crs
    )

    # 保留其他未合并区域
    # 过滤掉已经被合并的区域
    other_regions = gdf[~gdf["name"].isin([*regions_to_merge, *regions_to_remove])]

    # 整合最终数据
    # 将未合并的区域与其他新创建的区域整合成一个完整的地理数据集
    final_gdf = gpd.GeoDataFrame(
        pd.concat([other_regions, new_region], ignore_index=True),  # 忽略索引重新编号
        crs=gdf.crs,
    )
    return final_gdf


def modify_geojson():
    # 输入和输出文件路径配置
    INPUT_FILE_PATH = os.path.join(
        SCRIPT_DIR, "input/shen_zhen.json"
    )  # 原始地理数据文件路径
    OUTPUT_FILE_PATH = os.path.join(
        SCRIPT_DIR, "output/china.json"
    )  # 合并后输出文件路径
    china_gdf = None
    # 数据加载
    try:
        # 尝试从GeoJSON文件中加载中国的地理数据
        china_gdf = gpd.read_file(INPUT_FILE_PATH)
    except Exception as e:
        raise FileNotFoundError(
            f"无法读取输入文件 '{INPUT_FILE_PATH}'。请检查文件路径是否正确。\n错误详情: {e}"
        )
    # final_gdf = combin_regions(china_gdf, ['湖北', '湖南'], ['南海诸岛', '十段线'], '湖广行省')

    ## 读取数据库
    # rows = db.read(f"SELECT * FROM info WHERE {JIAN_CHENG}=\"洛陽\" LIMIT 1")
    
    dialect_rows = get_dialect_rows()
    print("🍓", len(dialect_rows))
    # area = gdf[gdf['name'] == area_name]

    # for gdf_item in final_gdf:
    #     print(gdf_item['name'])

    # 遍历 GeoDataFrame 中的每个区块
    for index, gdf_item in china_gdf.iterrows():
        for dialect_item in dialect_rows:
            longitude = dialect_item[JING_WEI_DU].split(",")[0]
            latitude = dialect_item[JING_WEI_DU].split(",")[1]

            # 判断点是否在指定区块境内
            point_in_area = judge_point_in_area(
                latitude, longitude, geometry=gdf_item["geometry"]
            )
            if point_in_area:
                print(f"{dialect_item[JIAN_CHENG]} 🔹 {gdf_item['name']} ✅")

    # 输出结果
    try:
        # 导出处理后的地理数据到新的GeoJSON文件
        china_gdf.to_file(OUTPUT_FILE_PATH, driver="GeoJSON")
    except Exception as e:
        raise IOError(f"保存输出文件失败，请检查权限或磁盘空间。\n错误详情: {e}")

    # 打印操作完成提示
    print(f"\n输出文件已保存至：{OUTPUT_FILE_PATH}")


def main():
    # modify_geojson() 
    dialect_rows = get_dialect_rows()
    # dialect_info_tree = transform_dialect_infos_to_tree(dialect_rows)
    # print(dialect_info_tree)


# 确保 main 只在脚本直接运行时执行
if __name__ == "__main__":
    main()
