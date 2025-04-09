import geopandas as gpd
import matplotlib.pyplot as plt

def render_geojson(geojson_path: str):
    """
    渲染 GeoJSON 文件
    """
    gdf = gpd.read_file(geojson_path)
    gdf.plot()
    plt.show()
