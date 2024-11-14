import logging
import folium
from folium import plugins

logger = logging.getLogger(__name__)

class CongestionPolt:
    def __init__(self, zone_tbl, metrix_df):
        self.zone_tbl = zone_tbl
        self.metrix_df = metrix_df
        pass
    
    def seq_to_seq_poly_line(self, loc_name):
        # 지도 생성
        m = folium.Map(location=[37.772421, 128.947647], zoom_start=14)

        # 원 표시
        for _, row in self.zone_tbl.iterrows():
            logger.info(f"Location Coords : {row["coords"]}")
            folium.Circle(
                location = row["coords"],
                radius = 40,
                fill_color = "blue",
                fill_opacity = 0.2,
                stroke=False
            ).add_to(m)
        
            # 마커 표시
            folium.Marker(
                location = row["coords"],
                icon = plugins.BeautifyIcon(
                    icon="leaf",
                    icon_shape="marker", 
                    border_width=2, 
                    text_color="#ffffff",
                    border_color="transparent",
                    background_color="#1f77b4",
                    number=row["sequence"],
                    inner_icon_style="font-size:12px;"
                )
            ).add_to(m)
        
        # SRCMAC의 흐름을 PolyLine으로 표시
        for _, row in self.metrix_df.iterrows():
            path = []
            if row["seq_1"] == loc_name:
                path.append(row["coords"])
            
            if path:
                folium.PolyLine(
                    locations=path,
                    color="blue",
                    weight=10,
                    opacity=0.01
                ).add_to(m)

        m.save(f"{loc_name}.html")