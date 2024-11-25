import os
import json
import logging
import folium
import numpy as np
import pandas as pd
from PIL import Image
from folium import plugins
from branca.colormap import LinearColormap
from attrdictionary  import AttrDict
from ast import literal_eval

logger = logging.getLogger(__name__)

class CongestionPolt:
    def __init__(self, zone_tbl, args):
        self.zone_tbl = zone_tbl
        self.args = args

        try:
            self.zone_tbl["coords"] = self.zone_tbl["coords"].map(literal_eval)
        
        except:
            pass
    
    def seq_to_seq_flow_polyline(self, df, weight=10, n_priority=50):
        """
        Sequence 순으로 Map에 Flow를 Plot하는 함수
        """
        # Folium 맵 객체 생성
        m = folium.Map(location=[37.772421, 128.947647], zoom_start=14)

        # 원 표시
        for _, row in self.zone_tbl.iterrows():
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
        for idx, row in df[df.columns[:-3]][:n_priority].iterrows():
            path = []
            
            for loc, val in row.items():
                if val > 0:
                    coords = self.zone_tbl[self.zone_tbl["location"] == loc]["coords"].values[0]
                    path.append(coords)

                if path:
                    folium.PolyLine(
                        locations=path,
                        color="blue",
                        weight=weight,
                        opacity=0.03
                    ).add_to(m)
        
        m.save(f"{self.args.save_data[:5]}_sequence_flow_polyline.html")
    
    def src_to_dst_poly_line(self, df, loc_name):
        """
        seq1에서 seq2 Plot 함수
        """
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
        for _, row in df.iterrows():
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

        m.save(f"{self.args.save_data[:5]}_src_to_dst_polyline.html")

    def generate_heatmap_points(self, start_poi, end_poi, num_poi=3):
        """
        경로에 따라 임의의 heatmap point 생성 함수
        두 지점 사이의 경로를 따라 점들과 가중치를 생성
        start_poi: 시작 지점 (위도, 경도)
        end_poi: 끝 지점 (위도, 경도)
        num_poi: 생성할 poi의 개수
        """
        latitudes = np.linspace(start_poi[0], end_poi[0], num_poi)
        longitudes = np.linspace(start_poi[1], end_poi[1], num_poi)
        counts = np.linspace(start_poi[2], end_poi[2], num_poi)
        
        return [[lat, lon, cnt] for lat, lon, cnt in zip(latitudes, longitudes, counts)]
    
    def heat_map(self, df, on_time, radius=35, is_interpolate=False, min_value=11, max_value=575):
        # on_time 데이터 생성
        loc_h_people_cnt = pd.merge(df[df["on_time"] == on_time], self.zone_tbl, left_on="location", right_on="location", how="right")
        
        # 데이터 유실로 인한 Null 처리(첫째날 단말기 off)
        loc_h_people_cnt["on_time"] = loc_h_people_cnt["on_time"].ffill()
        loc_h_people_cnt.fillna({"SRCMAC": 0}, inplace=True)

        # sequence 순으로 정렬
        loc_h_people_cnt.sort_values(by="sequence", inplace=True)

        # Folium 지도 생성
        m = folium.Map(location=[37.772421, 128.947647], zoom_start=14)
        
        # 마커 표시
        for _, row in self.zone_tbl.iterrows():
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

        heat_data = []
        for lat, long, cnt in zip(loc_h_people_cnt["latitude"], loc_h_people_cnt["longitude"], loc_h_people_cnt["SRCMAC"]):
            heat_data.append([lat, long, cnt])

        # HeatMap에 데이터를 추가하면서 정규화
        heat_data = [
            [point[0], point[1], (point[2] - min_value) / (max_value - min_value)]  # 정규화된 값
            for point in heat_data
        ]

        if is_interpolate == True:
            gen_heat_data = []
            gen_heat_data.append(self.generate_heatmap_points(heat_data[0], heat_data[1], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[1], heat_data[2], num_poi=4))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[1], heat_data[3], num_poi=4))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[2], heat_data[4], num_poi=5))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[4], heat_data[5], num_poi=4))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[5], heat_data[6], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[6], heat_data[7], num_poi=4))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[7], heat_data[8], num_poi=5))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[8], heat_data[9], num_poi=2))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[8], heat_data[10], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[8], heat_data[11], num_poi=6))

            # 3차원 리스트를 2차원 리스트로 변환
            gen_heat_data = [item for sublist in gen_heat_data for item in sublist]

            # 중복 제거 (리스트를 튜플로 변환하여 set으로 처리 후 다시 리스트로 변환)
            gen_heat_data = list(map(list, set(map(tuple, gen_heat_data))))
            heat_data = gen_heat_data

        # HeatMap 추가
        plugins.HeatMap(
            heat_data,
            min_opacity = 0.1,
            radius = radius,
            blur = 30,
            max_zoom = 10
        ).add_to(m)

        # Gradient Bar 생성
        colormap = LinearColormap(
            colors=["blue", "green", "yellow", "red"],
            vmin = min_value,
            vmax = max_value
        )
        colormap.caption = "강릉커피축제 Heatmap Intensity"  # 설명 추가
        colormap.add_to(m)

        m.save(f"{self.args.save_data[:5]}_heatmap_{on_time}.html")

class CongestionImagePolt(CongestionPolt):
    def __init__(self, zone_tbl, args):
        super().__init__(zone_tbl, args)

        try:
            self.zone_tbl["coords"] = self.zone_tbl["coords"].map(literal_eval)

        except:
            pass
    
    def seq_to_seq_flow_polyline(self, df, n_priority=50):
        # 배경 이미지 로드
        image_path = os.path.join(self.args.path, self.args.img_file)
        image = Image.open(image_path)
        image_width, image_height = image.size
        image_width = image_width
        image_height = image_height

        # 이미지 크기에 따라 지도의 경계 설정 (좌표계 변환 없이 단순 픽셀 매핑)
        bounds = [[0, 0], [image_height, image_width]] # [y_min, x_min], [y_max, x_max]

        # Folium 맵 객체 생성
        m = folium.Map(
            location=[image_height / 2, image_width / 2],  # 이미지 중심에 맵을 위치
            zoom_start=1,  # 초기 줌 설정
            crs="Simple",  # CRS를 Simple로 설정하여 픽셀 좌표계 유지
            tiles=None
        )

        # 이미지를 타일로 추가
        image_overlay = folium.raster_layers.ImageOverlay(
            image_path,
            bounds=bounds,
            opacity=1.0
        )
        image_overlay.add_to(m)

        # 마커 표시
        for _, row in self.zone_tbl.iterrows():
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
        for idx, row in df[df.columns[:-3]][:n_priority].iterrows():
            path = []
            
            for loc, val in row.items():
                if val > 0:
                    coords = self.zone_tbl[self.zone_tbl["location"] == loc]["coords"].values[0]
                    path.append(coords)

                if path:
                    # for cnt in count:
                    folium.PolyLine(
                        locations=path,
                        color="blue",
                        weight=15,
                        opacity=0.03
                    ).add_to(m)
        
        m.save(f"{self.args.save_data[:5]}_sequence_flow_polyline.html")
    
    def heat_map(self, df, on_time, radius=35, is_interpolate=False, min_value=26, max_value=794):
        # on_time 데이터 생성
        loc_h_people_cnt = pd.merge(df[df["on_time"] == on_time], self.zone_tbl, left_on="location", right_on="location", how="right")
        
        # 데이터 유실로 인한 Null 처리(첫째날 단말기 off)
        loc_h_people_cnt["on_time"] = loc_h_people_cnt["on_time"].ffill()
        loc_h_people_cnt.fillna({"SRCMAC": 0}, inplace=True)
        
        # sequence 순으로 정렬
        loc_h_people_cnt.sort_values(by="sequence", inplace=True)

        # 배경 이미지 로드
        image_path = os.path.join(self.args.path, self.args.img_file)
        image = Image.open(image_path)
        image_width, image_height = image.size
        image_width = image_width # /4 : 4배로 축소
        image_height = image_height

        # 이미지 크기에 따라 지도의 경계 설정 (좌표계 변환 없이 단순 픽셀 매핑)
        bounds = [[0, 0], [image_height, image_width]] # [y_min, x_min], [y_max, x_max]

        # Folium 맵 객체 생성
        m = folium.Map(
            location=[image_height / 2, image_width / 2],  # 이미지 중심에 맵을 위치
            zoom_start=1,  # 초기 줌 설정
            crs="Simple",  # CRS를 Simple로 설정하여 픽셀 좌표계 유지
            tiles=None
        )

        # 이미지를 타일로 추가
        image_overlay = folium.raster_layers.ImageOverlay(
            image_path,
            bounds=bounds,
            opacity=1.0
        )
        image_overlay.add_to(m)

        # 마커 표시
        for _, row in self.zone_tbl.iterrows():
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

        heat_data = []
        for lat, long, cnt in zip(loc_h_people_cnt["latitude"], loc_h_people_cnt["longitude"], loc_h_people_cnt["SRCMAC"]):
            heat_data.append([lat, long, cnt])
        
        # HeatMap에 데이터를 추가하면서 정규화
        heat_data = [
            [point[0], point[1], (point[2] - min_value) / (max_value - min_value)]  # 정규화된 값
            for point in heat_data
        ]

        # Heatmap Interpolation
        if is_interpolate == True:
            # Horizontal
            gen_heat_data = []
            gen_heat_data.append(self.generate_heatmap_points(heat_data[0], heat_data[1], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[2], heat_data[3], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[4], heat_data[5], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[5], heat_data[6], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[6], heat_data[7], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[8], heat_data[9], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[9], heat_data[10], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[10], heat_data[11], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[12], heat_data[13], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[13], heat_data[14], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[14], heat_data[15], num_poi=3))

            # Vertical
            gen_heat_data.append(self.generate_heatmap_points(heat_data[4], heat_data[8], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[8], heat_data[12], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[5], heat_data[9], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[9], heat_data[13], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[1], heat_data[6], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[6], heat_data[10], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[10], heat_data[14], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[3], heat_data[7], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[7], heat_data[11], num_poi=3))
            gen_heat_data.append(self.generate_heatmap_points(heat_data[11], heat_data[15], num_poi=3))

            # 3차원 리스트를 2차원 리스트로 변환
            gen_heat_data = [item for sublist in gen_heat_data for item in sublist]

            # 중복 제거 (리스트를 튜플로 변환하여 set으로 처리 후 다시 리스트로 변환)
            gen_heat_data = list(map(list, set(map(tuple, gen_heat_data))))
            heat_data = gen_heat_data

        # HeatMap 추가
        plugins.HeatMap(
            heat_data,
            min_opacity = 0.1,
            radius = radius,
            blur = 70,
            max_zoom = 1,
            origin="lower",
            max_val = 1
        ).add_to(m)

        # Gradient Bar 생성
        colormap = LinearColormap(
            colors=["blue", "green", "yellow", "red"],
            vmin = min_value,
            vmax = max_value
        )
        colormap.caption = "KME 2024 Heatmap Intensity"  # 설명 추가
        colormap.add_to(m)

        m.save(f"{self.args.save_data[:5]}_heatmap_{on_time}.html")

if __name__ == "__main__":
    args = AttrDict(json.load(open("./config/config.json", encoding="utf-8")))
    loc_h_people_cnt = pd.read_csv("./data/gangneung/save/v1026_일자별 공간별 시간별 체류인원수.csv", encoding="utf-8")
    zone_tbl = pd.read_csv("./data/gangneung/gangneung_zoneTable.csv", encoding="cp949")
    plot = CongestionPolt(zone_tbl, args)


    # Heat Map
    for hour in range(9, 21):
        plot.heat_map(loc_h_people_cnt, hour, is_interpolate=True)