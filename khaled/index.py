import pandas as pd
import folium


# Load data from CSV
file_path = "sensor_data_all.csv"
df = pd.read_csv(file_path)


# Create a base map
madrid_map = folium.Map(location=[40.416775, -3.703790], zoom_start=4)  # Centered on Madrid

# Add markers for each location
for index, row in df.iterrows():
    popup_text = (f"<b>Location:</b> {row['location']}<br>"
                  f"<b>PM2.5:</b> {row['pm25']} µg/m³<br>"
                  f"<b>PM10:</b> {row['pm10']} µg/m³<br>"
                  f"<b>SO2:</b> {row['so2']} µg/m³<br>"
                  f"<b>NO:</b> {row['no']} µg/m³<br>"
                  f"<b>O3:</b> {row['o3']} µg/m³<br>")
    

    if pd.isna(row['pm25']):
         color = "red"

    elif row['pm25'] < 12:
         color = "green"
    elif 12 <= row['pm25'] <= 20:
        color = "lightgreen"
    else:
        color = "yellow"

    folium.CircleMarker(
        location=[row['lat'], row['lon']],
        radius=5,
        popup=folium.Popup(popup_text, max_width=250),
        tooltip=folium.Tooltip(f"{row['location']}"), 
        color=color,
        fill=True,
        fill_opacity=0.7
    ).add_to(madrid_map)

# Display the map
madrid_map.save('output.html')