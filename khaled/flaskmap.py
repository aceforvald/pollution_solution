from flask import Flask, render_template_string
import folium
import pandas as pd


app = Flask(__name__)

#  # Replace with the path to your file
df = pd.read_csv('sensor_data_all.csv')

@app.route("/")
def home():

    madrid_map = folium.Map(location=[40.416775, -3.703790], zoom_start=11)  # Centered on Madrid

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

    madrid_map.get_root().render()

    header = madrid_map.get_root().header.render()

    body_html= madrid_map.get_root().html.render()

    script= madrid_map.get_root().script.render()

    return render_template_string ("""
    <!DOCTYPE html>
    <html>  
        <head>
        {{header | safe}}
        </head>
        <body>
            <h1> Air quality in different cities </h1>
            {{body_html | safe}}

            <script>
            {{script|safe}}
            </script>
        </body>
    </html>
    """, header = header, body_html= body_html, script=script)


if __name__=="__main__":
    app.run(host="0.0.0.0", port=50100, debug=True)
